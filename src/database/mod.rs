use anyhow::Result;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};

use crate::config::NyroConfig;
use crate::models::{LogEntry, Operation};
use crate::storage::LogStorage;
use crate::utils::logger::Logger;
use crate::utils::metrics::{Metrics, MetricsReport};

pub struct NyroDB {
    storages: Arc<DashMap<String, Arc<LogStorage>>>,
    batch_sender: mpsc::UnboundedSender<BatchOperation>,
    metrics: Arc<Metrics>,
    shutdown_flag: Arc<AtomicBool>,
    config: NyroConfig,
    concurrency_limiter: Arc<Semaphore>,
    pub real_time_tx: tokio::sync::broadcast::Sender<String>,
}

#[derive(Debug)]
enum BatchOperation {
    Insert(String, Vec<u8>),
}

impl NyroDB {
    pub fn new(config: NyroConfig) -> Self {
        let storages = Arc::new(DashMap::new());
        let metrics = Arc::new(Metrics::new(&config.metrics));
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();
        let log_config = config.logging.clone();
        let log_config_clone = log_config.clone();
        let storages_clone = storages.clone();
        let metrics_clone = metrics.clone();

        Logger::info_with_config(&log_config, "Initializing NyroDB engine");
        Logger::info_with_config(
            &log_config,
            &format!(
                "Config - Server: {}:{}, Batch size: {}, Buffer: {}KB, Models: {}",
                config.server.host,
                config.server.port,
                config.performance.batch_size,
                config.storage.buffer_size / 1024,
                config.models.len()
            ),
        );

        let batch_size = config.performance.batch_size;
        let batch_timeout = Duration::from_millis(config.performance.batch_timeout);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let (real_time_tx, _) = tokio::sync::broadcast::channel(10000);
        let real_time_tx_clone = real_time_tx.clone();

        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(batch_size);
            let mut interval = tokio::time::interval(batch_timeout);

            loop {
                if shutdown_flag_clone.load(Ordering::Relaxed) {
                    if !batch.is_empty() {
                        Logger::info_with_config(
                            &log_config_clone,
                            "Processing final batch before shutdown",
                        );
                        Self::process_batch(
                            &storages_clone,
                            &mut batch,
                            &metrics_clone,
                            &log_config_clone,
                            &real_time_tx_clone,
                        )
                        .await;
                    }
                    break;
                }

                tokio::select! {
                    operation = rx.recv() => {
                        if let Some(op) = operation {
                            batch.push(op);
                            if batch.len() >= batch_size {
                                Self::process_batch(&storages_clone, &mut batch, &metrics_clone, &log_config_clone, &real_time_tx_clone).await;
                            }
                        } else {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if !batch.is_empty() {
                            Self::process_batch(&storages_clone, &mut batch, &metrics_clone, &log_config_clone, &real_time_tx_clone).await;
                        }
                    }
                }
            }

            Logger::info_with_config(&log_config_clone, "Batch processing task terminated");
        });

        Logger::info_with_config(&log_config, "NyroDB engine initialized successfully");

        Self {
            storages,
            batch_sender: tx,
            metrics,
            shutdown_flag,
            concurrency_limiter: Arc::new(Semaphore::new(config.performance.max_concurrent_ops)),
            config,
            real_time_tx,
        }
    }

    async fn process_batch(
        storages: &Arc<DashMap<String, Arc<LogStorage>>>,
        batch: &mut Vec<BatchOperation>,
        metrics: &Arc<Metrics>,
        log_config: &crate::config::LoggingConfig,
        real_time_tx: &tokio::sync::broadcast::Sender<String>,
    ) {
        let mut grouped: HashMap<String, Vec<Vec<u8>>> = HashMap::new();

        for op in batch.drain(..) {
            match op {
                BatchOperation::Insert(model_name, data) => {
                    grouped.entry(model_name).or_default().push(data);
                }
            }
        }

        for (model_name, entries) in grouped {
            if let Some(storage) = storages.get(&model_name) {
                let storage_arc = storage.value().clone();
                for entry_data in entries {
                    let timer = Instant::now();
                    match serde_json::from_slice::<LogEntry<Value>>(&entry_data) {
                        Ok(entry) => {
                            if let Err(e) = storage_arc.append(&entry) {
                                Logger::error_with_config(
                                    log_config,
                                    &format!(
                                        "Failed to append entry to storage for {}: {}",
                                        model_name, e
                                    ),
                                );
                            } else {
                                metrics.record_batch_insert();
                                metrics.record_insert(timer.elapsed(), 1000);
                                let _ = real_time_tx.send(format!(
                                    "INSERT:{}:{}",
                                    model_name,
                                    serde_json::to_string(&entry.data).unwrap_or_default()
                                ));
                            }
                        }
                        Err(e) => {
                            Logger::error_with_config(
                                log_config,
                                &format!(
                                    "Failed to deserialize batch entry for {}: {}",
                                    model_name, e
                                ),
                            );
                        }
                    }
                }
            } else {
                Logger::warn_with_config(
                    log_config,
                    &format!("Storage for model '{}' not found during batch processing. Entries dropped.", model_name),
                );
            }
        }
    }

    pub fn get_storage(&self, model_name: &str) -> Result<Arc<LogStorage>> {
        if !self.config.models.contains_key(model_name) {
            return Err(anyhow::anyhow!(
                "Model '{}' not defined in configuration",
                model_name
            ));
        }

        if let Some(storage) = self.storages.get(model_name) {
            return Ok(storage.clone());
        }

        let storage = Arc::new(
            LogStorage::new(model_name, &self.config.storage, &self.config.logging).unwrap(),
        );
        self.storages
            .insert(model_name.to_string(), storage.clone());
        Ok(storage)
    }

    #[inline(always)]
    fn validate_data_fast(
        &self,
        model_name: &str,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<()> {
        let schema = self
            .config
            .models
            .get(model_name)
            .ok_or_else(|| anyhow::anyhow!("Model not found"))?;

        for field in &schema.fields {
            if field.required && !obj.contains_key(&field.name) {
                return Err(anyhow::anyhow!("Missing required field: '{}'", field.name));
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn filter_data_fast(
        &self,
        model_name: &str,
        obj: serde_json::Map<String, Value>,
    ) -> serde_json::Map<String, Value> {
        if let Some(schema) = self.config.models.get(model_name) {
            let mut filtered = serde_json::Map::with_capacity(schema.fields.len());

            for field in &schema.fields {
                if let Some(value) = obj.get(&field.name) {
                    filtered.insert(field.name.clone(), value.clone());
                }
            }

            filtered
        } else {
            obj
        }
    }

    pub async fn insert_raw(&self, model_name: &str, data: Value) -> Result<u64> {
        let start = Instant::now();

        let obj = data
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Data must be a JSON object"))?;

        self.validate_data_fast(model_name, obj)?;

        let id = obj
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid 'id' field"))?;

        let filtered_data = self.filter_data_fast(model_name, obj.clone());
        let raw_entry_data = Value::Object(filtered_data);
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if self.config.performance.batch_size > 1 {
            self.get_storage(model_name)?; // Ensure storage is initialized
            let log_entry = LogEntry {
                timestamp: start_time,
                operation: Operation::Insert,
                data: raw_entry_data,
            };
            let entry_bytes = serde_json::to_vec(&log_entry)?;
            self.batch_sender
                .send(BatchOperation::Insert(model_name.to_string(), entry_bytes))?;
        } else {
            let log_entry = LogEntry {
                timestamp: start_time,
                operation: Operation::Insert,
                data: raw_entry_data,
            };
            self.get_storage(model_name)?.append(&log_entry)?;
            self.metrics
                .record_insert(start.elapsed(), self.config.metrics.max_samples);
        }

        Ok(id)
    }

    pub async fn get_raw(&self, model_name: &str, id: u64) -> Result<Option<Value>> {
        let start = Instant::now();

        if !self.config.models.contains_key(model_name) {
            return Err(anyhow::anyhow!(
                "Model '{}' not defined in configuration",
                model_name
            ));
        }

        let result = if let Some(entry) = self.get_storage(model_name)?.get::<Value>(id)? {
            Some(entry.data)
        } else {
            None
        };

        self.metrics
            .record_get(start.elapsed(), self.config.metrics.max_samples);

        Ok(result)
    }

    pub async fn query_raw(&self, model_name: &str) -> Result<Vec<Value>> {
        if !self.config.models.contains_key(model_name) {
            return Err(anyhow::anyhow!(
                "Model '{}' not defined in configuration",
                model_name
            ));
        }

        let entries = self.get_storage(model_name)?.get_all::<Value>()?;
        self.metrics.record_query();

        Ok(entries.into_iter().map(|e| e.data).collect())
    }

    pub async fn query_by_field_raw(
        &self,
        model_name: &str,
        field: &str,
        value: &str,
    ) -> Result<Vec<Value>> {
        let storage = self.get_storage(model_name)?;

        let mut results = Vec::new();
        if let Some(field_idx) = storage.secondary_indices.get(field) {
            if let Some(ids) = field_idx.get(value) {
                for id in ids.value() {
                    if let Some(entry) = storage.get::<Value>(*id)? {
                        results.push(entry.data);
                    }
                }
            }
        }

        self.metrics.record_query();
        Ok(results)
    }

    pub fn get_metrics(&self) -> MetricsReport {
        self.metrics.get_stats()
    }

    pub async fn shutdown(&self) -> Result<()> {
        Logger::shutdown_with_config(&self.config.logging, "Initiating graceful shutdown");
        self.shutdown_flag.store(true, Ordering::Relaxed);

        let timeout = Duration::from_secs(self.config.server.graceful_shutdown_timeout);
        tokio::time::sleep(timeout).await;

        let mut shutdown_count = 0;
        for item in self.storages.iter() {
            let (model_name, storage) = item.pair();
            if let Err(e) = storage.shutdown(&self.config.logging) {
                Logger::error_with_config(
                    &self.config.logging,
                    &format!("Failed to shutdown storage for {}: {}", model_name, e),
                );
            } else {
                shutdown_count += 1;
            }
        }

        Logger::shutdown_with_config(
            &self.config.logging,
            &format!("Flushed {} storage engines", shutdown_count),
        );

        let final_metrics = self.get_metrics();
        Logger::shutdown_with_config(
            &self.config.logging,
            &format!(
                "Final stats: {} total operations processed",
                final_metrics.total_operations
            ),
        );

        Logger::shutdown_with_config(&self.config.logging, "NyroDB shutdown completed");
        Ok(())
    }

    pub fn get_config(&self) -> &NyroConfig {
        &self.config
    }

    pub fn get_concurrency_limiter(&self) -> Arc<Semaphore> {
        Arc::clone(&self.concurrency_limiter)
    }
}
