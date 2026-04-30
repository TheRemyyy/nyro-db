mod batch;
mod helpers;
#[cfg(test)]
mod tests;
mod types;
pub(crate) mod validation;

use anyhow::Result;
use dashmap::{mapref::entry::Entry, DashMap};
use rayon::prelude::*;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};

pub use types::NyroDB;

use crate::config::{ModelSchema, NyroConfig};
use crate::models::{LogEntry, Operation};
use crate::storage::LogStorage;
use crate::utils::logger::Logger;
use crate::utils::metrics::{Metrics, MetricsReport};
use helpers::{current_unix_millis, entry_id, field_matches, publish_insert_event};
use types::BatchOperation;

impl NyroDB {
    pub fn new(config: NyroConfig) -> Self {
        let storages = Arc::new(DashMap::new());
        let metrics = Arc::new(Metrics::new(&config.metrics));
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let log_config = config.logging.clone();

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

        let (batch_sender, batch_receiver) = mpsc::unbounded_channel();
        let (real_time_tx, _) = tokio::sync::broadcast::channel(10000);
        tokio::spawn(
            batch::BatchProcessor {
                storages: storages.clone(),
                receiver: batch_receiver,
                metrics: metrics.clone(),
                shutdown_flag: shutdown_flag.clone(),
                log_config: log_config.clone(),
                real_time_tx: real_time_tx.clone(),
                batch_size: config.performance.batch_size,
                batch_timeout: Duration::from_millis(config.performance.batch_timeout),
            }
            .run(),
        );

        Logger::info_with_config(&log_config, "NyroDB engine initialized successfully");

        Self {
            storages,
            batch_sender,
            metrics,
            shutdown_flag,
            concurrency_limiter: Arc::new(Semaphore::new(config.performance.max_concurrent_ops)),
            config,
            real_time_tx,
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

        let schema = self.config.models.get(model_name).ok_or_else(|| {
            anyhow::anyhow!("Model '{}' not defined in configuration", model_name)
        })?;
        let new_storage = Arc::new(LogStorage::new(
            model_name,
            &self.config.storage,
            &self.config.logging,
            schema,
        )?);
        match self.storages.entry(model_name.to_string()) {
            Entry::Occupied(existing_storage) => Ok(existing_storage.get().clone()),
            Entry::Vacant(empty_slot) => {
                empty_slot.insert(new_storage.clone());
                Ok(new_storage)
            }
        }
    }

    pub async fn insert_raw(&self, model_name: &str, data: Value) -> Result<u64> {
        let start = Instant::now();
        let log_entry = self.prepare_insert_entry(model_name, data)?;
        let id = entry_id(&log_entry)?;

        if self.config.performance.batch_size > 1 {
            self.insert_batched(model_name, log_entry).await?;
        } else {
            self.get_storage(model_name)?.append(&log_entry)?;
            self.metrics
                .record_insert(start.elapsed(), self.config.metrics.max_samples);
            self.publish_insert(model_name, &log_entry);
        }

        Ok(id)
    }

    pub async fn insert_many_raw(&self, model_name: &str, rows: Vec<Value>) -> Result<Vec<u64>> {
        let start = Instant::now();
        let timestamp = current_unix_millis()?;
        let schema = self.config.models.get(model_name).ok_or_else(|| {
            anyhow::anyhow!("Model '{}' not defined in configuration", model_name)
        })?;
        let prepared_rows = rows
            .into_par_iter()
            .map(|row| {
                let entry = Self::prepare_insert_entry_for_schema(schema, row, timestamp)?;
                let id = entry_id(&entry)?;
                Ok((id, entry))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut ids = Vec::with_capacity(prepared_rows.len());
        let mut entries = Vec::with_capacity(prepared_rows.len());

        for (id, entry) in prepared_rows {
            ids.push(id);
            entries.push(entry);
        }

        let entry_refs = entries.iter().collect::<Vec<_>>();
        self.get_storage(model_name)?.append_many(&entry_refs)?;

        for entry in &entries {
            self.metrics
                .record_insert(start.elapsed(), self.config.metrics.max_samples);
            self.publish_insert(model_name, entry);
        }

        Ok(ids)
    }

    fn prepare_insert_entry(&self, model_name: &str, data: Value) -> Result<LogEntry<Value>> {
        let schema = self.config.models.get(model_name).ok_or_else(|| {
            anyhow::anyhow!("Model '{}' not defined in configuration", model_name)
        })?;
        Self::prepare_insert_entry_for_schema(schema, data, current_unix_millis()?)
    }

    fn prepare_insert_entry_for_schema(
        schema: &ModelSchema,
        data: Value,
        timestamp: u64,
    ) -> Result<LogEntry<Value>> {
        let obj = match data {
            Value::Object(obj) => obj,
            _ => return Err(anyhow::anyhow!("Data must be a JSON object")),
        };

        validation::validate_data_with_schema(schema, &obj)?;
        Ok(LogEntry {
            timestamp,
            operation: Operation::Insert,
            data: Value::Object(validation::filter_data_owned(schema, obj)),
        })
    }

    pub async fn get_raw(&self, model_name: &str, id: u64) -> Result<Option<Value>> {
        let start = Instant::now();
        let result = self.get_storage(model_name)?.get_value(id)?;

        self.metrics
            .record_get(start.elapsed(), self.config.metrics.max_samples);

        Ok(result)
    }

    pub async fn query_raw(&self, model_name: &str) -> Result<Vec<Value>> {
        let entries = self.get_storage(model_name)?.get_all::<Value>()?;
        self.metrics.record_query();
        Ok(entries.into_iter().map(|entry| entry.data).collect())
    }

    pub async fn query_by_field_raw(
        &self,
        model_name: &str,
        field: &str,
        value: &str,
    ) -> Result<Vec<Value>> {
        let storage = self.get_storage(model_name)?;
        let mut results = Vec::new();

        if let Some(field_index) = storage.secondary_indices.get(field) {
            if let Some(ids) = field_index.get(value) {
                for id in ids.value() {
                    if let Some(entry) = storage.get::<Value>(*id)? {
                        results.push(entry.data);
                    }
                }
            }
        } else {
            for entry in storage.get_all::<Value>()? {
                if field_matches(&entry.data, field, value) {
                    results.push(entry.data);
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
            if let Err(error) = storage.shutdown(&self.config.logging) {
                Logger::error_with_config(
                    &self.config.logging,
                    &format!("Failed to shutdown storage for {}: {}", model_name, error),
                );
            } else {
                shutdown_count += 1;
            }
        }

        Logger::shutdown_with_config(
            &self.config.logging,
            &format!("Flushed {} storage engines", shutdown_count),
        );
        let total_operations = self.get_metrics().total_operations;
        Logger::shutdown_with_config(
            &self.config.logging,
            &format!(
                "Final stats: {} total operations processed",
                total_operations
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

    async fn insert_batched(&self, model_name: &str, entry: LogEntry<Value>) -> Result<()> {
        self.get_storage(model_name)?;
        let (committed_tx, committed_rx) = oneshot::channel();
        self.batch_sender.send(BatchOperation::Insert {
            model_name: model_name.to_string(),
            entry,
            committed: committed_tx,
        })?;

        committed_rx
            .await
            .map_err(|_| anyhow::anyhow!("Batch processor stopped before committing insert"))?
            .map_err(|error| anyhow::anyhow!(error))
    }

    fn publish_insert(&self, model_name: &str, entry: &LogEntry<Value>) {
        publish_insert_event(&self.real_time_tx, &self.config.logging, model_name, entry);
    }
}
