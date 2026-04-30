use dashmap::DashMap;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::config::LoggingConfig;
use crate::database::types::BatchOperation;
use crate::models::LogEntry;
use crate::storage::LogStorage;
use crate::utils::logger::Logger;
use crate::utils::metrics::Metrics;

pub(crate) struct BatchProcessor {
    pub(crate) storages: Arc<DashMap<String, Arc<LogStorage>>>,
    pub(crate) receiver: mpsc::UnboundedReceiver<BatchOperation>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shutdown_flag: Arc<AtomicBool>,
    pub(crate) log_config: LoggingConfig,
    pub(crate) real_time_tx: tokio::sync::broadcast::Sender<String>,
    pub(crate) batch_size: usize,
    pub(crate) batch_timeout: std::time::Duration,
}

impl BatchProcessor {
    pub(crate) async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut interval = tokio::time::interval(self.batch_timeout);

        loop {
            if self.shutdown_flag.load(Ordering::Relaxed) {
                if !batch.is_empty() {
                    Logger::info_with_config(
                        &self.log_config,
                        "Processing final batch before shutdown",
                    );
                    process_batch(
                        &self.storages,
                        &mut batch,
                        &self.metrics,
                        &self.log_config,
                        &self.real_time_tx,
                    )
                    .await;
                }
                break;
            }

            tokio::select! {
                operation = self.receiver.recv() => {
                    if let Some(operation) = operation {
                        batch.push(operation);
                        if batch.len() >= self.batch_size {
                            process_batch(&self.storages, &mut batch, &self.metrics, &self.log_config, &self.real_time_tx).await;
                        }
                    } else {
                        process_batch(&self.storages, &mut batch, &self.metrics, &self.log_config, &self.real_time_tx).await;
                        break;
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        process_batch(&self.storages, &mut batch, &self.metrics, &self.log_config, &self.real_time_tx).await;
                    }
                }
            }
        }

        Logger::info_with_config(&self.log_config, "Batch processing task terminated");
    }
}

async fn process_batch(
    storages: &Arc<DashMap<String, Arc<LogStorage>>>,
    batch: &mut Vec<BatchOperation>,
    metrics: &Arc<Metrics>,
    log_config: &LoggingConfig,
    real_time_tx: &tokio::sync::broadcast::Sender<String>,
) {
    for operation in batch.drain(..) {
        match operation {
            BatchOperation::Insert {
                model_name,
                entry,
                committed,
            } => {
                let result = append_entry(storages, &model_name, &entry, metrics);
                if result.is_ok() {
                    publish_insert(real_time_tx, &model_name, &entry, log_config);
                }
                let _ = committed.send(result.map_err(|error| error.to_string()));
            }
        }
    }
}

fn append_entry(
    storages: &Arc<DashMap<String, Arc<LogStorage>>>,
    model_name: &str,
    entry: &LogEntry<Value>,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    let storage = storages
        .get(model_name)
        .ok_or_else(|| anyhow::anyhow!("Storage for model '{}' not found", model_name))?;

    let timer = Instant::now();
    storage.value().append(entry)?;
    metrics.record_insert(timer.elapsed(), metrics.max_samples());
    Ok(())
}

fn publish_insert(
    real_time_tx: &tokio::sync::broadcast::Sender<String>,
    model_name: &str,
    entry: &LogEntry<Value>,
    log_config: &LoggingConfig,
) {
    match serde_json::to_string(&entry.data) {
        Ok(data) => {
            let _ = real_time_tx.send(format!("INSERT:{}:{}", model_name, data));
        }
        Err(error) => Logger::error_with_config(
            log_config,
            &format!(
                "Failed to serialize realtime event for {}: {}",
                model_name, error
            ),
        ),
    }
}
