use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use crate::config::LoggingConfig;
use crate::database::helpers::publish_insert_event;
use crate::database::types::BatchOperation;
use crate::models::LogEntry;
use crate::storage::LogStorage;
use crate::utils::logger::Logger;
use crate::utils::metrics::Metrics;

type PendingInsert = (
    LogEntry<Value>,
    tokio::sync::oneshot::Sender<Result<(), String>>,
);

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
                    );
                }
                break;
            }

            tokio::select! {
                operation = self.receiver.recv() => {
                    if let Some(operation) = operation {
                        batch.push(operation);
                        drain_ready_operations(&mut self.receiver, &mut batch, self.batch_size);
                        if batch.len() >= self.batch_size {
                            process_batch(&self.storages, &mut batch, &self.metrics, &self.log_config, &self.real_time_tx);
                        }
                    } else {
                        process_batch(&self.storages, &mut batch, &self.metrics, &self.log_config, &self.real_time_tx);
                        break;
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        process_batch(&self.storages, &mut batch, &self.metrics, &self.log_config, &self.real_time_tx);
                    }
                }
            }
        }

        Logger::info_with_config(&self.log_config, "Batch processing task terminated");
    }
}

fn drain_ready_operations(
    receiver: &mut mpsc::UnboundedReceiver<BatchOperation>,
    batch: &mut Vec<BatchOperation>,
    batch_size: usize,
) {
    while batch.len() < batch_size {
        match receiver.try_recv() {
            Ok(operation) => batch.push(operation),
            Err(_) => break,
        }
    }
}

fn process_batch(
    storages: &Arc<DashMap<String, Arc<LogStorage>>>,
    batch: &mut Vec<BatchOperation>,
    metrics: &Arc<Metrics>,
    log_config: &LoggingConfig,
    real_time_tx: &tokio::sync::broadcast::Sender<String>,
) {
    let mut grouped: HashMap<String, Vec<PendingInsert>> = HashMap::new();

    for operation in batch.drain(..) {
        match operation {
            BatchOperation::Insert {
                model_name,
                entry,
                committed,
            } => {
                grouped
                    .entry(model_name)
                    .or_default()
                    .push((entry, committed));
            }
        }
    }

    for (model_name, entries) in grouped {
        process_model_batch(
            storages,
            &model_name,
            entries,
            metrics,
            log_config,
            real_time_tx,
        );
    }
}

fn process_model_batch(
    storages: &Arc<DashMap<String, Arc<LogStorage>>>,
    model_name: &str,
    entries: Vec<PendingInsert>,
    metrics: &Arc<Metrics>,
    log_config: &LoggingConfig,
    real_time_tx: &tokio::sync::broadcast::Sender<String>,
) {
    let timer = Instant::now();
    let result = storages
        .get(model_name)
        .ok_or_else(|| anyhow::anyhow!("Storage for model '{}' not found", model_name))
        .and_then(|storage| {
            let log_entries = entries.iter().map(|(entry, _)| entry).collect::<Vec<_>>();
            storage.value().append_many(&log_entries)
        });

    match result {
        Ok(()) => {
            let elapsed = timer.elapsed();
            for (entry, committed) in entries {
                metrics.record_insert(elapsed, metrics.max_samples());
                publish_insert_event(real_time_tx, log_config, model_name, &entry);
                let _ = committed.send(Ok(()));
            }
        }
        Err(error) => {
            let message = error.to_string();
            for (_, committed) in entries {
                let _ = committed.send(Err(message.clone()));
            }
        }
    }
}
