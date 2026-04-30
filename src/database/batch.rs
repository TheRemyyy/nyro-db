use serde_json::Value;
use std::collections::hash_map::Entry;
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

type InsertCommitter = tokio::sync::oneshot::Sender<Result<(), String>>;

struct PendingModelBatch {
    storage: Arc<LogStorage>,
    model_name: String,
    entries: Vec<LogEntry<Value>>,
    committers: Vec<InsertCommitter>,
}

pub(crate) struct BatchProcessor {
    pub(crate) receiver: mpsc::Receiver<BatchOperation>,
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
                            process_batch(&mut batch, &self.metrics, &self.log_config, &self.real_time_tx);
                        }
                    } else {
                        process_batch(&mut batch, &self.metrics, &self.log_config, &self.real_time_tx);
                        break;
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        process_batch(&mut batch, &self.metrics, &self.log_config, &self.real_time_tx);
                    }
                }
            }
        }

        Logger::info_with_config(&self.log_config, "Batch processing task terminated");
    }
}

fn drain_ready_operations(
    receiver: &mut mpsc::Receiver<BatchOperation>,
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
    batch: &mut Vec<BatchOperation>,
    metrics: &Arc<Metrics>,
    log_config: &LoggingConfig,
    real_time_tx: &tokio::sync::broadcast::Sender<String>,
) {
    let mut primary_batch = None;
    let mut grouped_batches = None;

    for operation in batch.drain(..) {
        match operation {
            BatchOperation::Insert {
                storage,
                entry,
                committed,
            } => {
                let storage_key = Arc::as_ptr(&storage) as usize;
                push_pending_insert(
                    &mut primary_batch,
                    &mut grouped_batches,
                    storage_key,
                    storage,
                    entry,
                    committed,
                );
            }
        }
    }

    if let Some(grouped) = grouped_batches {
        for (_, pending_batch) in grouped {
            process_model_batch(pending_batch, metrics, log_config, real_time_tx);
        }
    } else if let Some((_, pending_batch)) = primary_batch {
        process_model_batch(pending_batch, metrics, log_config, real_time_tx);
    }
}

fn push_pending_insert(
    primary_batch: &mut Option<(usize, PendingModelBatch)>,
    grouped_batches: &mut Option<HashMap<usize, PendingModelBatch>>,
    storage_key: usize,
    storage: Arc<LogStorage>,
    entry: LogEntry<Value>,
    committed: tokio::sync::oneshot::Sender<Result<(), String>>,
) {
    if let Some(grouped) = grouped_batches {
        push_grouped_insert(grouped, storage_key, storage, entry, committed);
        return;
    }

    match primary_batch {
        Some((primary_key, pending_batch)) if *primary_key == storage_key => {
            pending_batch.push(entry, committed);
        }
        Some(_) => {
            let Some((primary_key, pending_batch)) = primary_batch.take() else {
                return;
            };
            let mut grouped = HashMap::new();
            grouped.insert(primary_key, pending_batch);
            push_grouped_insert(&mut grouped, storage_key, storage, entry, committed);
            *grouped_batches = Some(grouped);
        }
        None => {
            *primary_batch = Some((storage_key, new_pending_batch(storage, entry, committed)));
        }
    }
}

fn push_grouped_insert(
    grouped: &mut HashMap<usize, PendingModelBatch>,
    storage_key: usize,
    storage: Arc<LogStorage>,
    entry: LogEntry<Value>,
    committed: tokio::sync::oneshot::Sender<Result<(), String>>,
) {
    match grouped.entry(storage_key) {
        Entry::Occupied(mut model_batch) => {
            model_batch.get_mut().push(entry, committed);
        }
        Entry::Vacant(empty_slot) => {
            empty_slot.insert(new_pending_batch(storage, entry, committed));
        }
    }
}

fn new_pending_batch(
    storage: Arc<LogStorage>,
    entry: LogEntry<Value>,
    committed: tokio::sync::oneshot::Sender<Result<(), String>>,
) -> PendingModelBatch {
    PendingModelBatch {
        model_name: storage.model_name().to_string(),
        entries: vec![entry],
        committers: vec![committed],
        storage,
    }
}

impl PendingModelBatch {
    fn push(&mut self, entry: LogEntry<Value>, committed: InsertCommitter) {
        self.entries.push(entry);
        self.committers.push(committed);
    }
}

fn process_model_batch(
    pending_batch: PendingModelBatch,
    metrics: &Arc<Metrics>,
    log_config: &LoggingConfig,
    real_time_tx: &tokio::sync::broadcast::Sender<String>,
) {
    let timer = Instant::now();
    let result = pending_batch.storage.append_entries(&pending_batch.entries);

    match result {
        Ok(()) => {
            let elapsed = timer.elapsed();
            if metrics.enabled {
                metrics.record_inserts(
                    pending_batch.entries.len() as u64,
                    elapsed,
                    metrics.max_samples(),
                );
            }
            let publish_realtime = real_time_tx.receiver_count() > 0;
            if publish_realtime {
                for entry in &pending_batch.entries {
                    publish_insert_event(
                        real_time_tx,
                        log_config,
                        &pending_batch.model_name,
                        entry,
                    );
                }
            }
            for committed in pending_batch.committers {
                let _ = committed.send(Ok(()));
            }
        }
        Err(error) => {
            let message = error.to_string();
            for committed in pending_batch.committers {
                let _ = committed.send(Err(message.clone()));
            }
        }
    }
}
