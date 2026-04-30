use dashmap::DashMap;
use serde_json::Value;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::config::NyroConfig;
use crate::models::LogEntry;
use crate::storage::LogStorage;
use crate::utils::metrics::Metrics;

pub struct NyroDB {
    pub(crate) storages: Arc<DashMap<String, Arc<LogStorage>>>,
    pub(crate) batch_sender: mpsc::Sender<BatchOperation>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shutdown_flag: Arc<AtomicBool>,
    pub(crate) config: NyroConfig,
    pub(crate) concurrency_limiter: Arc<Semaphore>,
    pub real_time_tx: tokio::sync::broadcast::Sender<String>,
}

pub(crate) enum BatchOperation {
    Insert {
        storage: Arc<LogStorage>,
        entry: LogEntry<Value>,
        committed: oneshot::Sender<Result<(), String>>,
    },
}
