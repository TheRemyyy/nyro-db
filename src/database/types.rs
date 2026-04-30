use dashmap::DashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::config::{ModelSchema, NyroConfig};
use crate::storage::LogStorage;
use crate::utils::metrics::Metrics;

pub struct NyroDB {
    pub(crate) runtimes: Arc<DashMap<String, Arc<ModelRuntime>>>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shutdown_flag: Arc<AtomicBool>,
    pub(crate) config: NyroConfig,
    pub(crate) concurrency_limiter: Arc<Semaphore>,
    pub real_time_tx: tokio::sync::broadcast::Sender<String>,
}

pub(crate) struct ModelRuntime {
    pub(crate) schema: Arc<ModelSchema>,
    pub(crate) storage: Arc<LogStorage>,
}
