use crate::config::MetricsConfig;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Metrics {
    pub total_inserts: AtomicU64,
    pub total_gets: AtomicU64,
    pub total_queries: AtomicU64,
    pub start_time: Instant,
    pub insert_times: Arc<RwLock<Vec<Duration>>>,
    pub get_times: Arc<RwLock<Vec<Duration>>>,
    pub enabled: bool,
}

impl Metrics {
    pub fn new(config: &MetricsConfig) -> Self {
        Self {
            total_inserts: AtomicU64::new(0),
            total_gets: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            start_time: Instant::now(),
            insert_times: Arc::new(RwLock::new(Vec::with_capacity(config.max_samples))),
            get_times: Arc::new(RwLock::new(Vec::with_capacity(config.max_samples))),
            enabled: config.enable,
        }
    }

    pub fn record_insert(&self, duration: Duration, max_samples: usize) {
        if !self.enabled {
            return;
        }
        self.total_inserts.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut times) = self.insert_times.try_write() {
            times.push(duration);
            if times.len() > max_samples {
                times.drain(0..max_samples / 2);
            }
        }
    }

    pub fn record_get(&self, duration: Duration, max_samples: usize) {
        if !self.enabled {
            return;
        }
        self.total_gets.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut times) = self.get_times.try_write() {
            times.push(duration);
            if times.len() > max_samples {
                times.drain(0..max_samples / 2);
            }
        }
    }

    pub fn record_query(&self) {
        if !self.enabled {
            return;
        }
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_batch_insert(&self) {
        if !self.enabled {
            return;
        }
        self.total_inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> MetricsReport {
        let uptime = self.start_time.elapsed();
        let total_inserts = self.total_inserts.load(Ordering::Relaxed);
        let total_gets = self.total_gets.load(Ordering::Relaxed);
        let total_queries = self.total_queries.load(Ordering::Relaxed);

        let insert_times = self.insert_times.read().unwrap();
        let get_times = self.get_times.read().unwrap();

        let avg_insert_latency = if !insert_times.is_empty() {
            insert_times.iter().sum::<Duration>().as_nanos() as f64 / insert_times.len() as f64
        } else {
            0.0
        };

        let avg_get_latency = if !get_times.is_empty() {
            get_times.iter().sum::<Duration>().as_nanos() as f64 / get_times.len() as f64
        } else {
            0.0
        };

        let mut sorted_inserts = insert_times.clone();
        sorted_inserts.sort();
        let p99_insert = if !sorted_inserts.is_empty() {
            let idx = (sorted_inserts.len() as f64 * 0.99) as usize;
            sorted_inserts
                .get(idx)
                .unwrap_or(&Duration::ZERO)
                .as_nanos() as f64
        } else {
            0.0
        };

        MetricsReport {
            uptime_secs: uptime.as_secs_f64(),
            total_operations: total_inserts + total_gets + total_queries,
            total_inserts,
            total_gets,
            total_queries,
            inserts_per_sec: if uptime.as_secs_f64() > 0.0 {
                total_inserts as f64 / uptime.as_secs_f64()
            } else {
                0.0
            },
            gets_per_sec: if uptime.as_secs_f64() > 0.0 {
                total_gets as f64 / uptime.as_secs_f64()
            } else {
                0.0
            },
            avg_insert_latency_ns: avg_insert_latency,
            avg_get_latency_ns: avg_get_latency,
            p99_insert_latency_ns: p99_insert,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MetricsReport {
    pub uptime_secs: f64,
    pub total_operations: u64,
    pub total_inserts: u64,
    pub total_gets: u64,
    pub total_queries: u64,
    pub inserts_per_sec: f64,
    pub gets_per_sec: f64,
    pub avg_insert_latency_ns: f64,
    pub avg_get_latency_ns: f64,
    pub p99_insert_latency_ns: f64,
}
