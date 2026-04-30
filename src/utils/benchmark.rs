use anyhow::Result;
use serde::Serialize;
use std::cmp::Ordering;
use std::time::Duration;

#[derive(Debug, Serialize)]
pub struct Stats {
    pub mean: f64,
    pub median: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub stddev: f64,
    pub min: f64,
    pub max: f64,
}

pub fn stats(values: &[f64]) -> Stats {
    if values.is_empty() {
        return Stats {
            mean: 0.0,
            median: 0.0,
            p90: 0.0,
            p95: 0.0,
            p99: 0.0,
            stddev: 0.0,
            min: 0.0,
            max: 0.0,
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let variance = sorted
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / sorted.len() as f64;

    Stats {
        mean,
        median: sorted[sorted.len() / 2],
        p90: percentile(&sorted, 0.90),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        stddev: variance.sqrt(),
        min: sorted[0],
        max: sorted[sorted.len() - 1],
    }
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    let max_index = sorted.len().saturating_sub(1);
    let index = (max_index as f64 * quantile).ceil() as usize;
    sorted[index.min(max_index)]
}

pub fn rate(operations: u64, duration: Duration) -> f64 {
    if duration.as_secs_f64() > 0.0 {
        operations as f64 / duration.as_secs_f64()
    } else {
        0.0
    }
}

pub fn benchmark_data_dir(iteration: usize) -> String {
    std::env::temp_dir()
        .join(format!(
            "nyrodb-benchmark-{}-{}",
            std::process::id(),
            iteration
        ))
        .to_string_lossy()
        .into_owned()
}

pub fn cleanup_path(path: &str) -> Result<()> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path).map_err(Into::into),
        Ok(_) => std::fs::remove_file(path).map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
