use anyhow::Result;
use futures_util::{stream::FuturesUnordered, StreamExt};
use nyrodb::config::NyroConfig;
use nyrodb::database::NyroDB;
use serde::Serialize;
use serde_json::json;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

const WARMUP_ITERATIONS: usize = 1;
const MEASURED_ITERATIONS: usize = 5;
const OPERATIONS_PER_ITERATION: u64 = 50_000;
const CONCURRENCY: usize = 1_024;
const READ_CONCURRENCY: usize = 256;
const BATCH_SIZE: usize = 1_000;
const BATCH_TIMEOUT_MS: u64 = 1;

#[derive(Debug, Serialize)]
struct IterationReport {
    iteration: usize,
    warmup: bool,
    successful_inserts: u64,
    failed_inserts: u64,
    successful_gets: u64,
    failed_gets: u64,
    insert_duration_s: f64,
    get_duration_s: f64,
    insert_ops_per_sec: f64,
    get_ops_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    operations_per_iteration: u64,
    concurrency: usize,
    read_concurrency: usize,
    batch_size: usize,
    batch_timeout_ms: u64,
    measured_insert_ops_per_sec: Stats,
    measured_get_ops_per_sec: Stats,
    iterations: Vec<IterationReport>,
}

#[derive(Debug, Serialize)]
struct Stats {
    mean: f64,
    median: f64,
    stddev: f64,
    min: f64,
    max: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut reports = Vec::new();

    for iteration in 0..(WARMUP_ITERATIONS + MEASURED_ITERATIONS) {
        reports.push(run_iteration(iteration, iteration < WARMUP_ITERATIONS).await?);
    }
    ensure_no_failures(&reports)?;

    let measured = reports
        .iter()
        .filter(|report| !report.warmup)
        .collect::<Vec<_>>();
    let insert_rates = measured
        .iter()
        .map(|report| report.insert_ops_per_sec)
        .collect::<Vec<_>>();
    let get_rates = measured
        .iter()
        .map(|report| report.get_ops_per_sec)
        .collect::<Vec<_>>();

    let report = BenchmarkReport {
        operations_per_iteration: OPERATIONS_PER_ITERATION,
        concurrency: CONCURRENCY,
        read_concurrency: READ_CONCURRENCY,
        batch_size: BATCH_SIZE,
        batch_timeout_ms: BATCH_TIMEOUT_MS,
        measured_insert_ops_per_sec: stats(&insert_rates),
        measured_get_ops_per_sec: stats(&get_rates),
        iterations: reports,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_iteration(iteration: usize, warmup: bool) -> Result<IterationReport> {
    let data_dir = benchmark_data_dir(iteration);
    cleanup_path(&data_dir)?;
    let db = Arc::new(NyroDB::new(benchmark_config(&data_dir)));

    let insert_start = Instant::now();
    let (successful_inserts, failed_inserts) = run_inserts(db.clone(), iteration).await;
    let insert_duration = insert_start.elapsed();

    let get_start = Instant::now();
    let (successful_gets, failed_gets) = run_gets(db.clone()).await;
    let get_duration = get_start.elapsed();

    db.shutdown().await?;
    cleanup_path(&data_dir)?;

    Ok(IterationReport {
        iteration,
        warmup,
        successful_inserts,
        failed_inserts,
        successful_gets,
        failed_gets,
        insert_duration_s: insert_duration.as_secs_f64(),
        get_duration_s: get_duration.as_secs_f64(),
        insert_ops_per_sec: rate(successful_inserts, insert_duration),
        get_ops_per_sec: rate(successful_gets, get_duration),
    })
}

async fn run_inserts(db: Arc<NyroDB>, iteration: usize) -> (u64, u64) {
    let mut next_id = 0;
    let mut running = FuturesUnordered::new();
    let mut successful = 0;
    let mut failed = 0;

    loop {
        while next_id < OPERATIONS_PER_ITERATION && running.len() < READ_CONCURRENCY {
            let id = next_id;
            let db_clone = db.clone();
            running.push(tokio::spawn(async move {
                db_clone
                    .insert_raw(
                        "user",
                        json!({
                            "id": id,
                            "email": format!("bench-{}-{}@nyro.local", iteration, id),
                            "hash_password": format!("hash_{}", id),
                            "created_at": id
                        }),
                    )
                    .await
            }));
            next_id += 1;
        }

        if running.is_empty() {
            break;
        }

        match running.next().await {
            Some(Ok(Ok(_))) => successful += 1,
            Some(_) => failed += 1,
            None => break,
        }
    }

    (successful, failed)
}

async fn run_gets(db: Arc<NyroDB>) -> (u64, u64) {
    let mut next_id = 0;
    let mut running = FuturesUnordered::new();
    let mut successful = 0;
    let mut failed = 0;

    loop {
        while next_id < OPERATIONS_PER_ITERATION && running.len() < CONCURRENCY {
            let id = next_id;
            let db_clone = db.clone();
            running.push(tokio::spawn(
                async move { db_clone.get_raw("user", id).await },
            ));
            next_id += 1;
        }

        if running.is_empty() {
            break;
        }

        match running.next().await {
            Some(Ok(Ok(Some(_)))) => successful += 1,
            Some(_) => failed += 1,
            None => break,
        }
    }

    (successful, failed)
}

fn benchmark_config(data_dir: &str) -> NyroConfig {
    let mut config = NyroConfig::default();
    config.storage.data_dir = data_dir.to_string();
    config.storage.sync_interval = 0;
    config.storage.enable_mmap = false;
    config.performance.batch_size = BATCH_SIZE;
    config.performance.batch_timeout = BATCH_TIMEOUT_MS;
    config.performance.max_concurrent_ops = CONCURRENCY;
    config.server.graceful_shutdown_timeout = 0;
    config.logging.level = "error".to_string();
    config.metrics.enable = false;
    config
}

fn stats(values: &[f64]) -> Stats {
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
        stddev: variance.sqrt(),
        min: sorted[0],
        max: sorted[sorted.len() - 1],
    }
}

fn ensure_no_failures(reports: &[IterationReport]) -> Result<()> {
    let failed_report = reports
        .iter()
        .find(|report| report.failed_inserts > 0 || report.failed_gets > 0);

    if let Some(report) = failed_report {
        return Err(anyhow::anyhow!(
            "Benchmark iteration {} had {} failed inserts and {} failed gets",
            report.iteration,
            report.failed_inserts,
            report.failed_gets
        ));
    }

    Ok(())
}

fn rate(operations: u64, duration: Duration) -> f64 {
    if duration.as_secs_f64() > 0.0 {
        operations as f64 / duration.as_secs_f64()
    } else {
        0.0
    }
}

fn benchmark_data_dir(iteration: usize) -> String {
    std::env::temp_dir()
        .join(format!(
            "nyrodb-real-bench-{}-{}",
            std::process::id(),
            iteration
        ))
        .to_string_lossy()
        .into_owned()
}

fn cleanup_path(path: &str) -> Result<()> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path).map_err(Into::into),
        Ok(_) => std::fs::remove_file(path).map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
