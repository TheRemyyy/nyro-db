use anyhow::Result;
use nyrodb::config::NyroConfig;
use nyrodb::database::NyroDB;
use nyrodb::utils::benchmark::{benchmark_data_dir, cleanup_path, rate, stats, Stats};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;

const WARMUP_ITERATIONS: usize = 1;
const MEASURED_ITERATIONS: usize = 5;
const OPERATIONS_PER_ITERATION: u64 = 100_000;
const BULK_OPERATIONS_PER_ITERATION: u64 = 1_000_000;
const BULK_CHUNK_SIZE: u64 = 1_000_000;
const CONCURRENCY: usize = 1_024;
const READ_CONCURRENCY: usize = 256;
const BATCH_SIZE: usize = 1_024;
const BATCH_TIMEOUT_MS: u64 = 100;

#[derive(Debug, Serialize)]
struct IterationReport {
    iteration: usize,
    warmup: bool,
    successful_inserts: u64,
    failed_inserts: u64,
    successful_gets: u64,
    failed_gets: u64,
    successful_bulk_inserts: u64,
    failed_bulk_inserts: u64,
    insert_duration_s: f64,
    get_duration_s: f64,
    bulk_insert_duration_s: f64,
    insert_ops_per_sec: f64,
    get_ops_per_sec: f64,
    bulk_insert_ops_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    operations_per_iteration: u64,
    bulk_operations_per_iteration: u64,
    bulk_chunk_size: u64,
    concurrency: usize,
    read_concurrency: usize,
    batch_size: usize,
    batch_timeout_ms: u64,
    measured_insert_ops_per_sec: Stats,
    measured_get_ops_per_sec: Stats,
    measured_bulk_insert_ops_per_sec: Stats,
    iterations: Vec<IterationReport>,
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
    let bulk_insert_rates = measured
        .iter()
        .map(|report| report.bulk_insert_ops_per_sec)
        .collect::<Vec<_>>();

    let report = BenchmarkReport {
        operations_per_iteration: OPERATIONS_PER_ITERATION,
        bulk_operations_per_iteration: BULK_OPERATIONS_PER_ITERATION,
        bulk_chunk_size: BULK_CHUNK_SIZE,
        concurrency: CONCURRENCY,
        read_concurrency: READ_CONCURRENCY,
        batch_size: BATCH_SIZE,
        batch_timeout_ms: BATCH_TIMEOUT_MS,
        measured_insert_ops_per_sec: stats(&insert_rates),
        measured_get_ops_per_sec: stats(&get_rates),
        measured_bulk_insert_ops_per_sec: stats(&bulk_insert_rates),
        iterations: reports,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_iteration(iteration: usize, warmup: bool) -> Result<IterationReport> {
    let data_dir = benchmark_data_dir(iteration);
    cleanup_path(&data_dir)?;
    let db = Arc::new(NyroDB::new(benchmark_config(&data_dir)));
    let insert_rows = build_rows("bench", iteration, OPERATIONS_PER_ITERATION, 0);

    let insert_start = Instant::now();
    let (successful_inserts, failed_inserts) = run_inserts(db.clone(), insert_rows).await;
    let insert_duration = insert_start.elapsed();

    let get_start = Instant::now();
    let (successful_gets, failed_gets) = run_gets(db.clone()).await;
    let get_duration = get_start.elapsed();

    db.shutdown().await?;
    cleanup_path(&data_dir)?;
    let bulk_db = Arc::new(NyroDB::new(benchmark_config(&data_dir)));
    let bulk_rows = build_rows("bulk", iteration, BULK_OPERATIONS_PER_ITERATION, 0);
    let bulk_start = Instant::now();
    let (successful_bulk_inserts, failed_bulk_inserts) =
        run_bulk_inserts(bulk_db.clone(), bulk_rows).await;
    let bulk_insert_duration = bulk_start.elapsed();
    bulk_db.shutdown().await?;

    cleanup_path(&data_dir)?;

    Ok(IterationReport {
        iteration,
        warmup,
        successful_inserts,
        failed_inserts,
        successful_gets,
        failed_gets,
        successful_bulk_inserts,
        failed_bulk_inserts,
        insert_duration_s: insert_duration.as_secs_f64(),
        get_duration_s: get_duration.as_secs_f64(),
        bulk_insert_duration_s: bulk_insert_duration.as_secs_f64(),
        insert_ops_per_sec: rate(successful_inserts, insert_duration),
        get_ops_per_sec: rate(successful_gets, get_duration),
        bulk_insert_ops_per_sec: rate(successful_bulk_inserts, bulk_insert_duration),
    })
}

async fn run_inserts(db: Arc<NyroDB>, rows: Vec<Value>) -> (u64, u64) {
    let partitions = partition_rows(rows, CONCURRENCY);
    let mut workers = Vec::with_capacity(CONCURRENCY);

    for partition in partitions {
        let db_clone = Arc::clone(&db);
        workers.push(tokio::spawn(async move {
            let mut successful = 0;
            let mut failed = 0;

            for row in partition {
                match db_clone.insert_raw("user", row).await {
                    Ok(_) => successful += 1,
                    Err(_) => failed += 1,
                }
            }

            (successful, failed)
        }));
    }

    collect_worker_counts(workers).await
}

async fn collect_worker_counts(workers: Vec<tokio::task::JoinHandle<(u64, u64)>>) -> (u64, u64) {
    let mut successful = 0;
    let mut failed = 0;

    for worker in workers {
        match worker.await {
            Ok((worker_successful, worker_failed)) => {
                successful += worker_successful;
                failed += worker_failed;
            }
            Err(_) => failed += 1,
        }
    }

    (successful, failed)
}

async fn run_bulk_inserts(db: Arc<NyroDB>, mut rows: Vec<Value>) -> (u64, u64) {
    let mut successful = 0;
    let mut failed = 0;

    while !rows.is_empty() {
        let take_count = (BULK_CHUNK_SIZE as usize).min(rows.len());
        let remaining = rows.split_off(take_count);
        let chunk = std::mem::replace(&mut rows, remaining);
        let chunk_size = chunk.len() as u64;

        match db.insert_many_raw("user", chunk).await {
            Ok(ids) => successful += ids.len() as u64,
            Err(_) => failed += chunk_size,
        }
    }

    (successful, failed)
}

fn build_rows(prefix: &str, iteration: usize, count: u64, start_id: u64) -> Vec<Value> {
    (0..count)
        .map(|offset| {
            let id = start_id + offset;
            json!({
                "id": id,
                "email": format!("{}-{}-{}@nyro.local", prefix, iteration, id),
                "hash_password": format!("hash_{}", id),
                "created_at": id
            })
        })
        .collect()
}

async fn run_gets(db: Arc<NyroDB>) -> (u64, u64) {
    let ranges = partition_ranges(OPERATIONS_PER_ITERATION, READ_CONCURRENCY);
    let mut workers = Vec::with_capacity(READ_CONCURRENCY);

    for (start_id, end_id) in ranges {
        let db_clone = Arc::clone(&db);
        workers.push(tokio::spawn(async move {
            let mut successful = 0;
            let mut failed = 0;

            for id in start_id..end_id {
                match db_clone.get_raw("user", id).await {
                    Ok(Some(_)) => successful += 1,
                    Ok(None) | Err(_) => failed += 1,
                }
            }

            (successful, failed)
        }));
    }

    collect_worker_counts(workers).await
}

fn partition_rows(rows: Vec<Value>, partition_count: usize) -> Vec<Vec<Value>> {
    let mut partitions = (0..partition_count).map(|_| Vec::new()).collect::<Vec<_>>();
    for (index, row) in rows.into_iter().enumerate() {
        partitions[index % partition_count].push(row);
    }
    partitions
        .into_iter()
        .filter(|partition| !partition.is_empty())
        .collect()
}

fn partition_ranges(total: u64, partition_count: usize) -> Vec<(u64, u64)> {
    let chunk_size = total.div_ceil(partition_count as u64);
    (0..partition_count as u64)
        .map(|partition| {
            let start = partition * chunk_size;
            let end = (start + chunk_size).min(total);
            (start, end)
        })
        .filter(|(start, end)| start < end)
        .collect()
}

fn benchmark_config(data_dir: &str) -> NyroConfig {
    let mut config = NyroConfig::default();
    config.storage.data_dir = data_dir.to_string();
    config.storage.sync_interval = 60_000;
    config.storage.enable_mmap = false;
    config.performance.batch_size = BATCH_SIZE;
    config.performance.batch_timeout = BATCH_TIMEOUT_MS;
    config.performance.max_concurrent_ops = CONCURRENCY;
    config.server.graceful_shutdown_timeout = 0;
    config.logging.level = "off".to_string();
    config.metrics.enable = false;
    config
}

fn ensure_no_failures(reports: &[IterationReport]) -> Result<()> {
    let failed_report = reports.iter().find(|report| {
        report.failed_inserts > 0 || report.failed_gets > 0 || report.failed_bulk_inserts > 0
    });

    if let Some(report) = failed_report {
        return Err(anyhow::anyhow!(
            "Benchmark iteration {} had {} failed inserts, {} failed gets, and {} failed bulk inserts",
            report.iteration,
            report.failed_inserts,
            report.failed_gets,
            report.failed_bulk_inserts
        ));
    }

    Ok(())
}
