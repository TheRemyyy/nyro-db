mod nyro_benchmark {
    pub(crate) mod support;
}

use anyhow::Result;
use nyro_benchmark::support::{
    environment_report, log_file_size, operation_report, run_get_workers, run_insert_workers,
    worker_report, EnvironmentReport, OperationReport,
};
use nyrodb::config::NyroConfig;
use nyrodb::database::NyroDB;
use nyrodb::utils::benchmark::{benchmark_data_dir, cleanup_path, stats, Stats};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;

const WARMUP_ITERATIONS: usize = 1;
const MEASURED_ITERATIONS: usize = 5;
const OPERATIONS_PER_ITERATION: u64 = 100_000;
const CHUNKED_OPERATIONS_PER_ITERATION: u64 = 1_000_000;
const CHUNKED_INSERT_CHUNK_SIZE: u64 = 1_024;
const BULK_OPERATIONS_PER_ITERATION: u64 = 1_000_000;
const BULK_CHUNK_SIZE: u64 = 1_000_000;
const CONCURRENCY: usize = 1_024;
const READ_CONCURRENCY: usize = 256;
const BATCH_SIZE: usize = 1_024;
const BATCH_TIMEOUT_MS: u64 = 100;
const STORAGE_MODE: &str = "buffered_throughput";
const STORAGE_SYNC_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Serialize)]
struct IterationReport {
    iteration: usize,
    warmup: bool,
    insert: OperationReport,
    get: OperationReport,
    chunked_insert: OperationReport,
    bulk_insert: OperationReport,
    insert_log_file_bytes: u64,
    chunked_log_file_bytes: u64,
    bulk_log_file_bytes: u64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    operations_per_iteration: u64,
    chunked_operations_per_iteration: u64,
    chunked_insert_chunk_size: u64,
    bulk_operations_per_iteration: u64,
    bulk_chunk_size: u64,
    concurrency: usize,
    read_concurrency: usize,
    single_insert_mode: &'static str,
    configured_batch_size: usize,
    configured_batch_timeout_ms: u64,
    storage_mode: &'static str,
    storage_sync_interval_ms: u64,
    environment: EnvironmentReport,
    row_shape: RowShapeReport,
    measured_insert_ops_per_sec: Stats,
    measured_get_ops_per_sec: Stats,
    measured_chunked_insert_ops_per_sec: Stats,
    measured_bulk_insert_ops_per_sec: Stats,
    iterations: Vec<IterationReport>,
}

#[derive(Debug, Serialize)]
struct RowShapeReport {
    fields: usize,
    sample_json_bytes: usize,
    indexed_secondary_fields: usize,
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
        .map(|report| report.insert.ops_per_sec)
        .collect::<Vec<_>>();
    let get_rates = measured
        .iter()
        .map(|report| report.get.ops_per_sec)
        .collect::<Vec<_>>();
    let bulk_insert_rates = measured
        .iter()
        .map(|report| report.bulk_insert.ops_per_sec)
        .collect::<Vec<_>>();
    let chunked_insert_rates = measured
        .iter()
        .map(|report| report.chunked_insert.ops_per_sec)
        .collect::<Vec<_>>();

    let report = BenchmarkReport {
        operations_per_iteration: OPERATIONS_PER_ITERATION,
        chunked_operations_per_iteration: CHUNKED_OPERATIONS_PER_ITERATION,
        chunked_insert_chunk_size: CHUNKED_INSERT_CHUNK_SIZE,
        bulk_operations_per_iteration: BULK_OPERATIONS_PER_ITERATION,
        bulk_chunk_size: BULK_CHUNK_SIZE,
        concurrency: CONCURRENCY,
        read_concurrency: READ_CONCURRENCY,
        single_insert_mode: "direct_writer",
        configured_batch_size: BATCH_SIZE,
        configured_batch_timeout_ms: BATCH_TIMEOUT_MS,
        storage_mode: STORAGE_MODE,
        storage_sync_interval_ms: STORAGE_SYNC_INTERVAL_MS,
        environment: environment_report(),
        row_shape: row_shape_report()?,
        measured_insert_ops_per_sec: stats(&insert_rates),
        measured_get_ops_per_sec: stats(&get_rates),
        measured_chunked_insert_ops_per_sec: stats(&chunked_insert_rates),
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
    let (successful_inserts, failed_inserts, insert_workers) =
        run_insert_workers(db.clone(), insert_rows, CONCURRENCY).await;
    let insert_duration = insert_start.elapsed();

    let get_start = Instant::now();
    let (successful_gets, failed_gets, get_workers) =
        run_get_workers(db.clone(), OPERATIONS_PER_ITERATION, READ_CONCURRENCY).await;
    let get_duration = get_start.elapsed();

    db.shutdown().await?;
    let insert_log_file_bytes = log_file_size(&data_dir, "user");
    cleanup_path(&data_dir)?;
    let chunked_db = Arc::new(NyroDB::new(benchmark_config(&data_dir)));
    let chunked_rows = build_rows("chunked", iteration, CHUNKED_OPERATIONS_PER_ITERATION, 0);
    let chunked_start = Instant::now();
    let (successful_chunked_inserts, failed_chunked_inserts, chunked_workers) =
        run_chunked_inserts(chunked_db.clone(), chunked_rows, CHUNKED_INSERT_CHUNK_SIZE).await;
    let chunked_insert_duration = chunked_start.elapsed();
    chunked_db.shutdown().await?;
    let chunked_log_file_bytes = log_file_size(&data_dir, "user");
    cleanup_path(&data_dir)?;

    let bulk_db = Arc::new(NyroDB::new(benchmark_config(&data_dir)));
    let bulk_rows = build_rows("bulk", iteration, BULK_OPERATIONS_PER_ITERATION, 0);
    let bulk_start = Instant::now();
    let (successful_bulk_inserts, failed_bulk_inserts, bulk_workers) =
        run_chunked_inserts(bulk_db.clone(), bulk_rows, BULK_CHUNK_SIZE).await;
    let bulk_insert_duration = bulk_start.elapsed();
    bulk_db.shutdown().await?;
    let bulk_log_file_bytes = log_file_size(&data_dir, "user");

    cleanup_path(&data_dir)?;

    Ok(IterationReport {
        iteration,
        warmup,
        insert: operation_report(
            successful_inserts,
            failed_inserts,
            insert_duration,
            &insert_workers,
        ),
        get: operation_report(successful_gets, failed_gets, get_duration, &get_workers),
        chunked_insert: operation_report(
            successful_chunked_inserts,
            failed_chunked_inserts,
            chunked_insert_duration,
            &chunked_workers,
        ),
        bulk_insert: operation_report(
            successful_bulk_inserts,
            failed_bulk_inserts,
            bulk_insert_duration,
            &bulk_workers,
        ),
        insert_log_file_bytes,
        chunked_log_file_bytes,
        bulk_log_file_bytes,
    })
}

async fn run_chunked_inserts(
    db: Arc<NyroDB>,
    rows: Vec<Value>,
    chunk_size: u64,
) -> (u64, u64, Vec<nyro_benchmark::support::WorkerReport>) {
    let mut successful = 0;
    let mut failed = 0;
    let mut reports = Vec::new();
    let mut row_iter = rows.into_iter();
    let chunk_capacity = chunk_size as usize;

    loop {
        let chunk = row_iter.by_ref().take(chunk_capacity).collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let current_chunk_size = chunk.len() as u64;

        let chunk_start = Instant::now();
        let mut chunk_successful = 0;
        let mut chunk_failed = 0;
        match db.insert_many_raw("user", chunk).await {
            Ok(ids) => chunk_successful = ids.len() as u64,
            Err(_) => chunk_failed = current_chunk_size,
        }
        successful += chunk_successful;
        failed += chunk_failed;
        reports.push(worker_report(
            chunk_successful,
            chunk_failed,
            chunk_start.elapsed(),
        ));
    }

    (successful, failed, reports)
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

fn benchmark_config(data_dir: &str) -> NyroConfig {
    let mut config = NyroConfig::default();
    config.storage.data_dir = data_dir.to_string();
    config.storage.sync_interval = STORAGE_SYNC_INTERVAL_MS;
    config.storage.enable_mmap = false;
    config.performance.batch_size = BATCH_SIZE;
    config.performance.batch_timeout = BATCH_TIMEOUT_MS;
    config.performance.max_concurrent_ops = CONCURRENCY;
    config.server.graceful_shutdown_timeout = 0;
    config.logging.level = "off".to_string();
    config.metrics.enable = false;
    config
}

fn row_shape_report() -> Result<RowShapeReport> {
    let sample = build_rows("shape", 0, 1, 0)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Failed to build benchmark sample row"))?;

    Ok(RowShapeReport {
        fields: sample.as_object().map(|object| object.len()).unwrap_or(0),
        sample_json_bytes: serde_json::to_vec(&sample)?.len(),
        indexed_secondary_fields: 0,
    })
}

fn ensure_no_failures(reports: &[IterationReport]) -> Result<()> {
    let failed_report = reports.iter().find(|report| {
        report.insert.failed > 0
            || report.get.failed > 0
            || report.chunked_insert.failed > 0
            || report.bulk_insert.failed > 0
    });

    if let Some(report) = failed_report {
        return Err(anyhow::anyhow!(
            "Benchmark iteration {} had {} failed inserts, {} failed gets, {} failed chunked inserts, and {} failed bulk inserts",
            report.iteration,
            report.insert.failed,
            report.get.failed,
            report.chunked_insert.failed,
            report.bulk_insert.failed
        ));
    }

    Ok(())
}
