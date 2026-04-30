#[path = "nyro_benchmark/chunked.rs"]
mod chunked;
#[allow(dead_code)]
#[path = "nyro_benchmark/support.rs"]
mod support;

use anyhow::Result;
use chunked::run_chunked_inserts;
use nyrodb::config::NyroConfig;
use nyrodb::database::NyroDB;
use nyrodb::utils::benchmark::{benchmark_data_dir, cleanup_path, stats, Stats};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;
use support::{
    environment_report, log_file_size, operation_report, EnvironmentReport, OperationReport,
};

const WARMUP_ITERATIONS: usize = 1;
const MEASURED_ITERATIONS: usize = 5;
const OPERATIONS_PER_ITERATION: u64 = 1_000_000;
const BULK_CHUNK_SIZES: &[u64] = &[1_024, 4_096, 16_384, 65_536, 262_144, 1_000_000];
const MAX_CONCURRENT_OPS: usize = 64;
const STORAGE_SYNC_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Serialize)]
struct BulkSweepReport {
    operations_per_iteration: u64,
    chunk_sizes: &'static [u64],
    max_concurrent_ops: usize,
    storage_mode: &'static str,
    storage_sync_interval_ms: u64,
    environment: EnvironmentReport,
    best_chunk_size: BestChunkReport,
    cases: Vec<BulkCaseReport>,
}

#[derive(Debug, Serialize)]
struct BulkCaseReport {
    chunk_size: u64,
    measured_ops_per_sec: Stats,
    iterations: Vec<CaseIterationReport>,
}

#[derive(Debug, Serialize)]
struct CaseIterationReport {
    iteration: usize,
    warmup: bool,
    operation: OperationReport,
    log_file_bytes: u64,
}

#[derive(Debug, Serialize)]
struct BestChunkReport {
    chunk_size: u64,
    mean_ops_per_sec: f64,
    median_ops_per_sec: f64,
    stddev_ops_per_sec: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut cases = Vec::with_capacity(BULK_CHUNK_SIZES.len());
    for &chunk_size in BULK_CHUNK_SIZES {
        cases.push(run_case(chunk_size).await?);
    }

    let report = BulkSweepReport {
        operations_per_iteration: OPERATIONS_PER_ITERATION,
        chunk_sizes: BULK_CHUNK_SIZES,
        max_concurrent_ops: MAX_CONCURRENT_OPS,
        storage_mode: "buffered_throughput",
        storage_sync_interval_ms: STORAGE_SYNC_INTERVAL_MS,
        environment: environment_report(),
        best_chunk_size: best_case(&cases)?,
        cases,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_case(chunk_size: u64) -> Result<BulkCaseReport> {
    let mut iterations = Vec::with_capacity(WARMUP_ITERATIONS + MEASURED_ITERATIONS);
    for iteration in 0..(WARMUP_ITERATIONS + MEASURED_ITERATIONS) {
        let warmup = iteration < WARMUP_ITERATIONS;
        let data_dir = benchmark_data_dir((chunk_size as usize).saturating_add(iteration));
        cleanup_path(&data_dir)?;
        let db = Arc::new(NyroDB::new(benchmark_config(&data_dir)));
        let rows = build_rows(iteration, OPERATIONS_PER_ITERATION);

        let start = Instant::now();
        let (successful, failed, worker_reports) =
            run_chunked_inserts(db.clone(), rows, chunk_size).await;
        let operation = operation_report(successful, failed, start.elapsed(), &worker_reports);
        db.shutdown().await?;
        let log_file_bytes = log_file_size(&data_dir, "user");
        cleanup_path(&data_dir)?;

        if failed > 0 {
            return Err(anyhow::anyhow!(
                "bulk sweep chunk_size={} iteration={} failed {} operations",
                chunk_size,
                iteration,
                failed
            ));
        }
        iterations.push(CaseIterationReport {
            iteration,
            warmup,
            operation,
            log_file_bytes,
        });
    }

    Ok(case_report(chunk_size, iterations))
}

fn case_report(chunk_size: u64, iterations: Vec<CaseIterationReport>) -> BulkCaseReport {
    let rates = iterations
        .iter()
        .filter(|iteration| !iteration.warmup)
        .map(|iteration| iteration.operation.ops_per_sec)
        .collect::<Vec<_>>();

    BulkCaseReport {
        chunk_size,
        measured_ops_per_sec: stats(&rates),
        iterations,
    }
}

fn best_case(cases: &[BulkCaseReport]) -> Result<BestChunkReport> {
    let best = cases
        .iter()
        .max_by(|left, right| {
            left.measured_ops_per_sec
                .mean
                .total_cmp(&right.measured_ops_per_sec.mean)
        })
        .ok_or_else(|| anyhow::anyhow!("bulk sweep produced no cases"))?;

    Ok(BestChunkReport {
        chunk_size: best.chunk_size,
        mean_ops_per_sec: best.measured_ops_per_sec.mean,
        median_ops_per_sec: best.measured_ops_per_sec.median,
        stddev_ops_per_sec: best.measured_ops_per_sec.stddev,
    })
}

fn build_rows(iteration: usize, count: u64) -> Vec<Value> {
    (0..count)
        .map(|id| {
            json!({
                "id": id,
                "email": format!("bulk-sweep-{}-{}@nyro.local", iteration, id),
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
    config.performance.max_concurrent_ops = MAX_CONCURRENT_OPS;
    config.server.graceful_shutdown_timeout = 0;
    config.logging.level = "off".to_string();
    config.metrics.enable = false;
    config
}
