#[path = "nyro_benchmark/support.rs"]
mod support;

use anyhow::Result;
use nyrodb::config::NyroConfig;
use nyrodb::database::NyroDB;
use nyrodb::utils::benchmark::{benchmark_data_dir, cleanup_path, stats, Stats};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;
use support::{
    environment_report, log_file_size, operation_report, run_insert_workers,
    run_repeated_get_workers, EnvironmentReport, OperationReport,
};

const WARMUP_ITERATIONS: usize = 1;
const MEASURED_ITERATIONS: usize = 5;
const OPERATIONS_PER_ITERATION: u64 = 100_000;
const WORKER_COUNTS: &[usize] = &[1, 4, 8, 16, 32, 64, 128];
const STORAGE_SYNC_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Serialize)]
struct SweepReport {
    operations_per_iteration: u64,
    worker_counts: &'static [usize],
    storage_mode: &'static str,
    storage_sync_interval_ms: u64,
    environment: EnvironmentReport,
    insert_sweep: Vec<SweepCaseReport>,
    get_sweep: Vec<SweepCaseReport>,
    best_insert_workers: BestCaseReport,
    best_get_workers: BestCaseReport,
}

#[derive(Debug, Serialize)]
struct SweepCaseReport {
    workers: usize,
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
struct BestCaseReport {
    workers: usize,
    mean_ops_per_sec: f64,
    median_ops_per_sec: f64,
    stddev_ops_per_sec: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut insert_sweep = Vec::with_capacity(WORKER_COUNTS.len());
    let mut get_sweep = Vec::with_capacity(WORKER_COUNTS.len());

    for &workers in WORKER_COUNTS {
        insert_sweep.push(run_insert_case(workers).await?);
    }
    for &workers in WORKER_COUNTS {
        get_sweep.push(run_get_case(workers).await?);
    }

    let report = SweepReport {
        operations_per_iteration: OPERATIONS_PER_ITERATION,
        worker_counts: WORKER_COUNTS,
        storage_mode: "buffered_throughput",
        storage_sync_interval_ms: STORAGE_SYNC_INTERVAL_MS,
        environment: environment_report(),
        best_insert_workers: best_case(&insert_sweep)?,
        best_get_workers: best_case(&get_sweep)?,
        insert_sweep,
        get_sweep,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_insert_case(workers: usize) -> Result<SweepCaseReport> {
    let mut iterations = Vec::with_capacity(WARMUP_ITERATIONS + MEASURED_ITERATIONS);

    for iteration in 0..(WARMUP_ITERATIONS + MEASURED_ITERATIONS) {
        let warmup = iteration < WARMUP_ITERATIONS;
        let data_dir = benchmark_data_dir(iteration + workers * 10_000);
        cleanup_path(&data_dir)?;
        let db = Arc::new(NyroDB::new(benchmark_config(&data_dir, workers)));
        let rows = build_rows("insert-sweep", workers, iteration, OPERATIONS_PER_ITERATION);

        let start = Instant::now();
        let (successful, failed, worker_reports) =
            run_insert_workers(db.clone(), rows, workers).await;
        let operation = operation_report(successful, failed, start.elapsed(), &worker_reports);
        db.shutdown().await?;
        let log_file_bytes = log_file_size(&data_dir, "user");
        cleanup_path(&data_dir)?;

        if failed > 0 {
            return Err(anyhow::anyhow!(
                "insert sweep workers={} iteration={} failed {} operations",
                workers,
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

    Ok(case_report(workers, iterations))
}

async fn run_get_case(workers: usize) -> Result<SweepCaseReport> {
    let mut iterations = Vec::with_capacity(WARMUP_ITERATIONS + MEASURED_ITERATIONS);

    for iteration in 0..(WARMUP_ITERATIONS + MEASURED_ITERATIONS) {
        let warmup = iteration < WARMUP_ITERATIONS;
        let data_dir = benchmark_data_dir(1_000_000 + iteration + workers * 10_000);
        cleanup_path(&data_dir)?;
        let db = Arc::new(NyroDB::new(benchmark_config(&data_dir, workers)));
        let rows = build_rows("get-sweep", workers, iteration, OPERATIONS_PER_ITERATION);
        db.insert_many_raw("user", rows).await?;

        let start = Instant::now();
        let (successful, failed, worker_reports) = run_repeated_get_workers(
            db.clone(),
            OPERATIONS_PER_ITERATION,
            OPERATIONS_PER_ITERATION,
            workers,
        )
        .await;
        let operation = operation_report(successful, failed, start.elapsed(), &worker_reports);
        db.shutdown().await?;
        let log_file_bytes = log_file_size(&data_dir, "user");
        cleanup_path(&data_dir)?;

        if failed > 0 {
            return Err(anyhow::anyhow!(
                "get sweep workers={} iteration={} failed {} operations",
                workers,
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

    Ok(case_report(workers, iterations))
}

fn case_report(workers: usize, iterations: Vec<CaseIterationReport>) -> SweepCaseReport {
    let rates = iterations
        .iter()
        .filter(|iteration| !iteration.warmup)
        .map(|iteration| iteration.operation.ops_per_sec)
        .collect::<Vec<_>>();

    SweepCaseReport {
        workers,
        measured_ops_per_sec: stats(&rates),
        iterations,
    }
}

fn best_case(cases: &[SweepCaseReport]) -> Result<BestCaseReport> {
    let best = cases
        .iter()
        .max_by(|left, right| {
            left.measured_ops_per_sec
                .mean
                .total_cmp(&right.measured_ops_per_sec.mean)
        })
        .ok_or_else(|| anyhow::anyhow!("sweep produced no cases"))?;

    Ok(BestCaseReport {
        workers: best.workers,
        mean_ops_per_sec: best.measured_ops_per_sec.mean,
        median_ops_per_sec: best.measured_ops_per_sec.median,
        stddev_ops_per_sec: best.measured_ops_per_sec.stddev,
    })
}

fn build_rows(prefix: &str, workers: usize, iteration: usize, count: u64) -> Vec<Value> {
    (0..count)
        .map(|id| {
            json!({
                "id": id,
                "email": format!("{}-{}-{}-{}@nyro.local", prefix, workers, iteration, id),
                "hash_password": format!("hash_{}", id),
                "created_at": id
            })
        })
        .collect()
}

fn benchmark_config(data_dir: &str, workers: usize) -> NyroConfig {
    let mut config = NyroConfig::default();
    config.storage.data_dir = data_dir.to_string();
    config.storage.sync_interval = STORAGE_SYNC_INTERVAL_MS;
    config.storage.enable_mmap = false;
    config.performance.max_concurrent_ops = workers.max(1);
    config.server.graceful_shutdown_timeout = 0;
    config.logging.level = "off".to_string();
    config.metrics.enable = false;
    config
}
