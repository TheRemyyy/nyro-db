use nyrodb::database::NyroDB;
use nyrodb::utils::benchmark::{rate, stats, Stats};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Serialize)]
pub(crate) struct EnvironmentReport {
    pub(crate) os: &'static str,
    pub(crate) arch: &'static str,
    pub(crate) logical_cpus: usize,
    pub(crate) process_id: u32,
}

#[derive(Debug, Serialize)]
pub(crate) struct OperationReport {
    pub(crate) successful: u64,
    pub(crate) failed: u64,
    pub(crate) duration_s: f64,
    pub(crate) ops_per_sec: f64,
    pub(crate) failure_rate: f64,
    pub(crate) wall_avg_latency_us: f64,
    pub(crate) worker_ops_per_sec: Stats,
}

#[derive(Debug)]
pub(crate) struct WorkerReport {
    successful: u64,
    failed: u64,
    duration: Duration,
}

pub(crate) fn environment_report() -> EnvironmentReport {
    let logical_cpus = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);

    EnvironmentReport {
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        logical_cpus,
        process_id: std::process::id(),
    }
}

pub(crate) fn worker_report(successful: u64, failed: u64, duration: Duration) -> WorkerReport {
    WorkerReport {
        successful,
        failed,
        duration,
    }
}

pub(crate) fn operation_report(
    successful: u64,
    failed: u64,
    duration: Duration,
    workers: &[WorkerReport],
) -> OperationReport {
    let total = successful + failed;
    let worker_rates = workers
        .iter()
        .filter(|worker| worker.successful > 0)
        .map(|worker| rate(worker.successful, worker.duration))
        .collect::<Vec<_>>();

    OperationReport {
        successful,
        failed,
        duration_s: duration.as_secs_f64(),
        ops_per_sec: rate(successful, duration),
        failure_rate: if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        },
        wall_avg_latency_us: if successful > 0 {
            duration.as_secs_f64() * 1_000_000.0 / successful as f64
        } else {
            0.0
        },
        worker_ops_per_sec: stats(&worker_rates),
    }
}

pub(crate) async fn collect_worker_reports(
    workers: Vec<tokio::task::JoinHandle<WorkerReport>>,
) -> (u64, u64, Vec<WorkerReport>) {
    let mut successful = 0;
    let mut failed = 0;
    let mut reports = Vec::with_capacity(workers.len());

    for worker in workers {
        match worker.await {
            Ok(report) => {
                successful += report.successful;
                failed += report.failed;
                reports.push(report);
            }
            Err(_) => failed += 1,
        }
    }

    (successful, failed, reports)
}

pub(crate) async fn run_insert_workers(
    db: Arc<NyroDB>,
    rows: Vec<Value>,
    concurrency: usize,
) -> (u64, u64, Vec<WorkerReport>) {
    let partitions = partition_rows(rows, concurrency);
    let mut workers = Vec::with_capacity(partitions.len());

    for partition in partitions {
        let db_clone = Arc::clone(&db);
        workers.push(tokio::spawn(async move {
            let start = Instant::now();
            let mut successful = 0;
            let mut failed = 0;

            for row in partition {
                match db_clone.insert_raw("user", row).await {
                    Ok(_) => successful += 1,
                    Err(_) => failed += 1,
                }
            }

            worker_report(successful, failed, start.elapsed())
        }));
    }

    collect_worker_reports(workers).await
}

pub(crate) async fn run_repeated_get_workers(
    db: Arc<NyroDB>,
    total_operations: u64,
    key_count: u64,
    concurrency: usize,
) -> (u64, u64, Vec<WorkerReport>) {
    let ranges = partition_ranges(total_operations, concurrency);
    let mut workers = Vec::with_capacity(ranges.len());

    for (start_id, end_id) in ranges {
        let db_clone = Arc::clone(&db);
        workers.push(tokio::spawn(async move {
            let start = Instant::now();
            let mut successful = 0;
            let mut failed = 0;

            for operation_id in start_id..end_id {
                let id = operation_id % key_count;
                match db_clone.get_raw("user", id).await {
                    Ok(Some(_)) => successful += 1,
                    Ok(None) | Err(_) => failed += 1,
                }
            }

            worker_report(successful, failed, start.elapsed())
        }));
    }

    collect_worker_reports(workers).await
}

pub(crate) fn log_file_size(data_dir: &str, model_name: &str) -> u64 {
    let path = std::path::Path::new(data_dir).join(format!("{}.log", model_name));
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
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
