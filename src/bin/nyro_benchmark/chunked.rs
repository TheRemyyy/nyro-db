use nyrodb::database::NyroDB;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

use super::support::{worker_report, WorkerReport};

pub(crate) async fn run_chunked_inserts(
    db: Arc<NyroDB>,
    rows: Vec<Value>,
    chunk_size: u64,
) -> (u64, u64, Vec<WorkerReport>) {
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
