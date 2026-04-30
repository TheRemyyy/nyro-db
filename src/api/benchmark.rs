use futures_util::{stream::FuturesUnordered, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use warp::http::StatusCode;
use warp::{reply, Reply};

use crate::database::NyroDB;
use crate::utils::logger::Logger;

const MAX_BENCHMARK_OPERATIONS: u64 = 10_000;
const MAX_BENCHMARK_CONCURRENCY: usize = 128;

pub async fn benchmark_handler(
    model_name: String,
    operations: u64,
    db: Arc<NyroDB>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    let log_config = db.get_config().logging.clone();

    if !db.get_config().security.enable_auth {
        return Ok(json_status(
            serde_json::json!({ "error": "Benchmark endpoint requires authentication to be enabled" }),
            StatusCode::SERVICE_UNAVAILABLE,
        ));
    }

    if operations == 0 || operations > MAX_BENCHMARK_OPERATIONS {
        return Ok(json_status(
            serde_json::json!({
                "error": format!(
                    "Benchmark operations must be between 1 and {}",
                    MAX_BENCHMARK_OPERATIONS
                )
            }),
            StatusCode::BAD_REQUEST,
        ));
    }

    if !db.get_config().models.contains_key(&model_name) {
        return Ok(json_status(
            serde_json::json!({ "error": format!("Model '{}' not found", model_name) }),
            StatusCode::BAD_REQUEST,
        ));
    }

    Logger::info_with_config(
        &log_config,
        &format!(
            "Starting benchmark: {} operations on '{}'",
            operations, model_name
        ),
    );

    let start_time = Instant::now();
    let mut benchmark_config = db.get_config().clone();
    let benchmark_dir = benchmark_data_dir(&model_name);
    benchmark_config.storage.data_dir = benchmark_dir.clone();
    benchmark_config.logging.level = "error".to_string();
    let benchmark_db = Arc::new(NyroDB::new(benchmark_config));
    let concurrency = db
        .get_config()
        .performance
        .max_concurrent_ops
        .min(MAX_BENCHMARK_CONCURRENCY)
        .min(operations as usize)
        .max(1);
    let mut next_operation = 0;
    let mut running = FuturesUnordered::new();
    let mut successful_inserts = 0;
    let mut failed_inserts = 0;

    loop {
        while next_operation < operations && running.len() < concurrency {
            let operation_id = next_operation;
            let db_clone = benchmark_db.clone();
            let model_name_clone = model_name.clone();
            running.push(tokio::spawn(async move {
                let data = serde_json::json!({
                    "id": operation_id,
                    "email": format!("user{}@test.com", operation_id),
                    "hash_password": format!("hash_{}", operation_id),
                    "created_at": 0
                });
                db_clone.insert_raw(&model_name_clone, data).await
            }));
            next_operation += 1;
        }

        if running.is_empty() {
            break;
        }

        match running.next().await {
            Some(Ok(Ok(_))) => successful_inserts += 1,
            Some(Ok(Err(error))) => {
                failed_inserts += 1;
                Logger::warn_with_config(
                    &log_config,
                    &format!("Benchmark insert failed: {}", error),
                );
            }
            Some(Err(error)) => {
                failed_inserts += 1;
                Logger::warn_with_config(&log_config, &format!("Benchmark task failed: {}", error));
            }
            None => break,
        }
    }

    let insert_duration = start_time.elapsed();
    if let Err(error) = benchmark_db.shutdown().await {
        Logger::warn_with_config(
            &log_config,
            &format!("Benchmark shutdown failed: {}", error),
        );
    }
    if let Err(error) = std::fs::remove_dir_all(&benchmark_dir) {
        Logger::warn_with_config(
            &log_config,
            &format!(
                "Failed to remove benchmark data dir {}: {}",
                benchmark_dir, error
            ),
        );
    }

    let insert_ops_per_sec = if insert_duration.as_secs_f64() > 0.0 {
        successful_inserts as f64 / insert_duration.as_secs_f64()
    } else {
        0.0
    };

    Logger::info_with_config(
        &log_config,
        &format!(
            "Benchmark completed: {} ops in {:.2}s = {:.0} ops/sec",
            successful_inserts,
            insert_duration.as_secs_f64(),
            insert_ops_per_sec
        ),
    );

    Ok(json_status(
        serde_json::json!({
            "model": model_name,
            "requested_ops": operations,
            "successful_ops": successful_inserts,
            "failed_ops": failed_inserts,
            "duration_s": insert_duration.as_secs_f64(),
            "ops_per_sec": insert_ops_per_sec
        }),
        StatusCode::OK,
    ))
}

fn json_status(value: serde_json::Value, status: StatusCode) -> Box<dyn Reply> {
    Box::new(reply::with_status(reply::json(&value), status))
}

fn benchmark_data_dir(model_name: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir()
        .join(format!("nyrodb-benchmark-{}-{}", model_name, timestamp))
        .to_string_lossy()
        .into_owned()
}
