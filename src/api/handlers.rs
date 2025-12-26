use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use warp::http::StatusCode;
use warp::{reply, Rejection, Reply};

use crate::api::realtime::RealtimeServer;
use crate::database::NyroDB;
use crate::utils::logger::Logger;

pub async fn insert_handler(
    model_name: String,
    data: Value,
    db: Arc<NyroDB>,
) -> Result<impl Reply, warp::Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter.acquire().await.unwrap();

    match db.insert_raw(&model_name, data).await {
        Ok(id) => {
            Logger::info_with_config(
                &db.get_config().logging,
                &format!("Inserted into '{}': ID {}", model_name, id),
            );
            Ok(reply::with_status(
                format!("{{\"id\":{}}}", id),
                StatusCode::CREATED,
            ))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!("Failed to insert into '{}': {}", model_name, e),
            );
            Ok(reply::with_status(
                format!("{{\"error\":\"{}\"}}", e),
                StatusCode::BAD_REQUEST,
            ))
        }
    }
}

pub async fn get_handler(
    model_name: String,
    id: u64,
    db: Arc<NyroDB>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter.acquire().await.unwrap();

    match db.get_raw(&model_name, id).await {
        Ok(Some(data)) => Ok(Box::new(reply::json(&data))),
        Ok(None) => {
            Logger::warn_with_config(
                &db.get_config().logging,
                &format!("'{}' not found: ID {}", model_name, id),
            );
            Ok(Box::new(reply::with_status(
                "Not found",
                StatusCode::NOT_FOUND,
            )))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!("Failed to get from '{}' ID {}: {}", model_name, id, e),
            );
            Ok(Box::new(reply::with_status(
                format!("{{\"error\":\"{}\"}}", e),
                StatusCode::BAD_REQUEST,
            )))
        }
    }
}

pub async fn query_handler(model_name: String, db: Arc<NyroDB>) -> Result<impl Reply, Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter.acquire().await.unwrap();
    match db.query_raw(&model_name).await {
        Ok(results) => {
            Logger::info_with_config(
                &db.get_config().logging,
                &format!("Queried {} items from '{}'", results.len(), model_name),
            );
            Ok(warp::reply::json(&results))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!("Failed to query '{}': {}", model_name, e),
            );
            Ok(warp::reply::json(
                &serde_json::json!({ "error": e.to_string() }),
            ))
        }
    }
}

pub async fn query_field_handler(
    model_name: String,
    field: String,
    value: String,
    db: Arc<NyroDB>,
) -> Result<impl Reply, Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter.acquire().await.unwrap();
    match db.query_by_field_raw(&model_name, &field, &value).await {
        Ok(results) => {
            Logger::info_with_config(
                &db.get_config().logging,
                &format!(
                    "Queried {} items from '{}' by field '{}'='{}'",
                    results.len(),
                    model_name,
                    field,
                    value
                ),
            );
            Ok(warp::reply::json(&results))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!(
                    "Failed to query '{}' by field '{}'='{}': {}",
                    model_name, field, value, e
                ),
            );
            Ok(warp::reply::json(
                &serde_json::json!({ "error": e.to_string() }),
            ))
        }
    }
}

pub async fn realtime_handler(ws: warp::ws::Ws, db: Arc<NyroDB>) -> Result<impl Reply, Rejection> {
    let tx = db.real_time_tx.clone();
    Logger::info_with_config(&db.get_config().logging, "Realtime client connected");
    Ok(ws.on_upgrade(move |socket| RealtimeServer::handle_client(socket, db, tx)))
}

pub async fn metrics_handler(db: Arc<NyroDB>) -> Result<Box<dyn Reply>, warp::Rejection> {
    if !db.get_config().metrics.enable {
        return Ok(Box::new(reply::with_status(
            "Metrics are disabled in the configuration.",
            StatusCode::SERVICE_UNAVAILABLE,
        )));
    }
    let metrics = db.get_metrics();
    Logger::info_with_config(&db.get_config().logging, "Metrics requested");
    Ok(Box::new(reply::json(&metrics)))
}

pub async fn benchmark_handler(
    model_name: String,
    operations: u64,
    db: Arc<NyroDB>,
) -> Result<impl Reply, warp::Rejection> {
    let log_config = db.get_config().logging.clone();

    if !db.get_config().models.contains_key(&model_name) {
        return Ok(reply::with_status(
            format!("{{\"error\":\"Model '{}' not found\"}}", model_name),
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
    let mut tasks = Vec::new();

    for i in 0..operations {
        let db_clone = db.clone();
        let model_name_clone = model_name.clone();
        let permit = db
            .get_concurrency_limiter()
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        tasks.push(tokio::spawn(async move {
            let data = serde_json::json!({
                "id": i,
                "email": format!("user{}@test.com", i),
                "hash_password": format!("hash_{}", i),
                "created_at": 0
            });
            let result = db_clone.insert_raw(&model_name_clone, data).await;
            drop(permit);
            result
        }));
    }

    let mut successful_inserts = 0;
    for task in tasks {
        if task.await.is_ok() {
            successful_inserts += 1;
        }
    }

    let insert_duration = start_time.elapsed();
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

    Ok(reply::with_status(
        serde_json::to_string(&serde_json::json!({
            "model": model_name,
            "ops": successful_inserts,
            "duration_s": insert_duration.as_secs_f64(),
            "ops_per_sec": insert_ops_per_sec
        }))
        .unwrap(),
        StatusCode::OK,
    ))
}

pub async fn config_handler(db: Arc<NyroDB>) -> Result<impl Reply, warp::Rejection> {
    let config = db.get_config();
    Logger::info_with_config(&db.get_config().logging, "Configuration requested");
    Ok(reply::json(config))
}

pub async fn models_handler(db: Arc<NyroDB>) -> Result<impl Reply, warp::Rejection> {
    let models: Vec<String> = db.get_config().models.keys().cloned().collect();
    Logger::info_with_config(&db.get_config().logging, "Models list requested");
    Ok(reply::json(&serde_json::json!({
        "models": models
    })))
}
