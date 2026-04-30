use serde_json::Value;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::{reply, Rejection, Reply};

use crate::api::realtime::RealtimeServer;
use crate::database::NyroDB;
use crate::utils::logger::Logger;

pub async fn insert_handler(
    model_name: String,
    data: Value,
    db: Arc<NyroDB>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter
        .acquire()
        .await
        .map_err(|_| warp::reject::reject())?;

    match db.insert_raw(&model_name, data).await {
        Ok(id) => {
            Logger::info_with_config(
                &db.get_config().logging,
                &format!("Inserted into '{}': ID {}", model_name, id),
            );
            Ok(json_status(
                serde_json::json!({ "id": id }),
                StatusCode::CREATED,
            ))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!("Failed to insert into '{}': {}", model_name, e),
            );
            Ok(error_status(e.to_string(), StatusCode::BAD_REQUEST))
        }
    }
}

pub async fn insert_many_handler(
    model_name: String,
    rows: Vec<Value>,
    db: Arc<NyroDB>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter
        .acquire()
        .await
        .map_err(|_| warp::reject::reject())?;

    match db.insert_many_raw(&model_name, rows).await {
        Ok(ids) => {
            Logger::info_with_config(
                &db.get_config().logging,
                &format!("Inserted {} rows into '{}'", ids.len(), model_name),
            );
            Ok(json_status(
                serde_json::json!({ "count": ids.len(), "ids": ids }),
                StatusCode::CREATED,
            ))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!("Failed to insert many rows into '{}': {}", model_name, e),
            );
            Ok(error_status(e.to_string(), StatusCode::BAD_REQUEST))
        }
    }
}

pub async fn get_handler(
    model_name: String,
    id: u64,
    db: Arc<NyroDB>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter
        .acquire()
        .await
        .map_err(|_| warp::reject::reject())?;

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
            Ok(error_status(e.to_string(), StatusCode::BAD_REQUEST))
        }
    }
}

pub async fn query_handler(
    model_name: String,
    db: Arc<NyroDB>,
) -> Result<Box<dyn Reply>, Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter
        .acquire()
        .await
        .map_err(|_| warp::reject::reject())?;
    match db.query_raw(&model_name).await {
        Ok(results) => {
            Logger::info_with_config(
                &db.get_config().logging,
                &format!("Queried {} items from '{}'", results.len(), model_name),
            );
            Ok(Box::new(warp::reply::json(&results)))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!("Failed to query '{}': {}", model_name, e),
            );
            Ok(error_status(e.to_string(), StatusCode::BAD_REQUEST))
        }
    }
}

pub async fn query_field_handler(
    model_name: String,
    field: String,
    value: String,
    db: Arc<NyroDB>,
) -> Result<Box<dyn Reply>, Rejection> {
    let limiter = db.get_concurrency_limiter();
    let _permit = limiter
        .acquire()
        .await
        .map_err(|_| warp::reject::reject())?;
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
            Ok(Box::new(warp::reply::json(&results)))
        }
        Err(e) => {
            Logger::error_with_config(
                &db.get_config().logging,
                &format!(
                    "Failed to query '{}' by field '{}'='{}': {}",
                    model_name, field, value, e
                ),
            );
            Ok(error_status(e.to_string(), StatusCode::BAD_REQUEST))
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

pub async fn config_handler(db: Arc<NyroDB>) -> Result<impl Reply, warp::Rejection> {
    let mut config = db.get_config().clone();
    if !config.security.api_key.is_empty() {
        config.security.api_key = "[redacted]".to_string();
    }
    Logger::info_with_config(&db.get_config().logging, "Configuration requested");
    Ok(reply::json(&config))
}

pub async fn models_handler(db: Arc<NyroDB>) -> Result<impl Reply, warp::Rejection> {
    let models: Vec<String> = db.get_config().models.keys().cloned().collect();
    Logger::info_with_config(&db.get_config().logging, "Models list requested");
    Ok(reply::json(&serde_json::json!({
        "models": models
    })))
}

fn json_status(value: serde_json::Value, status: StatusCode) -> Box<dyn Reply> {
    Box::new(reply::with_status(reply::json(&value), status))
}

fn error_status(message: String, status: StatusCode) -> Box<dyn Reply> {
    json_status(serde_json::json!({ "error": message }), status)
}
