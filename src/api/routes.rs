use std::convert::Infallible;
use std::sync::Arc;
use warp::http::StatusCode;
use warp::{Filter, Rejection, Reply};

use crate::api::handlers;
use crate::database::NyroDB;

#[derive(Debug)]
struct AuthError;
impl warp::reject::Reject for AuthError {}

fn with_auth(db: Arc<NyroDB>) -> impl Filter<Extract = (), Error = warp::Rejection> + Clone {
    warp::header::optional::<String>("x-api-key")
        .and(warp::any().map(move || db.clone()))
        .and_then(|key: Option<String>, db: Arc<NyroDB>| async move {
            let config = db.get_config();
            if !config.security.enable_auth {
                return Ok(());
            }
            match key {
                Some(k) if k == config.security.api_key => Ok(()),
                _ => Err(warp::reject::custom(AuthError)),
            }
        })
        .untuple_one()
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    if err.find::<AuthError>().is_some() {
        Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"error": "Unauthorized"})),
            StatusCode::UNAUTHORIZED,
        ))
    } else if err.is_not_found() {
        Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"error": "Not Found"})),
            StatusCode::NOT_FOUND,
        ))
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({"error": "Internal Server Error"})),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

pub fn create_routes(
    db: Arc<NyroDB>,
) -> impl Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    let db_clone = db.clone();
    let db_filter = warp::any().map(move || db_clone.clone());

    let insert_route = warp::path!("insert" / String)
        .and(warp::post())
        .and(warp::body::json())
        .and(db_filter.clone())
        .and_then(handlers::insert_handler);

    let get_route = warp::path!("get" / String / u64)
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(handlers::get_handler);

    let query_route = warp::path!("query" / String)
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(handlers::query_handler);

    let metrics_route = warp::path!("metrics")
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(handlers::metrics_handler);

    let benchmark_route = warp::path!("benchmark" / String / u64)
        .and(warp::post())
        .and(db_filter.clone())
        .and_then(handlers::benchmark_handler);

    let config_route = warp::path!("config")
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(handlers::config_handler);

    let models_route = warp::path!("models")
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(handlers::models_handler);

    let query_field_route = warp::path!("query" / String / String / String)
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(handlers::query_field_handler);

    let realtime_route = warp::path("ws")
        .and(warp::ws())
        .and(db_filter.clone())
        .and_then(handlers::realtime_handler);

    let auth = with_auth(db.clone());

    let routes = insert_route
        .or(get_route)
        .or(query_route)
        .or(query_field_route)
        .or(realtime_route)
        .or(metrics_route)
        .or(benchmark_route)
        .or(config_route)
        .or(models_route);

    auth.and(routes).recover(handle_rejection)
}
