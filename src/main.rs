use anyhow::Result;
use nyrodb::api;
use nyrodb::config::NyroConfig;
use nyrodb::database::NyroDB;
use nyrodb::utils::logger::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "--generate-config" {
        NyroConfig::save_default_config()?;
        println!("Default configuration generated: nyrodb.toml");
        return Ok(());
    }

    let config = NyroConfig::load()?;
    config.validate()?;

    let db = Arc::new(NyroDB::new(config.clone()));
    let db_shutdown = db.clone();

    let routes = api::routes::create_routes(db.clone());

    if config.metrics.enable {
        let db_clone = db.clone();
        let log_config = config.logging.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(config.metrics.report_interval));
            loop {
                interval.tick().await;
                let metrics = db_clone.get_metrics();
                if metrics.total_operations > 0 {
                    Logger::info_with_config(&log_config, &format!("Stats: {} inserts ({:.0} ops/sec), {} gets ({:.0} ops/sec), avg latency: {:.1}μs", metrics.total_inserts, metrics.inserts_per_sec, metrics.total_gets, metrics.gets_per_sec, metrics.avg_insert_latency_ns / 1000.0));
                }
            }
        });
    }

    let db_shutdown_clone = db.clone();
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                Logger::shutdown_with_config(
                    &db_shutdown_clone.get_config().logging,
                    "Received Ctrl+C signal",
                );
                if let Err(e) = db_shutdown.shutdown().await {
                    Logger::error_with_config(
                        &db_shutdown_clone.get_config().logging,
                        &format!("Shutdown error: {}", e),
                    );
                }
                std::process::exit(0);
            }
            Err(err) => {
                Logger::error_with_config(
                    &db_shutdown_clone.get_config().logging,
                    &format!("Failed to listen for shutdown signal: {}", err),
                );
            }
        }
    });

    let bind_addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .map_err(|error| anyhow::anyhow!("Invalid server bind address: {}", error))?;

    Logger::info_with_config(
        &config.logging,
        &format!("Server starting on http://{}", bind_addr),
    );
    warp::serve(routes).run(bind_addr).await;
    Ok(())
}
