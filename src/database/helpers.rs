use anyhow::Result;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::LoggingConfig;
use crate::models::LogEntry;
use crate::utils::logger::Logger;

pub(crate) fn field_matches(data: &Value, field: &str, expected: &str) -> bool {
    data.get(field)
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
                == expected
        })
        .unwrap_or(false)
}

pub(crate) fn current_unix_millis() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("System clock is before UNIX epoch: {}", error))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| anyhow::anyhow!("Current UNIX timestamp does not fit into u64 millis"))
}

pub(crate) fn entry_id(entry: &LogEntry<Value>) -> Result<u64> {
    entry
        .data
        .get("id")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid 'id' field"))
}

pub(crate) fn publish_insert_event(
    real_time_tx: &tokio::sync::broadcast::Sender<String>,
    log_config: &LoggingConfig,
    model_name: &str,
    entry: &LogEntry<Value>,
) {
    if real_time_tx.receiver_count() == 0 {
        return;
    }

    match serde_json::to_string(&entry.data) {
        Ok(data) => {
            let _ = real_time_tx.send(format!("INSERT:{}:{}", model_name, data));
        }
        Err(error) => Logger::error_with_config(
            log_config,
            &format!(
                "Failed to serialize realtime event for {}: {}",
                model_name, error
            ),
        ),
    }
}
