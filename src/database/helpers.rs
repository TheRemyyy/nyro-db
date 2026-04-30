use anyhow::Result;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::models::LogEntry;

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
