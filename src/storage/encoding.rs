use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::models::{LogEntry, Operation};

#[derive(Serialize, Deserialize)]
pub(crate) struct RawEntry {
    pub(crate) timestamp: u64,
    pub(crate) operation: u8,
    pub(crate) data: Vec<u8>,
}

pub(crate) struct EncodedEntry {
    pub(crate) data: Vec<u8>,
    pub(crate) size: u32,
    pub(crate) index_data: Option<IndexData>,
}

pub(crate) struct IndexData {
    pub(crate) id: u64,
    pub(crate) fields: Vec<(String, String)>,
}

pub(crate) fn encode_entry(entry: &LogEntry<Value>) -> Result<EncodedEntry> {
    let raw_entry = RawEntry {
        timestamp: entry.timestamp,
        operation: operation_to_u8(&entry.operation),
        data: serde_json::to_vec(&entry.data)?,
    };
    let data = bincode::serialize(&raw_entry)?;
    let size = u32::try_from(data.len())
        .map_err(|_| anyhow::anyhow!("Serialized entry is larger than u32::MAX"))?;

    Ok(EncodedEntry {
        data,
        size,
        index_data: build_index_data(&entry.data),
    })
}

fn operation_to_u8(operation: &Operation) -> u8 {
    match operation {
        Operation::Insert => 0,
        Operation::Update => 1,
        Operation::Delete => 2,
    }
}

fn build_index_data(data: &Value) -> Option<IndexData> {
    let id = data.get("id").and_then(|value| value.as_u64())?;
    let fields = data
        .as_object()
        .map(|object| {
            object
                .iter()
                .filter(|(field, _)| field.as_str() != "id")
                .map(|(field, value)| {
                    let value_string = value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string());
                    (field.clone(), value_string)
                })
                .collect()
        })
        .unwrap_or_default();

    Some(IndexData { id, fields })
}
