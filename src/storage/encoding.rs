use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

use crate::models::{LogEntry, Operation};
use crate::storage::index::CachedEntry;

const ENTRY_MAGIC: &[u8; 4] = b"NYR1";
const HEADER_SIZE: usize = ENTRY_MAGIC.len() + 8 + 1;

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
    pub(crate) cache_entry: CachedEntry,
}

pub(crate) struct IndexData {
    pub(crate) id: u64,
    pub(crate) fields: Vec<(String, String)>,
}

pub(crate) fn encode_entry(
    entry: &LogEntry<Value>,
    indexed_fields: &HashSet<String>,
) -> Result<EncodedEntry> {
    let json_data = serde_json::to_vec(&entry.data)?;
    let operation = operation_to_u8(&entry.operation);
    let data = encode_raw_entry(entry.timestamp, operation, &json_data);
    let size = u32::try_from(data.len())
        .map_err(|_| anyhow::anyhow!("Serialized entry is larger than u32::MAX"))?;

    Ok(EncodedEntry {
        data,
        size,
        index_data: build_index_data(&entry.data, indexed_fields),
        cache_entry: CachedEntry {
            timestamp: entry.timestamp,
            operation,
            data: Arc::from(json_data),
        },
    })
}

pub(crate) fn decode_raw_entry(data: &[u8]) -> Result<RawEntry> {
    if data.starts_with(ENTRY_MAGIC) {
        return decode_current_raw_entry(data);
    }
    bincode::deserialize(data).map_err(Into::into)
}

fn encode_raw_entry(timestamp: u64, operation: u8, json_data: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(HEADER_SIZE + json_data.len());
    data.extend_from_slice(ENTRY_MAGIC);
    data.extend_from_slice(&timestamp.to_le_bytes());
    data.push(operation);
    data.extend_from_slice(json_data);
    data
}

fn decode_current_raw_entry(data: &[u8]) -> Result<RawEntry> {
    if data.len() < HEADER_SIZE {
        return Err(anyhow::anyhow!("Corrupt log entry header"));
    }

    let timestamp_start = ENTRY_MAGIC.len();
    let timestamp_end = timestamp_start + 8;
    let timestamp_bytes: [u8; 8] = data[timestamp_start..timestamp_end]
        .try_into()
        .map_err(|_| anyhow::anyhow!("Corrupt log entry timestamp"))?;

    Ok(RawEntry {
        timestamp: u64::from_le_bytes(timestamp_bytes),
        operation: data[timestamp_end],
        data: data[HEADER_SIZE..].to_vec(),
    })
}

fn operation_to_u8(operation: &Operation) -> u8 {
    match operation {
        Operation::Insert => 0,
        Operation::Update => 1,
        Operation::Delete => 2,
    }
}

pub(crate) fn operation_from_u8(operation: u8) -> Result<Operation> {
    match operation {
        0 => Ok(Operation::Insert),
        1 => Ok(Operation::Update),
        2 => Ok(Operation::Delete),
        invalid => Err(anyhow::anyhow!("Invalid log operation byte: {}", invalid)),
    }
}

fn build_index_data(data: &Value, indexed_fields: &HashSet<String>) -> Option<IndexData> {
    let id = data.get("id").and_then(|value| value.as_u64())?;
    let fields = data
        .as_object()
        .map(|object| {
            object
                .iter()
                .filter(|(field, _)| indexed_fields.contains(field.as_str()))
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

#[cfg(test)]
mod tests {
    use super::{decode_raw_entry, encode_raw_entry, RawEntry};

    #[test]
    fn decodes_current_raw_entry_format() -> anyhow::Result<()> {
        let encoded = encode_raw_entry(123, 0, br#"{"id":1}"#);
        let decoded = decode_raw_entry(&encoded)?;

        assert_eq!(decoded.timestamp, 123);
        assert_eq!(decoded.operation, 0);
        assert_eq!(decoded.data, br#"{"id":1}"#);
        Ok(())
    }

    #[test]
    fn decodes_legacy_bincode_raw_entry_format() -> anyhow::Result<()> {
        let encoded = bincode::serialize(&RawEntry {
            timestamp: 456,
            operation: 1,
            data: br#"{"id":2}"#.to_vec(),
        })?;
        let decoded = decode_raw_entry(&encoded)?;

        assert_eq!(decoded.timestamp, 456);
        assert_eq!(decoded.operation, 1);
        assert_eq!(decoded.data, br#"{"id":2}"#);
        Ok(())
    }
}
