use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

use crate::models::{LogEntry, Operation};
use crate::storage::index::CachedEntry;
use crate::storage::typed::{decode_typed_payload, encode_typed_payload, FieldCodec};

const JSON_ENTRY_MAGIC: &[u8; 4] = b"NYR1";
const TYPED_ENTRY_MAGIC: &[u8; 4] = b"NYR2";
const JSON_HEADER_SIZE: usize = JSON_ENTRY_MAGIC.len() + 8 + 1;
const TYPED_HEADER_SIZE: usize = TYPED_ENTRY_MAGIC.len() + 8 + 1;

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
    field_codecs: &[FieldCodec],
) -> Result<EncodedEntry> {
    let json_data = serde_json::to_vec(&entry.data)?;
    let operation = operation_to_u8(&entry.operation);
    let data = encode_typed_payload(&entry.data, field_codecs)
        .map(|payload| encode_typed_raw_entry(entry.timestamp, operation, &payload))
        .unwrap_or_else(|| encode_json_raw_entry(entry.timestamp, operation, &json_data));
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

pub(crate) fn decode_raw_entry(data: &[u8], field_codecs: &[FieldCodec]) -> Result<RawEntry> {
    if data.starts_with(JSON_ENTRY_MAGIC) {
        return decode_json_raw_entry(data);
    }
    if data.starts_with(TYPED_ENTRY_MAGIC) {
        return decode_typed_raw_entry(data, field_codecs);
    }
    bincode::deserialize(data).map_err(Into::into)
}

fn encode_json_raw_entry(timestamp: u64, operation: u8, json_data: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(JSON_HEADER_SIZE + json_data.len());
    data.extend_from_slice(JSON_ENTRY_MAGIC);
    data.extend_from_slice(&timestamp.to_le_bytes());
    data.push(operation);
    data.extend_from_slice(json_data);
    data
}

fn encode_typed_raw_entry(timestamp: u64, operation: u8, payload: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(TYPED_HEADER_SIZE + payload.len());
    data.extend_from_slice(TYPED_ENTRY_MAGIC);
    data.extend_from_slice(&timestamp.to_le_bytes());
    data.push(operation);
    data.extend_from_slice(payload);
    data
}

fn decode_json_raw_entry(data: &[u8]) -> Result<RawEntry> {
    let (timestamp, operation) = decode_header(data, JSON_HEADER_SIZE)?;
    Ok(RawEntry {
        timestamp,
        operation,
        data: data[JSON_HEADER_SIZE..].to_vec(),
    })
}

fn decode_typed_raw_entry(data: &[u8], field_codecs: &[FieldCodec]) -> Result<RawEntry> {
    let (timestamp, operation) = decode_header(data, TYPED_HEADER_SIZE)?;
    let json_data = decode_typed_payload(&data[TYPED_HEADER_SIZE..], field_codecs)?;
    Ok(RawEntry {
        timestamp,
        operation,
        data: json_data,
    })
}

fn decode_header(data: &[u8], header_size: usize) -> Result<(u64, u8)> {
    if data.len() < header_size {
        return Err(anyhow::anyhow!("Corrupt log entry header"));
    }

    let timestamp_start = 4;
    let timestamp_end = timestamp_start + 8;
    let timestamp_bytes: [u8; 8] = data[timestamp_start..timestamp_end]
        .try_into()
        .map_err(|_| anyhow::anyhow!("Corrupt log entry timestamp"))?;

    Ok((u64::from_le_bytes(timestamp_bytes), data[timestamp_end]))
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
    if indexed_fields.is_empty() {
        return Some(IndexData {
            id,
            fields: Vec::new(),
        });
    }

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
    use super::{decode_raw_entry, encode_json_raw_entry, encode_typed_raw_entry, RawEntry};
    use crate::config::{ModelField, ModelSchema};
    use crate::storage::typed::{encode_typed_payload, field_codecs_from_schema};
    use serde_json::{json, Value};

    #[test]
    fn decodes_json_raw_entry_format() -> anyhow::Result<()> {
        let encoded = encode_json_raw_entry(123, 0, br#"{"id":1}"#);
        let decoded = decode_raw_entry(&encoded, &[])?;

        assert_eq!(decoded.timestamp, 123);
        assert_eq!(decoded.operation, 0);
        assert_eq!(decoded.data, br#"{"id":1}"#);
        Ok(())
    }

    #[test]
    fn decodes_typed_raw_entry_format() -> anyhow::Result<()> {
        let codecs = field_codecs_from_schema(&test_schema());
        let value = json!({
            "id": 7,
            "email": "a@nyro.local",
            "hash_password": "hash_7",
            "created_at": 99
        });
        let payload = encode_typed_payload(&value, &codecs)
            .ok_or_else(|| anyhow::anyhow!("expected typed payload"))?;
        let encoded = encode_typed_raw_entry(456, 0, &payload);
        let decoded = decode_raw_entry(&encoded, &codecs)?;
        let decoded_value: Value = serde_json::from_slice(&decoded.data)?;

        assert_eq!(decoded.timestamp, 456);
        assert_eq!(decoded.operation, 0);
        assert_eq!(decoded_value, value);
        Ok(())
    }

    #[test]
    fn decodes_legacy_bincode_raw_entry_format() -> anyhow::Result<()> {
        let encoded = bincode::serialize(&RawEntry {
            timestamp: 789,
            operation: 1,
            data: br#"{"id":2}"#.to_vec(),
        })?;
        let decoded = decode_raw_entry(&encoded, &[])?;

        assert_eq!(decoded.timestamp, 789);
        assert_eq!(decoded.operation, 1);
        assert_eq!(decoded.data, br#"{"id":2}"#);
        Ok(())
    }

    fn test_schema() -> ModelSchema {
        ModelSchema {
            fields: vec![
                field("id", "u64"),
                field("email", "string"),
                field("hash_password", "string"),
                field("created_at", "u64"),
            ],
        }
    }

    fn field(name: &str, field_type: &str) -> ModelField {
        ModelField {
            name: name.to_string(),
            field_type: field_type.to_string(),
            required: true,
            indexed: false,
        }
    }
}
