use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

use crate::models::{LogEntry, Operation};
use crate::storage::index::{CachedData, CachedEntry};
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

#[derive(Clone, Copy)]
pub(crate) enum CacheMode {
    EncodedFrame,
    ParsedValue,
}

pub(crate) fn encode_entry(
    entry: &LogEntry<Value>,
    indexed_fields: &HashSet<String>,
    field_codecs: &[FieldCodec],
    cache_mode: CacheMode,
) -> Result<EncodedEntry> {
    let core = encode_entry_core(
        entry.timestamp,
        operation_to_u8(&entry.operation),
        &entry.data,
        indexed_fields,
        field_codecs,
        false,
    )?;
    let cache_data = match cache_mode {
        CacheMode::EncodedFrame => CachedData::Encoded(Arc::from(core.data.as_slice())),
        CacheMode::ParsedValue => CachedData::Parsed(Arc::new(entry.data.clone())),
    };
    Ok(core.into_encoded_entry(entry.timestamp, cache_data))
}

pub(crate) fn encode_owned_entry(
    entry: LogEntry<Value>,
    indexed_fields: &HashSet<String>,
    field_codecs: &[FieldCodec],
) -> Result<EncodedEntry> {
    let timestamp = entry.timestamp;
    let operation = operation_to_u8(&entry.operation);
    let data = entry.data;
    let core = encode_entry_core(
        timestamp,
        operation,
        &data,
        indexed_fields,
        field_codecs,
        false,
    )?;
    Ok(core.into_encoded_entry(timestamp, CachedData::Parsed(Arc::new(data))))
}

struct EncodedCore {
    data: Vec<u8>,
    size: u32,
    operation: u8,
    index_data: Option<IndexData>,
}

impl EncodedCore {
    fn into_encoded_entry(self, timestamp: u64, cache_data: CachedData) -> EncodedEntry {
        EncodedEntry {
            data: self.data,
            size: self.size,
            index_data: self.index_data,
            cache_entry: CachedEntry {
                timestamp,
                operation: self.operation,
                data: cache_data,
            },
        }
    }
}

fn encode_entry_core(
    timestamp: u64,
    operation: u8,
    entry_data: &Value,
    indexed_fields: &HashSet<String>,
    field_codecs: &[FieldCodec],
    needs_json_cache: bool,
) -> Result<EncodedCore> {
    let typed_payload = encode_typed_payload(entry_data, field_codecs);
    let json_data = if needs_json_cache || typed_payload.is_none() {
        Some(serde_json::to_vec(entry_data)?)
    } else {
        None
    };
    let data = if let Some(payload) = typed_payload {
        encode_typed_raw_entry(timestamp, operation, &payload)
    } else {
        let json = json_data
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Missing JSON fallback payload"))?;
        encode_json_raw_entry(timestamp, operation, json)
    };
    let size = u32::try_from(data.len())
        .map_err(|_| anyhow::anyhow!("Serialized entry is larger than u32::MAX"))?;

    Ok(EncodedCore {
        data,
        size,
        operation,
        index_data: build_index_data(entry_data, indexed_fields),
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

pub(super) fn encode_json_raw_entry(timestamp: u64, operation: u8, json_data: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(JSON_HEADER_SIZE + json_data.len());
    data.extend_from_slice(JSON_ENTRY_MAGIC);
    data.extend_from_slice(&timestamp.to_le_bytes());
    data.push(operation);
    data.extend_from_slice(json_data);
    data
}

pub(super) fn encode_typed_raw_entry(timestamp: u64, operation: u8, payload: &[u8]) -> Vec<u8> {
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
