use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use std::io::{BufWriter, Write};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::encoding::{self, CacheMode, EncodedEntry};
use super::index::{EntryLocation, IndexedEntry, PrimaryIndex};
use super::LogStorage;
use crate::config::{LoggingConfig, ModelSchema, StorageConfig};
use crate::models::{LogEntry, Operation};
use crate::utils::benchmark::rate;

#[derive(Debug, Serialize)]
pub struct StorageStageReport {
    pub operations: u64,
    pub encoded_bytes: u64,
    pub encode_only: StageMeasurement,
    pub log_only: StageMeasurement,
    pub index_publish_only: StageMeasurement,
    pub append_entries: StageMeasurement,
    pub log_file_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct StageMeasurement {
    pub duration_s: f64,
    pub ops_per_sec: f64,
    pub bytes_per_sec: f64,
}

pub fn run_storage_stage_benchmark(
    rows: Vec<Value>,
    storage_config: &StorageConfig,
    logging_config: &LoggingConfig,
    schema: &ModelSchema,
) -> Result<StorageStageReport> {
    let entries = rows
        .into_iter()
        .map(|data| LogEntry {
            timestamp: 1,
            operation: Operation::Insert,
            data,
        })
        .collect::<Vec<_>>();
    let operations = entries.len() as u64;

    let storage = LogStorage::new("user", storage_config, logging_config, schema)?;

    let encode_start = Instant::now();
    let encoded_entries = encode_entries(&storage, &entries)?;
    let encode_duration = encode_start.elapsed();
    let encoded_bytes = total_encoded_bytes(&encoded_entries);

    let log_start = Instant::now();
    write_encoded_log_only(storage_config, &encoded_entries)?;
    let log_duration = log_start.elapsed();

    let index_start = Instant::now();
    publish_index_only(&encoded_entries);
    let index_duration = index_start.elapsed();

    let append_start = Instant::now();
    storage.append_entries(&entries)?;
    let append_duration = append_start.elapsed();

    Ok(StorageStageReport {
        operations,
        encoded_bytes,
        encode_only: measurement(operations, encoded_bytes, encode_duration),
        log_only: measurement(operations, encoded_bytes, log_duration),
        index_publish_only: measurement(operations, encoded_bytes, index_duration),
        append_entries: measurement(operations, encoded_bytes, append_duration),
        log_file_bytes: std::fs::metadata(&storage.file_path)?.len(),
    })
}

fn encode_entries(storage: &LogStorage, entries: &[LogEntry<Value>]) -> Result<Vec<EncodedEntry>> {
    entries
        .iter()
        .map(|entry| {
            encoding::encode_entry(
                entry,
                &storage.indexed_fields,
                &storage.field_codecs,
                CacheMode::EncodedFrame,
            )
        })
        .collect()
}

fn write_encoded_log_only(
    storage_config: &StorageConfig,
    encoded_entries: &[EncodedEntry],
) -> Result<()> {
    let path = std::path::Path::new(&storage_config.data_dir).join("log-only.log");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let mut writer = BufWriter::with_capacity(storage_config.buffer_size, file);
    for entry in encoded_entries {
        writer.write_all(&entry.size.to_le_bytes())?;
        writer.write_all(&entry.data)?;
    }
    Ok(())
}

fn publish_index_only(encoded_entries: &[EncodedEntry]) {
    let index = PrimaryIndex::new();
    let mut offset = 0u64;
    let entries = encoded_entries
        .iter()
        .filter_map(|entry| {
            let index_data = entry.index_data.as_ref()?;
            let indexed_entry = IndexedEntry {
                location: EntryLocation {
                    offset,
                    size: entry.size,
                },
                cache: entry.cache_entry.clone(),
            };
            offset += 4 + entry.size as u64;
            Some((index_data.id, indexed_entry))
        })
        .collect::<Vec<_>>();

    index.insert_many(entries);
    std::sync::atomic::compiler_fence(Ordering::SeqCst);
}

fn total_encoded_bytes(encoded_entries: &[EncodedEntry]) -> u64 {
    encoded_entries
        .iter()
        .map(|entry| u64::from(entry.size) + 4)
        .sum()
}

fn measurement(operations: u64, bytes: u64, duration: Duration) -> StageMeasurement {
    StageMeasurement {
        duration_s: duration.as_secs_f64(),
        ops_per_sec: rate(operations, duration),
        bytes_per_sec: rate(bytes, duration),
    }
}
