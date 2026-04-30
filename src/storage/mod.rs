mod encoding;
mod index;
mod rebuild;

use anyhow::Result;
use index::{EntryLocation, IndexedEntry, PrimaryIndex};
use memmap2::MmapOptions;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
};

use crate::config::{LoggingConfig, ModelSchema, StorageConfig};
use crate::models::{LogEntry, Operation};
use crate::utils::logger::Logger;

use dashmap::DashMap;
use encoding::{EncodedEntry, RawEntry};
use std::sync::atomic::{AtomicU64, Ordering};

const PARALLEL_ENCODE_THRESHOLD: usize = 1024;

pub struct LogStorage {
    file: Arc<RwLock<BufWriter<File>>>,
    mmap_file: Option<memmap2::Mmap>,
    sync_on_append: bool,
    indexed_fields: HashSet<String>,
    index: Arc<PrimaryIndex>,
    pub secondary_indices: Arc<DashMap<String, DashMap<String, Vec<u64>>>>,
    pub file_path: String,
    pub current_offset: Arc<AtomicU64>,
}

impl LogStorage {
    pub fn new(
        model_name: &str,
        config: &StorageConfig,
        log_config: &LoggingConfig,
        schema: &ModelSchema,
    ) -> Result<Self> {
        let file_path = format!("{}/{}.log", config.data_dir, model_name);
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            Logger::error_with_config(
                log_config,
                &format!("Failed to create data directory: {}", e),
            );
            e
        })?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&file_path)
            .map_err(|e| {
                Logger::error_with_config(
                    log_config,
                    &format!("Failed to open log file {}: {}", file_path, e),
                );
                e
            })?;

        let writer = BufWriter::with_capacity(config.buffer_size, file);

        let mut storage = Self {
            file: Arc::new(RwLock::new(writer)),
            mmap_file: None,
            sync_on_append: config.sync_interval == 0,
            indexed_fields: schema
                .fields
                .iter()
                .filter(|field| field.indexed && field.name != "id")
                .map(|field| field.name.clone())
                .collect(),
            index: Arc::new(PrimaryIndex::new()),
            secondary_indices: Arc::new(DashMap::new()),
            file_path: file_path.clone(),
            current_offset: Arc::new(AtomicU64::new(0)),
        };

        storage.rebuild_index()?;

        if config.enable_mmap {
            storage.setup_mmap()?;
        }

        let file_ref = Arc::downgrade(&storage.file);
        let sync_log_config = log_config.clone();
        let sync_interval = config.sync_interval;
        if sync_interval > 0 {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(sync_interval));
                loop {
                    interval.tick().await;
                    let Some(file_clone) = file_ref.upgrade() else {
                        break;
                    };
                    if let Ok(mut writer) = file_clone.write() {
                        if let Err(e) = writer.flush().and_then(|_| writer.get_ref().sync_data()) {
                            Logger::error_with_config(
                                &sync_log_config,
                                &format!("Failed to flush storage buffer: {}", e),
                            );
                        }
                    };
                }
            });
        }

        Logger::info_with_config(
            log_config,
            &format!(
                "Initialized storage for model: {} (buffer: {}KB, mmap: {}, sync: {})",
                model_name,
                config.buffer_size / 1024,
                config.enable_mmap,
                if sync_interval == 0 {
                    "every write".to_string()
                } else {
                    format!("{}ms", sync_interval)
                }
            ),
        );

        Ok(storage)
    }

    pub fn shutdown(&self, log_config: &LoggingConfig) -> Result<()> {
        let mut file = self
            .file
            .write()
            .map_err(|_| anyhow::anyhow!("Storage writer lock poisoned"))?;
        file.flush()?;
        file.get_ref().sync_data()?;
        Logger::info_with_config(
            log_config,
            &format!("Flushed pending writes for {}", self.file_path),
        );
        Ok(())
    }

    fn setup_mmap(&mut self) -> Result<()> {
        if Path::new(&self.file_path).exists() {
            let file = File::open(&self.file_path)?;
            if file.metadata()?.len() > 0 {
                self.mmap_file = Some(unsafe { MmapOptions::new().map(&file)? });
            }
        }
        Ok(())
    }

    pub fn append(&self, entry: &LogEntry<Value>) -> Result<()> {
        self.append_many(&[entry])
    }

    pub fn append_many(&self, entries: &[&LogEntry<Value>]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let encoded_entries = self.encode_entries(entries)?;
        let mut file = self
            .file
            .write()
            .map_err(|_| anyhow::anyhow!("Storage writer lock poisoned"))?;
        let mut offset = self.current_offset.load(Ordering::Acquire);
        let mut committed_entries = Vec::with_capacity(encoded_entries.len());

        for encoded_entry in encoded_entries {
            let entry_size = encoded_entry.size;
            file.write_all(&encoded_entry.size.to_le_bytes())?;
            file.write_all(&encoded_entry.data)?;
            committed_entries.push((offset, encoded_entry));
            offset += 4 + entry_size as u64;
        }
        if self.sync_on_append {
            file.flush()?;
            file.get_ref().sync_data()?;
        }

        for (entry_offset, encoded_entry) in &committed_entries {
            self.insert_indexes(*entry_offset, encoded_entry);
        }
        self.current_offset.store(offset, Ordering::SeqCst);

        Ok(())
    }

    fn encode_entries(&self, entries: &[&LogEntry<Value>]) -> Result<Vec<EncodedEntry>> {
        if entries.len() >= PARALLEL_ENCODE_THRESHOLD {
            return entries
                .par_iter()
                .map(|entry| encoding::encode_entry(entry, &self.indexed_fields))
                .collect::<Result<Vec<_>>>();
        }

        entries
            .iter()
            .map(|entry| encoding::encode_entry(entry, &self.indexed_fields))
            .collect::<Result<Vec<_>>>()
    }

    fn insert_indexes(&self, offset: u64, encoded_entry: &EncodedEntry) {
        if let Some(index_data) = &encoded_entry.index_data {
            self.index.insert(
                index_data.id,
                IndexedEntry {
                    location: EntryLocation {
                        offset,
                        size: encoded_entry.size,
                    },
                    cache: encoded_entry.cache_entry.clone(),
                },
            );
            for (field, value) in &index_data.fields {
                let field_idx = self.secondary_indices.entry(field.clone()).or_default();
                field_idx
                    .entry(value.clone())
                    .or_default()
                    .push(index_data.id);
            }
        }
    }

    pub fn get<T: for<'de> Deserialize<'de>>(&self, id: u64) -> Result<Option<LogEntry<T>>> {
        if let Some(indexed_entry) = self.index.get(id) {
            let location = indexed_entry.location;
            let raw_entry = if let Some(ref mmap) = self.mmap_file {
                let start = location.offset as usize;
                let end = start + 4 + location.size as usize;
                if end <= mmap.len() {
                    let data = &mmap[start + 4..end];
                    bincode::deserialize::<RawEntry>(data)?
                } else {
                    return self.decode_cached_entry(indexed_entry);
                }
            } else {
                return self.decode_cached_entry(indexed_entry);
            };

            let data: T = serde_json::from_slice(&raw_entry.data)?;
            let operation = operation_from_u8(raw_entry.operation)?;

            Ok(Some(LogEntry {
                timestamp: raw_entry.timestamp,
                operation,
                data,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_value(&self, id: u64) -> Result<Option<Value>> {
        self.index
            .get(id)
            .map(|entry| serde_json::from_slice(&entry.cache.data).map_err(Into::into))
            .transpose()
    }

    fn decode_cached_entry<T: for<'de> Deserialize<'de>>(
        &self,
        indexed_entry: IndexedEntry,
    ) -> Result<Option<LogEntry<T>>> {
        let data = serde_json::from_slice(&indexed_entry.cache.data)?;
        let operation = operation_from_u8(indexed_entry.cache.operation)?;

        Ok(Some(LogEntry {
            timestamp: indexed_entry.cache.timestamp,
            operation,
            data,
        }))
    }

    pub fn get_all<T: for<'de> Deserialize<'de>>(&self) -> Result<Vec<LogEntry<T>>> {
        let mut results = Vec::new();
        for id in self.index.ids() {
            if let Some(entry) = self.get(id)? {
                results.push(entry);
            }
        }
        Ok(results)
    }
}

fn operation_from_u8(operation: u8) -> Result<Operation> {
    match operation {
        0 => Ok(Operation::Insert),
        1 => Ok(Operation::Update),
        2 => Ok(Operation::Delete),
        invalid => Err(anyhow::anyhow!("Invalid log operation byte: {}", invalid)),
    }
}
