mod encoding;

use anyhow::Result;
use memmap2::MmapOptions;
use serde::Deserialize;
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::config::{LoggingConfig, StorageConfig};
use crate::models::{LogEntry, Operation};
use crate::utils::logger::Logger;

use dashmap::DashMap;
use encoding::{EncodedEntry, RawEntry};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LogStorage {
    file: Arc<RwLock<BufWriter<File>>>,
    read_file: Arc<File>,
    mmap_file: Option<memmap2::Mmap>,
    sync_on_append: bool,
    pub index: Arc<DashMap<u64, (u64, u32)>>,
    pub secondary_indices: Arc<DashMap<String, DashMap<String, Vec<u64>>>>,
    pub file_path: String,
    pub current_offset: Arc<AtomicU64>,
}

impl LogStorage {
    pub fn new(
        model_name: &str,
        config: &StorageConfig,
        log_config: &LoggingConfig,
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

        let read_file = Arc::new(file.try_clone()?);
        let writer = BufWriter::with_capacity(config.buffer_size, file);

        let mut storage = Self {
            file: Arc::new(RwLock::new(writer)),
            read_file,
            mmap_file: None,
            sync_on_append: config.sync_interval == 0,
            index: Arc::new(DashMap::new()),
            secondary_indices: Arc::new(DashMap::new()),
            file_path: file_path.clone(),
            current_offset: Arc::new(AtomicU64::new(0)),
        };

        storage.rebuild_index()?;

        if config.enable_mmap {
            storage.setup_mmap()?;
        }

        let file_clone = Arc::clone(&storage.file);
        let sync_interval = config.sync_interval;
        if sync_interval > 0 {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(sync_interval));
                loop {
                    interval.tick().await;
                    if let Ok(mut writer) = file_clone.write() {
                        if let Err(e) = writer.flush().and_then(|_| writer.get_ref().sync_data()) {
                            eprintln!("[ERROR] Failed to flush storage buffer: {}", e);
                        }
                    }
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

        let encoded_entries = entries
            .iter()
            .map(|entry| encoding::encode_entry(entry))
            .collect::<Result<Vec<_>>>()?;
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
        file.flush()?;
        if self.sync_on_append {
            file.get_ref().sync_data()?;
        }

        for (entry_offset, encoded_entry) in &committed_entries {
            self.insert_indexes(*entry_offset, encoded_entry);
        }
        self.current_offset.store(offset, Ordering::SeqCst);

        Ok(())
    }

    fn insert_indexes(&self, offset: u64, encoded_entry: &EncodedEntry) {
        if let Some(index_data) = &encoded_entry.index_data {
            self.index
                .insert(index_data.id, (offset, encoded_entry.size));
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
        if let Some(entry_ref) = self.index.get(&id) {
            let (offset, size) = *entry_ref;
            let raw_entry = if let Some(ref mmap) = self.mmap_file {
                let start = offset as usize;
                let end = start + 4 + size as usize;
                if end <= mmap.len() {
                    let data = &mmap[start + 4..end];
                    bincode::deserialize::<RawEntry>(data)?
                } else {
                    self.read_raw_entry(offset, size)?
                }
            } else {
                self.read_raw_entry(offset, size)?
            };

            let data: T = serde_json::from_slice(&raw_entry.data)?;
            let operation = match raw_entry.operation {
                0 => Operation::Insert,
                1 => Operation::Update,
                2 => Operation::Delete,
                _ => Operation::Insert,
            };

            Ok(Some(LogEntry {
                timestamp: raw_entry.timestamp,
                operation,
                data,
            }))
        } else {
            Ok(None)
        }
    }

    fn read_raw_entry(&self, offset: u64, size: u32) -> Result<RawEntry> {
        let mut buffer = vec![0u8; size as usize];
        self.read_file.read_exact_at(&mut buffer, offset + 4)?;
        Ok(bincode::deserialize::<RawEntry>(&buffer)?)
    }

    pub fn get_all<T: for<'de> Deserialize<'de>>(&self) -> Result<Vec<LogEntry<T>>> {
        let mut results = Vec::new();
        for item in self.index.iter() {
            if let Ok(Some(entry)) = self.get(*item.key()) {
                results.push(entry);
            }
        }
        Ok(results)
    }

    fn rebuild_index(&self) -> Result<()> {
        if !Path::new(&self.file_path).exists() {
            return Ok(());
        }

        let mut file = File::open(&self.file_path)?;
        self.current_offset.store(0, Ordering::Release);
        self.index.clear();
        self.secondary_indices.clear(); // Clear secondary indices on rebuild

        loop {
            let offset = self.current_offset.load(Ordering::Acquire);
            let mut size_bytes = [0u8; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => {
                    let size = u32::from_le_bytes(size_bytes);
                    let mut buffer = vec![0u8; size as usize];
                    file.read_exact(&mut buffer)?;

                    let raw_entry: RawEntry = bincode::deserialize(&buffer)?;
                    let data: Value = serde_json::from_slice(&raw_entry.data)?;

                    if let Some(id) = data.get("id").and_then(|v| v.as_u64()) {
                        self.index.insert(id, (offset, size));

                        // Rebuild secondary indices
                        if let Some(obj) = data.as_object() {
                            for (field, value) in obj {
                                if field != "id" {
                                    let value_str = if let Some(s) = value.as_str() {
                                        s.to_string()
                                    } else {
                                        value.to_string()
                                    };
                                    let field_idx =
                                        self.secondary_indices.entry(field.clone()).or_default();
                                    field_idx.entry(value_str).or_default().push(id);
                                }
                            }
                        }
                    }

                    self.current_offset
                        .fetch_add(4 + size as u64, Ordering::SeqCst);
                }
                Err(_) => break,
            }
        }

        Ok(())
    }
}
