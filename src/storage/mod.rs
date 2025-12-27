use anyhow::Result;
use memmap2::MmapOptions;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::config::{LoggingConfig, StorageConfig};
use crate::models::LogEntry;
use crate::utils::logger::Logger;

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Serialize, Deserialize)]
pub struct RawEntry {
    pub timestamp: u64,
    pub operation: u8,
    pub data: Vec<u8>,
}

pub struct LogStorage {
    file: Arc<RwLock<BufWriter<File>>>,
    mmap_file: Option<memmap2::Mmap>,
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

        let writer = BufWriter::with_capacity(config.buffer_size, file);

        let mut storage = Self {
            file: Arc::new(RwLock::new(writer)),
            mmap_file: None,
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
                        if let Err(e) = writer.flush() {
                            eprintln!("[ERROR] Failed to flush storage buffer: {}", e);
                        }
                    }
                }
            });
        }

        Logger::info_with_config(
            log_config,
            &format!(
                "Initialized storage for model: {} (buffer: {}KB, mmap: {}, sync: {}ms)",
                model_name,
                config.buffer_size / 1024,
                config.enable_mmap,
                sync_interval
            ),
        );

        Ok(storage)
    }

    pub fn shutdown(&self, log_config: &LoggingConfig) -> Result<()> {
        if let Ok(mut file) = self.file.write() {
            file.flush()?;
            Logger::info_with_config(
                log_config,
                &format!("Flushed pending writes for {}", self.file_path),
            );
        }
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
        let raw_entry = RawEntry {
            timestamp: entry.timestamp,
            operation: match entry.operation {
                crate::models::Operation::Insert => 0,
                crate::models::Operation::Update => 1,
                crate::models::Operation::Delete => 2,
            },
            data: serde_json::to_vec(&entry.data)?,
        };

        let serialized = bincode::serialize(&raw_entry)?;
        let size = serialized.len() as u32;
        let mut file = self.file.write().unwrap();
        let offset = self.current_offset.load(Ordering::Acquire);

        file.write_all(&size.to_le_bytes())?;
        file.write_all(&serialized)?;

        if let Some(id) = entry.data.get("id").and_then(|v| v.as_u64()) {
            self.index.insert(id, (offset, size));

            // Update secondary indices
            if let Some(obj) = entry.data.as_object() {
                for (field, value) in obj {
                    if field != "id" {
                        let value_str = if let Some(s) = value.as_str() {
                            s.to_string()
                        } else {
                            value.to_string()
                        };
                        let field_idx = self.secondary_indices.entry(field.clone()).or_default();
                        field_idx.entry(value_str).or_default().push(id);
                    }
                }
            }
        }

        self.current_offset
            .fetch_add(4 + size as u64, Ordering::SeqCst);

        Ok(())
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
                    let mut file = File::open(&self.file_path)?;
                    file.seek(SeekFrom::Start(offset + 4))?;
                    let mut buffer = vec![0u8; size as usize];
                    file.read_exact(&mut buffer)?;
                    bincode::deserialize::<RawEntry>(&buffer)?
                }
            } else {
                let mut file = File::open(&self.file_path)?;
                file.seek(SeekFrom::Start(offset + 4))?;
                let mut buffer = vec![0u8; size as usize];
                file.read_exact(&mut buffer)?;
                bincode::deserialize::<RawEntry>(&buffer)?
            };

            let data: T = serde_json::from_slice(&raw_entry.data)?;
            let operation = match raw_entry.operation {
                0 => crate::models::Operation::Insert,
                1 => crate::models::Operation::Update,
                2 => crate::models::Operation::Delete,
                _ => crate::models::Operation::Insert,
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
