use anyhow::Result;
use serde_json::Value;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::storage::encoding::decode_raw_entry;
use crate::storage::index::{CachedData, CachedEntry, EntryLocation, IndexedEntry};
use crate::storage::LogStorage;

impl LogStorage {
    pub(super) fn rebuild_index(&self) -> Result<()> {
        if !Path::new(&self.file_path).exists() {
            return Ok(());
        }

        let mut file = std::fs::File::open(&self.file_path)?;
        self.current_offset.store(0, Ordering::Release);
        self.index.clear();
        self.secondary_indices.clear();

        loop {
            let offset = self.current_offset.load(Ordering::Acquire);
            let mut size_bytes = [0u8; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => self.rebuild_entry_indexes(&mut file, offset, size_bytes)?,
                Err(_) => break,
            }
        }
        Ok(())
    }

    fn rebuild_entry_indexes(
        &self,
        file: &mut std::fs::File,
        offset: u64,
        size_bytes: [u8; 4],
    ) -> Result<()> {
        let size = u32::from_le_bytes(size_bytes);
        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer)?;

        let raw_entry = decode_raw_entry(&buffer, &self.field_codecs)?;
        let data: Value = serde_json::from_slice(&raw_entry.data)?;
        if let Some(id) = data.get("id").and_then(|value| value.as_u64()) {
            self.index.insert(
                id,
                IndexedEntry {
                    location: EntryLocation { offset, size },
                    cache: CachedEntry {
                        timestamp: raw_entry.timestamp,
                        operation: raw_entry.operation,
                        data: CachedData::Json(std::sync::Arc::from(raw_entry.data)),
                    },
                },
            );
            self.rebuild_secondary_indexes(id, &data);
        }

        self.current_offset
            .fetch_add(4 + size as u64, Ordering::SeqCst);
        Ok(())
    }

    fn rebuild_secondary_indexes(&self, id: u64, data: &Value) {
        if let Some(obj) = data.as_object() {
            for (field, value) in obj {
                if self.indexed_fields.contains(field.as_str()) {
                    let value_str = value
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| value.to_string());
                    let field_idx = self.secondary_indices.entry(field.clone()).or_default();
                    field_idx.entry(value_str).or_default().push(id);
                }
            }
        }
    }
}
