use anyhow::Result;
use rayon::prelude::*;
use serde_json::Value;
use std::io::Write;

use crate::models::LogEntry;

use super::encoding::{self, EncodedEntry};
use super::index::{EntryLocation, IndexedEntry};
use super::LogStorage;

const PARALLEL_ENCODE_THRESHOLD: usize = 1024;

impl LogStorage {
    pub fn append(&self, entry: &LogEntry<Value>) -> Result<()> {
        self.append_many(&[entry])
    }

    pub fn append_entries(&self, entries: &[LogEntry<Value>]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let encoded_entries = self.encode_entry_slice(entries)?;
        self.append_encoded_entries(encoded_entries)
    }

    pub fn append_many(&self, entries: &[&LogEntry<Value>]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let encoded_entries = self.encode_entry_refs(entries)?;
        self.append_encoded_entries(encoded_entries)
    }

    fn append_encoded_entries(&self, encoded_entries: Vec<EncodedEntry>) -> Result<()> {
        let mut file = self
            .file
            .write()
            .map_err(|_| anyhow::anyhow!("Storage writer lock poisoned"))?;
        let mut offset = self
            .current_offset
            .load(std::sync::atomic::Ordering::Acquire);
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
        self.current_offset
            .store(offset, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    fn encode_entry_slice(&self, entries: &[LogEntry<Value>]) -> Result<Vec<EncodedEntry>> {
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

    fn encode_entry_refs(&self, entries: &[&LogEntry<Value>]) -> Result<Vec<EncodedEntry>> {
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
}
