use anyhow::Result;
use rayon::prelude::*;
use serde_json::Value;
use std::io::Write;

use crate::models::LogEntry;

use super::encoding::{self, CacheMode, EncodedEntry};
use super::index::{EntryLocation, IndexedEntry};
use super::LogStorage;

const PARALLEL_ENCODE_THRESHOLD: usize = 16_384;

impl LogStorage {
    pub fn append(&self, entry: &LogEntry<Value>) -> Result<()> {
        let encoded_entry = encoding::encode_entry(
            entry,
            &self.indexed_fields,
            &self.field_codecs,
            CacheMode::ParsedValue,
        )?;
        self.append_encoded_entry(encoded_entry)
    }

    pub fn append_owned(&self, entry: LogEntry<Value>) -> Result<()> {
        let encoded_entry =
            encoding::encode_owned_entry(entry, &self.indexed_fields, &self.field_codecs)?;
        self.append_encoded_entry(encoded_entry)
    }

    pub fn append_entries(&self, entries: &[LogEntry<Value>]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let encoded_entries = self.encode_entry_slice(entries)?;
        self.append_encoded_entries(encoded_entries)
    }

    pub fn append_entries_owned(&self, entries: Vec<LogEntry<Value>>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let encoded_entries = self.encode_entry_slice(&entries)?;
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
        let mut file = self.file.write();
        let mut offset = self
            .current_offset
            .load(std::sync::atomic::Ordering::Acquire);
        let mut primary_entries = Vec::with_capacity(encoded_entries.len());
        let mut secondary_entries = Vec::new();

        for encoded_entry in encoded_entries {
            let entry_size = encoded_entry.size;
            file.write_all(&encoded_entry.size.to_le_bytes())?;
            file.write_all(&encoded_entry.data)?;
            Self::prepare_index_publish(
                offset,
                encoded_entry,
                &mut primary_entries,
                &mut secondary_entries,
            );
            offset += 4 + entry_size as u64;
        }
        if self.sync_on_append {
            file.flush()?;
            file.get_ref().sync_data()?;
        }

        self.current_offset
            .store(offset, std::sync::atomic::Ordering::SeqCst);
        self.publish_prepared_indexes(primary_entries, secondary_entries);

        Ok(())
    }

    fn append_encoded_entry(&self, encoded_entry: EncodedEntry) -> Result<()> {
        let mut file = self.file.write();
        let offset = self
            .current_offset
            .load(std::sync::atomic::Ordering::Acquire);
        let entry_size = encoded_entry.size;

        file.write_all(&entry_size.to_le_bytes())?;
        file.write_all(&encoded_entry.data)?;
        if self.sync_on_append {
            file.flush()?;
            file.get_ref().sync_data()?;
        }
        self.current_offset.store(
            offset + 4 + entry_size as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
        drop(file);

        self.insert_indexes(offset, &encoded_entry);
        Ok(())
    }

    fn encode_entry_slice(&self, entries: &[LogEntry<Value>]) -> Result<Vec<EncodedEntry>> {
        if entries.len() >= PARALLEL_ENCODE_THRESHOLD {
            return entries
                .par_iter()
                .map(|entry| {
                    encoding::encode_entry(
                        entry,
                        &self.indexed_fields,
                        &self.field_codecs,
                        CacheMode::EncodedFrame,
                    )
                })
                .collect::<Result<Vec<_>>>();
        }

        entries
            .iter()
            .map(|entry| {
                encoding::encode_entry(
                    entry,
                    &self.indexed_fields,
                    &self.field_codecs,
                    CacheMode::EncodedFrame,
                )
            })
            .collect::<Result<Vec<_>>>()
    }

    fn encode_entry_refs(&self, entries: &[&LogEntry<Value>]) -> Result<Vec<EncodedEntry>> {
        if entries.len() >= PARALLEL_ENCODE_THRESHOLD {
            return entries
                .par_iter()
                .map(|entry| {
                    encoding::encode_entry(
                        entry,
                        &self.indexed_fields,
                        &self.field_codecs,
                        CacheMode::EncodedFrame,
                    )
                })
                .collect::<Result<Vec<_>>>();
        }

        entries
            .iter()
            .map(|entry| {
                encoding::encode_entry(
                    entry,
                    &self.indexed_fields,
                    &self.field_codecs,
                    CacheMode::EncodedFrame,
                )
            })
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

    fn prepare_index_publish(
        offset: u64,
        encoded_entry: EncodedEntry,
        primary_entries: &mut Vec<(u64, IndexedEntry)>,
        secondary_entries: &mut Vec<(String, String, u64)>,
    ) {
        let Some(index_data) = encoded_entry.index_data else {
            return;
        };
        primary_entries.push((
            index_data.id,
            IndexedEntry {
                location: EntryLocation {
                    offset,
                    size: encoded_entry.size,
                },
                cache: encoded_entry.cache_entry,
            },
        ));
        for (field, value) in index_data.fields {
            secondary_entries.push((field, value, index_data.id));
        }
    }

    fn publish_prepared_indexes(
        &self,
        primary_entries: Vec<(u64, IndexedEntry)>,
        secondary_entries: Vec<(String, String, u64)>,
    ) {
        self.index.insert_many(primary_entries);
        for (field, value, id) in secondary_entries {
            let field_idx = self.secondary_indices.entry(field).or_default();
            field_idx.entry(value).or_default().push(id);
        }
    }
}
