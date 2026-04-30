use dashmap::DashMap;
use parking_lot::RwLock;
use serde_json::Value;
use std::sync::Arc;

const DENSE_GROWTH_SLACK: u64 = 1_000_000;
const DENSE_RESIZE_CHUNK: usize = 8192;

#[derive(Clone, Copy)]
pub(crate) struct EntryLocation {
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

#[derive(Clone)]
pub(crate) struct CachedEntry {
    pub(crate) timestamp: u64,
    pub(crate) operation: u8,
    pub(crate) data: CachedData,
}

#[derive(Clone)]
pub(crate) enum CachedData {
    Json(Arc<[u8]>),
    Encoded(Arc<[u8]>),
    Parsed(Arc<Value>),
}

#[derive(Clone)]
pub(crate) struct IndexedEntry {
    pub(crate) location: EntryLocation,
    pub(crate) cache: CachedEntry,
}

pub(crate) struct PrimaryIndex {
    dense: RwLock<Vec<Option<IndexedEntry>>>,
    sparse: DashMap<u64, IndexedEntry>,
}

impl PrimaryIndex {
    pub(crate) fn new() -> Self {
        Self {
            dense: RwLock::new(Vec::new()),
            sparse: DashMap::new(),
        }
    }

    pub(crate) fn clear(&self) {
        self.dense.write().clear();
        self.sparse.clear();
    }

    pub(crate) fn insert(&self, id: u64, entry: IndexedEntry) {
        let mut dense = self.dense.write();
        let dense_len = dense.len() as u64;
        if id <= dense_len.saturating_add(DENSE_GROWTH_SLACK) {
            let Ok(index) = usize::try_from(id) else {
                drop(dense);
                self.sparse.insert(id, entry);
                return;
            };
            if index >= dense.len() {
                let next_len = (index + 1).saturating_add(DENSE_RESIZE_CHUNK);
                dense.resize_with(next_len, || None);
            }
            dense[index] = Some(entry);
            drop(dense);
            self.sparse.remove(&id);
            return;
        }

        drop(dense);
        self.sparse.insert(id, entry);
    }

    pub(crate) fn insert_many(&self, entries: Vec<(u64, IndexedEntry)>) {
        if entries.is_empty() {
            return;
        }

        let mut dense_entries = Vec::with_capacity(entries.len());
        let mut sparse_entries = Vec::new();
        let dense_len = self.dense.read().len() as u64;
        let dense_limit = dense_len.saturating_add(DENSE_GROWTH_SLACK);

        for (id, entry) in entries {
            match usize::try_from(id) {
                Ok(index) if id <= dense_limit => dense_entries.push((id, index, entry)),
                _ => sparse_entries.push((id, entry)),
            }
        }

        if !dense_entries.is_empty() {
            let max_index = dense_entries
                .iter()
                .map(|(_, index, _)| *index)
                .max()
                .unwrap_or(0);
            let mut dense = self.dense.write();
            if max_index >= dense.len() {
                dense.resize_with(max_index.saturating_add(DENSE_RESIZE_CHUNK + 1), || None);
            }
            for (id, index, entry) in dense_entries {
                dense[index] = Some(entry);
                self.sparse.remove(&id);
            }
        }

        for (id, entry) in sparse_entries {
            self.sparse.insert(id, entry);
        }
    }

    pub(crate) fn get(&self, id: u64) -> Option<IndexedEntry> {
        let Ok(index) = usize::try_from(id) else {
            return self.sparse.get(&id).map(|entry| entry.clone());
        };
        let dense = self.dense.read();
        if let Some(entry) = dense.get(index).and_then(Clone::clone) {
            return Some(entry);
        }
        drop(dense);
        self.sparse.get(&id).map(|entry| entry.clone())
    }

    pub(crate) fn ids(&self) -> Vec<u64> {
        let dense_ids = self
            .dense
            .read()
            .iter()
            .enumerate()
            .filter_map(|(id, entry)| entry.as_ref().map(|_| id as u64))
            .collect::<Vec<_>>();
        let mut ids = Vec::with_capacity(dense_ids.len() + self.sparse.len());
        ids.extend(dense_ids);
        ids.extend(self.sparse.iter().map(|entry| *entry.key()));
        ids
    }
}
