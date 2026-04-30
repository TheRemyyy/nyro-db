use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;

const DENSE_GROWTH_SLACK: u64 = 1_000_000;

#[derive(Clone, Copy)]
pub(crate) struct EntryLocation {
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

#[derive(Clone)]
pub(crate) struct CachedEntry {
    pub(crate) timestamp: u64,
    pub(crate) operation: u8,
    pub(crate) data: Arc<[u8]>,
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
        let dense_len = self.dense.read().len() as u64;
        if id <= dense_len.saturating_add(DENSE_GROWTH_SLACK) {
            let Ok(index) = usize::try_from(id) else {
                self.sparse.insert(id, entry);
                return;
            };
            let mut dense = self.dense.write();
            if index >= dense.len() {
                dense.resize_with(index + 1, || None);
            }
            dense[index] = Some(entry);
            self.sparse.remove(&id);
            return;
        }

        self.sparse.insert(id, entry);
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
