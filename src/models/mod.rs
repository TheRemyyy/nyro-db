use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub timestamp: u64,
    pub operation: Operation,
    pub data: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}
