# Architecture Internals

NyroDB achieves its performance through append-only logging, memory-backed primary indexes, and explicit bulk ingestion.

## 1. Storage Engine (`LogStorage`)

The core storage unit is an append-only log file with an in-memory read cache.

### Compact Log Encoding

- **Writes**: New records use a compact fixed header followed by JSON payload bytes.
- **Reads**: Hot reads use the primary index cache, avoiding disk IO for recently loaded or inserted rows.
- **Compatibility**: Legacy bincode log entries are still decoded during reads and index rebuilds.

## 2. Indexing Strategy

### Primary Index

A dense vector-backed index handles normal sequential `u64` IDs, with a sparse `DashMap` fallback for very large or non-dense IDs.

- **Lookup**: O(1).
- **Concurrency**: Dense IDs use short `parking_lot` lock sections; sparse IDs use `DashMap`.

### Secondary Indexing

NyroDB maintains in-memory `DashMap<field_value, Vec<id>>` for fields indexed in `nyrodb.toml`.

- **Query**: Instant lookup returning a list of primary IDs.
- **Updates**: Asynchronous maintenance during insert.

## 3. Ingestion Paths

NyroDB exposes separate paths for single-row latency and high-throughput ingestion.

- **Single insert**: Writes directly to the storage writer and returns after the row is indexed.
- **insert_many**: Prepares rows in parallel and appends the batch in one writer pass.
- **Durability**: `sync_interval = 0` calls `sync_data` on each append; higher values use buffered throughput and periodic sync.

## 4. Concurrency Model

- **Tokio Runtime**: Powered by Rust's async/await.
- **Direct Writer Path**: Single inserts avoid per-row channel and oneshot acknowledgement overhead.
- **Semaphore**: A global semaphore limits `max_concurrent_ops` (default 100k) to prevent OOM (Out Of Memory) under extreme load.
