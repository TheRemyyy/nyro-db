# Architecture Internals

NyroDB achieves its performance through a combination of zero-copy memory mapping, append-only logging, and lock-free concurrency patterns.

## 1. Storage Engine (`LogStorage`)

The core storage unit is an append-only log file backed by `mmap`.

### Zero-Copy Serialization

Unlike traditional databases that copy data from kernel space -> user buffer -> parsing struct -> application object, NyroDB maps the disk file directly into the process's virtual memory address space.

- **Bincode**: Data is serialized using `bincode`, a compact binary format that rivals raw C structs in speed.
- **Reads**: Reading a record involves computing its offset and casting a raw pointer to a byte slice, which is then deserialized. This avoids multiple copies.

## 2. Indexing Strategy

### Primary Index

A `DashMap` (concurrent hash map) maps `id` (u64) -> `file_offset` (u64).

- **Lookup**: O(1).
- **Concurrency**: Lock-free reads, striped locking for writes.

### Secondary Indexing

NyroDB maintains in-memory `DashMap<field_value, Vec<id>>` for fields indexed in `nyrodb.toml`.

- **Query**: Instant lookup returning a list of primary IDs.
- **Updates**: Asynchronous maintenance during insert.

## 3. Asynchronous Batching

To overcome the latency of individual syscalls (fsync), writes are grouped.

1. `INSERT` requests are pushed to a `mmap::UnboundedChannel`.
2. A background `tokio` task drains the channel into a buffer.
3. Once `batch_size` (default 10k) is reached OR `batch_timeout` (100ms) expires:
   - The batch is serialized into a single binary blob.
   - A single `write` syscall appends to the file.
   - `fsync` can be deferred based on `sync_interval`.

## 4. Concurrency Model

- **Tokio Runtime**: Powered by Rust's async/await.
- **Actor-like Design**: The Database struct acts as a supervisor, spawning tasks for batching, metrics reporting, and WebSocket broadcasting.
- **Semaphore**: A global semaphore limits `max_concurrent_ops` (default 100k) to prevent OOM (Out Of Memory) under extreme load.
