# Introduction to NyroDB

**NyroDB** is a hyper-performant, real-time database engine built from the ground up in Rust. Designed for extreme throughput and universality, it bridges the gap between in-memory caches and persistent storage solutions.

## Core Philosophy

NyroDB was built to solve specific bottlenecks in modern systems programming:

1. **Serialization Overhead**: Traditional DBs waste cycles wrapping row payloads. NyroDB writes a compact fixed header followed by JSON payload bytes.
2. **Latency**: Single inserts use a direct writer path, while `insert_many` provides explicit high-throughput ingestion.
3. **Real-Time Needs**: Most apps need external message queues (Redis/Kafka) for updates. NyroDB has a native **WebSocket** layer for instant pub/sub.

## Key Features

- **Extreme Throughput**: Capable of **1,000,000+ operations per second** on standard hardware.
- **Universal Indexing**: O(1) secondary indexing on *any* JSON field. Query by metadata instantly without schema migrations.
- **Real-Time Native**: Built-in WebSocket server pushes `INSERT` and updates to connected clients immediately.
- **Production Ready**:
  - **Security**: Native API Key authentication.
  - **Persistence**: Append-only disk persistence with configurable `sync_data` durability.
  - **Recovery**: Automatic crash recovery from WAL (Write-Ahead Log) or memory maps.
- **Fast Append Storage**: Data is appended to compact log files and served from memory-backed indexes.
- **Observability**: Built-in `/metrics` endpoint for monitoring throughput and latency in real-time.

## Use Cases

- **High-Frequency Trading**: Where every microsecond of latency matters.
- **Real-Time Gaming**: Managing state for thousands of concurrent players.
- **Messaging Platforms**: Storing and broadcasting chat messages instantly.
- **Secure Transaction Logging**: Immutable, append-only logs for financial or audit data.
- **Access-Controlled Configuration**: Storing sensitive application secrets with strict API key validation.
- **Audit Trails**: High-throughput ingestion of system events with guaranteed persistence.
