<div align="center">

# NyroDB

**The Fastest Real-Time Database in the Known Universe**

[![Rust Version](https://img.shields.io/badge/Rust-1.75+-brown?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)
[![Ops/Sec](https://img.shields.io/badge/Performance-1M%20ops%2Fsec-blueviolet?style=flat-square)](https://github.com/TheRemyyy/nyro-db)

*A hyper-performant, zero-copy, real-time database engine designed for extreme throughput and universal versatility.*

[Features](#features) • [Installation](#installation) • [API Guide](#api-guide) • [Configuration](#configuration)

</div>

---

## Overview

NyroDB is a next-generation database engine built from the ground up in Rust. It utilizes **Zero-Copy Serialization (Bincode)**, **Asynchronous Batching**, and **Secondary Indexing** to achieve performance that transcends modern understanding. Whether you're building a real-time messaging app, a high-frequency trading platform, or a secure authentication system, NyroDB provides the speed and flexibility you need.

### Key Features

- **⚡ Extreme Throughput** — Capable of exceeding **1,000,000 operations per second** with sub-microsecond internal latency.
- **🧠 Universal Querying** — O(1) secondary indexing on any field. Query by email, status, or custom metadata instantly.
- **🌐 Real-Time Native** — Built-in WebSocket server for instant data streaming and pub/sub notifications.
- **🛡️ Secure by Design** — Native API Key authentication and schema validation for production-grade safety.
- **🚀 Zero-Copy Storage** — Optimized disk persistence using memory-mapped files and ultra-fast binary serialization.
- **📊 Real-Time Metrics** — Detailed performance monitoring including throughput windows and p99 latency stats.

## <a id="installation"></a>📦 Installation

### From Source

```bash
git clone https://github.com/TheRemyyy/nyro-db.git
cd nyro-db
cargo build --release
```

### Running

```bash
./target/release/NyroDB
```

## <a id="api-guide"></a>🔧 API Guide

NyroDB exposes a simple yet powerful REST and WebSocket API.

### REST Endpoints (Port 8081)

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/insert/:model` | Insert a new JSON record. |
| `GET` | `/get/:model/:id` | Retrieve a record by its primary ID. |
| `GET` | `/query/:model` | List all records in a model. |
| `GET` | `/query/:model/:field/:value` | **O(1)** search by secondary index. |
| `GET` | `/metrics` | Retrieve real-time performance statistics. |

### Real-Time WebSocket

Connect to `ws://127.0.0.1:8081/ws` to receive instant updates.
Format: `INSERT:model_name:{"id":123,...}`

## <a id="configuration"></a>⚙️ Configuration

NyroDB is configured via `nyrodb.toml`.

```toml
[server]
host = "127.0.0.1"
port = 8081

[security]
enable_auth = true
api_key = "your_secret_key"

[performance]
batch_size = 10000
max_concurrent_ops = 100000
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">
<sub>Built with ❤️ and Rust</sub>
</div>
