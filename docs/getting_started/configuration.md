# Configuration

NyroDB is configured via a `nyrodb.toml` file located in the working directory (or `/etc/nyrodb/`).

## Example Configuration

```toml
# Server settings
[server]
host = "127.0.0.1"
port = 8081
graceful_shutdown_timeout = 5

# Storage engine settings
[storage]
data_dir = "./data"      # Directory for persistence
buffer_size = 1048576    # 1MB buffer
enable_mmap = true       # Use memory-mapped files (Critical for performance)
sync_interval = 1000     # Sync to disk every 1000ms

# Performance tuning
[performance]
batch_size = 10000        # Max items per batch write
batch_timeout = 100       # Max ms to wait for batch fill
max_concurrent_ops = 50000

# Logging configuration
[logging]
level = "info" # "info", "warn", "error"
enable_colors = true
log_requests = true

# Metrics export
[metrics]
enable = true
report_interval = 30
max_samples = 10000

# Security settings
[security]
enable_auth = true
api_key = "change_me_in_production"

# Data Models Schema
[models.user]
fields = [
  { name = "id", type = "u64", required = true },
  { name = "email", type = "string", required = true },
  { name = "created_at", type = "u64", required = true }
]

[models.product]
fields = [
  { name = "id", type = "u64", required = true },
  { name = "name", type = "string", required = true },
  { name = "price", type = "u32", required = true }
]
```

## Section Reference

### `[server]`

- **host**: Interface to bind to (e.g., "0.0.0.0" for public access).
- **port**: TCP port (Default: 8081).
- **graceful_shutdown_timeout**: Seconds to wait for active connections on shutdown.

### `[storage]`

- **enable_mmap**: If `true`, uses OS-level memory mapping. This allows the DB to access data at RAM speeds without copying it into user-space buffers (Zero-Copy).
- **sync_interval**: How often (in ms) to force flush data to disk. Lower = safer, Higher = faster.

### `[performance]`

- **batch_size**: Number of `INSERT` operations to group into a single atomic write.
- **batch_timeout**: If the batch isn't full, write anyway after this many milliseconds.

### `[security]`

- **enable_auth**: Enforces check for `x-api-key` header on REST endpoints.
