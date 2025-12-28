# Installation

NyroDB is distributed as a high-performance binary compiled from Rust source.

## Prerequisites

- **Rust Toolchain**: You need Rust 1.75 or later. [Install Rust](https://rustup.rs/).
- **Git**: To clone the repository.
- **OS**: Linux, macOS, or Windows (WSL2 recommended for max performance).

## Building from Source

1. **Clone the repository**:

    ```bash
    git clone https://github.com/TheRemyyy/nyro-db.git
    cd nyro-db
    ```

2. **Build in Release Mode**:
    > **Note**: Always build with `--release`. Debug builds include runtime overhead that significantly impacts the zero-copy optimizations.

    ```bash
    cargo build --release
    ```

    The binary will be located at `target/release/NyroDB` (or `NyroDB.exe` on Windows).

## Running the Server

1. **Ensure a configuration file exists**:
    NyroDB looks for `nyrodb.toml` in the current working directory.

    ```bash
    # Copy example config if needed
    cp nyrodb.toml.example nyrodb.toml 
    ```

2. **Run the binary**:

    ```bash
    ./target/release/NyroDB
    ```

    You should see an output indicating the server is listening:

    ```text
    [INFO] NyroDB v1.0.0 starting...
    [INFO] Config loaded from nyrodb.toml
    [INFO] Server listening on 127.0.0.1:8081
    ```

## CLI Arguments

Currently, NyroDB is configured primarily via `nyrodb.toml`. CLI flags will be added in future versions (v1.1+).
