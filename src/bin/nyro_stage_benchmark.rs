use anyhow::Result;
use nyrodb::config::NyroConfig;
use nyrodb::storage::benchmark::run_storage_stage_benchmark;
use nyrodb::utils::benchmark::{benchmark_data_dir, cleanup_path};
use serde::Serialize;
use serde_json::{json, Value};

const OPERATIONS: u64 = 1_000_000;
const STORAGE_SYNC_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Serialize)]
struct StageBenchmarkReport {
    operations: u64,
    storage_sync_interval_ms: u64,
    row_shape: RowShapeReport,
    storage: nyrodb::storage::benchmark::StorageStageReport,
}

#[derive(Debug, Serialize)]
struct RowShapeReport {
    fields: usize,
    sample_json_bytes: usize,
}

fn main() -> Result<()> {
    let data_dir = benchmark_data_dir(10_000);
    cleanup_path(&data_dir)?;

    let mut config = NyroConfig::default();
    config.storage.data_dir = data_dir.clone();
    config.storage.sync_interval = STORAGE_SYNC_INTERVAL_MS;
    config.storage.enable_mmap = false;
    config.logging.level = "off".to_string();

    let rows = build_rows(OPERATIONS);
    let row_shape = row_shape_report(&rows)?;
    let schema = config
        .models
        .get("user")
        .ok_or_else(|| anyhow::anyhow!("Missing default user schema"))?;

    let storage = run_storage_stage_benchmark(rows, &config.storage, &config.logging, schema)?;
    let report = StageBenchmarkReport {
        operations: OPERATIONS,
        storage_sync_interval_ms: STORAGE_SYNC_INTERVAL_MS,
        row_shape,
        storage,
    };

    cleanup_path(&data_dir)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn build_rows(count: u64) -> Vec<Value> {
    (0..count)
        .map(|id| {
            json!({
                "id": id,
                "email": format!("stage-{}@nyro.local", id),
                "hash_password": format!("hash_{}", id),
                "created_at": id
            })
        })
        .collect()
}

fn row_shape_report(rows: &[Value]) -> Result<RowShapeReport> {
    let sample = rows
        .first()
        .ok_or_else(|| anyhow::anyhow!("Stage benchmark has no sample row"))?;

    Ok(RowShapeReport {
        fields: sample.as_object().map(|object| object.len()).unwrap_or(0),
        sample_json_bytes: serde_json::to_vec(sample)?.len(),
    })
}
