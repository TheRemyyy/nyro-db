use serde_json::json;
use std::sync::Arc;

use crate::config::NyroConfig;
use crate::database::NyroDB;

#[tokio::test]
async fn batched_insert_is_committed_before_returning() -> anyhow::Result<()> {
    let mut config = test_config("batched_commit");
    config.performance.batch_size = 10;
    config.performance.batch_timeout = 1;
    cleanup_path(&config.storage.data_dir)?;

    let db = NyroDB::new(config.clone());
    let id = db
        .insert_raw(
            "user",
            json!({
                "id": 42,
                "email": "user42@test.com",
                "hash_password": "hash_42",
                "created_at": 1
            }),
        )
        .await?;

    assert_eq!(id, 42);
    let stored = db.get_raw("user", 42).await?;
    assert_eq!(
        stored,
        Some(json!({
            "id": 42,
            "email": "user42@test.com",
            "hash_password": "hash_42",
            "created_at": 1
        }))
    );

    db.shutdown().await?;
    cleanup_data_dir(&config.storage.data_dir)?;
    Ok(())
}

#[tokio::test]
async fn schema_validation_rejects_wrong_field_type() -> anyhow::Result<()> {
    let config = test_config("schema_type");
    cleanup_path(&config.storage.data_dir)?;
    let db = NyroDB::new(config.clone());

    let result = db
        .insert_raw(
            "product",
            json!({
                "id": 1,
                "name": "Keyboard",
                "price": "not-a-u32",
                "category_id": 9
            }),
        )
        .await;

    assert!(result.is_err());
    db.shutdown().await?;
    cleanup_data_dir(&config.storage.data_dir)?;
    Ok(())
}

#[tokio::test]
async fn storage_initialization_error_is_returned() -> anyhow::Result<()> {
    let config = test_config("storage_error");
    cleanup_path(&config.storage.data_dir)?;
    std::fs::create_dir_all(parent_temp_dir())?;
    std::fs::write(&config.storage.data_dir, "not a directory")?;

    let db = NyroDB::new(config.clone());
    let result = db
        .insert_raw(
            "user",
            json!({
                "id": 7,
                "email": "user7@test.com",
                "hash_password": "hash_7",
                "created_at": 1
            }),
        )
        .await;

    assert!(result.is_err());
    std::fs::remove_file(&config.storage.data_dir)?;
    Ok(())
}

#[tokio::test]
async fn concurrent_first_inserts_keep_one_readable_storage() -> anyhow::Result<()> {
    let mut config = test_config("concurrent_storage_init");
    config.performance.batch_size = 32;
    config.performance.batch_timeout = 1;
    cleanup_path(&config.storage.data_dir)?;

    let db = Arc::new(NyroDB::new(config.clone()));
    let mut tasks = Vec::new();
    for id in 0..512_u64 {
        let db_clone = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db_clone
                .insert_raw(
                    "user",
                    json!({
                        "id": id,
                        "email": format!("user{}@test.com", id),
                        "hash_password": format!("hash_{}", id),
                        "created_at": id
                    }),
                )
                .await
        }));
    }

    for task in tasks {
        task.await??;
    }

    for id in 0..512_u64 {
        assert!(db.get_raw("user", id).await?.is_some(), "missing id {id}");
    }

    db.shutdown().await?;
    cleanup_data_dir(&config.storage.data_dir)?;
    Ok(())
}

fn test_config(name: &str) -> NyroConfig {
    let mut config = NyroConfig::default();
    config.storage.data_dir = parent_temp_dir().join(name).to_string_lossy().into_owned();
    config.storage.sync_interval = 0;
    config.logging.level = "error".to_string();
    config.metrics.enable = false;
    config
}

fn parent_temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("nyrodb-tests-{}", std::process::id()))
}

fn cleanup_data_dir(path: &str) -> anyhow::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn cleanup_path(path: &str) -> anyhow::Result<()> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path).map_err(Into::into),
        Ok(_) => std::fs::remove_file(path).map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
