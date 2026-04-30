use std::collections::HashMap;

use crate::config::{
    LoggingConfig, MetricsConfig, ModelField, ModelSchema, NyroConfig, PerformanceConfig,
    SecurityConfig, ServerConfig, StorageConfig,
};

impl Default for NyroConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                graceful_shutdown_timeout: 5,
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                buffer_size: 8 * 1024 * 1024,
                enable_mmap: true,
                sync_interval: 1000,
            },
            performance: PerformanceConfig {
                batch_size: 1000,
                batch_timeout: 100,
                max_concurrent_ops: 10000,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                enable_colors: true,
                log_requests: false,
            },
            metrics: MetricsConfig {
                enable: true,
                report_interval: 30,
                max_samples: 10000,
            },
            security: SecurityConfig {
                enable_auth: false,
                api_key: String::new(),
            },
            models: default_models(),
        }
    }
}

fn default_models() -> HashMap<String, ModelSchema> {
    let mut models = HashMap::new();
    models.insert(
        "user".to_string(),
        ModelSchema {
            fields: vec![
                field("id", "u64", true),
                field("email", "string", true),
                field("hash_password", "string", true),
                field("created_at", "u64", true),
            ],
        },
    );
    models.insert(
        "product".to_string(),
        ModelSchema {
            fields: vec![
                field("id", "u64", true),
                field("name", "string", true),
                field("price", "u32", true),
                field("category_id", "u64", true),
            ],
        },
    );
    models
}

fn field(name: &str, field_type: &str, required: bool) -> ModelField {
    ModelField {
        name: name.to_string(),
        field_type: field_type.to_string(),
        required,
        indexed: false,
    }
}
