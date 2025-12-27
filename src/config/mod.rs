use crate::utils::logger::Logger;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NyroConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub performance: PerformanceConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
    pub security: SecurityConfig,
    pub models: HashMap<String, ModelSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub enable_auth: bool,
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub graceful_shutdown_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub buffer_size: usize,
    pub enable_mmap: bool,
    pub sync_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub batch_size: usize,
    pub batch_timeout: u64,
    pub max_concurrent_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub enable_colors: bool,
    pub log_requests: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enable: bool,
    pub report_interval: u64,
    pub max_samples: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSchema {
    pub fields: Vec<ModelField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub required: bool,
}

impl Default for NyroConfig {
    fn default() -> Self {
        let mut models = HashMap::new();

        models.insert(
            "user".to_string(),
            ModelSchema {
                fields: vec![
                    ModelField {
                        name: "id".to_string(),
                        field_type: "u64".to_string(),
                        required: true,
                    },
                    ModelField {
                        name: "email".to_string(),
                        field_type: "string".to_string(),
                        required: true,
                    },
                    ModelField {
                        name: "hash_password".to_string(),
                        field_type: "string".to_string(),
                        required: true,
                    },
                    ModelField {
                        name: "created_at".to_string(),
                        field_type: "u64".to_string(),
                        required: true,
                    },
                ],
            },
        );

        models.insert(
            "product".to_string(),
            ModelSchema {
                fields: vec![
                    ModelField {
                        name: "id".to_string(),
                        field_type: "u64".to_string(),
                        required: true,
                    },
                    ModelField {
                        name: "name".to_string(),
                        field_type: "string".to_string(),
                        required: true,
                    },
                    ModelField {
                        name: "price".to_string(),
                        field_type: "u32".to_string(),
                        required: true,
                    },
                    ModelField {
                        name: "category_id".to_string(),
                        field_type: "u64".to_string(),
                        required: true,
                    },
                ],
            },
        );

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
                api_key: "default_nyro_key_replace_me".to_string(),
            },
            models,
        }
    }
}

impl NyroConfig {
    pub fn load() -> Result<Self> {
        if let Ok(config) = Self::load_from_file("nyrodb.toml") {
            Logger::info_with_config(&config.logging, "Loaded configuration from nyrodb.toml");
            return Ok(config);
        }

        for path in &["config/nyrodb.toml", "/etc/nyrodb/nyrodb.toml"] {
            if let Ok(config) = Self::load_from_file(path) {
                Logger::info_with_config(
                    &config.logging,
                    &format!("Loaded configuration from {}", path),
                );
                return Ok(config);
            }
        }

        let config = Self::default();
        Logger::info_with_config(&config.logging, "Using default configuration");
        Ok(config)
    }

    fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn save_default_config() -> Result<()> {
        let config = Self::default();
        let toml_content = toml::to_string_pretty(&config)?;
        std::fs::write("nyrodb.toml", toml_content)?;
        Logger::info("Default configuration saved to nyrodb.toml");
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.server.port == 0 {
            return Err(anyhow::anyhow!("Server port cannot be 0"));
        }
        if self.server.host.is_empty() {
            return Err(anyhow::anyhow!("Server host cannot be empty"));
        }
        if self.storage.buffer_size == 0 {
            return Err(anyhow::anyhow!("Buffer size cannot be 0"));
        }
        if self.storage.data_dir.is_empty() {
            return Err(anyhow::anyhow!("Data directory cannot be empty"));
        }
        if self.performance.batch_size == 0 {
            return Err(anyhow::anyhow!("Batch size cannot be 0"));
        }
        if self.performance.max_concurrent_ops == 0 {
            return Err(anyhow::anyhow!("Max concurrent ops cannot be 0"));
        }
        match self.logging.level.as_str() {
            "info" | "warn" | "error" | "shutdown" => {}
            _ => return Err(anyhow::anyhow!("Invalid log level: {}", self.logging.level)),
        }
        if self.metrics.max_samples == 0 {
            return Err(anyhow::anyhow!("Max samples cannot be 0"));
        }
        if self.models.is_empty() {
            return Err(anyhow::anyhow!("No models defined in configuration"));
        }
        for (model_name, schema) in &self.models {
            if schema.fields.is_empty() {
                return Err(anyhow::anyhow!(
                    "Model '{}' has no fields defined",
                    model_name
                ));
            }
            if !schema.fields.iter().any(|f| f.name == "id") {
                return Err(anyhow::anyhow!(
                    "Model '{}' must have an 'id' field",
                    model_name
                ));
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_model_schema(&self, model_name: &str) -> Option<&ModelSchema> {
        self.models.get(model_name)
    }
}
