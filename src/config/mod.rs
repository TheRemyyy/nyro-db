mod defaults;

use crate::utils::logger::Logger;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

const API_KEY_ENV_VAR: &str = "NYRODB_API_KEY";
const DEFAULT_API_KEY_PLACEHOLDER: &str = "replace_me";

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
    #[serde(default)]
    pub indexed: bool,
}

impl NyroConfig {
    pub fn load() -> Result<Self> {
        if let Ok(config) = Self::load_from_file("nyrodb.toml") {
            Logger::info_with_config(&config.logging, "Loaded configuration from nyrodb.toml");
            return Ok(config.with_env_overrides());
        }

        for path in &["config/nyrodb.toml", "/etc/nyrodb/nyrodb.toml"] {
            if let Ok(config) = Self::load_from_file(path) {
                Logger::info_with_config(
                    &config.logging,
                    &format!("Loaded configuration from {}", path),
                );
                return Ok(config.with_env_overrides());
            }
        }

        let config = Self::default();
        Logger::info_with_config(&config.logging, "Using default configuration");
        Ok(config.with_env_overrides())
    }

    fn load_from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    fn with_env_overrides(mut self) -> Self {
        if let Ok(api_key) = std::env::var(API_KEY_ENV_VAR) {
            self.security.api_key = api_key;
        }
        self
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
        let bind_addr: SocketAddr = format!("{}:{}", self.server.host, self.server.port)
            .parse()
            .map_err(|error| anyhow::anyhow!("Invalid server bind address: {}", error))?;
        if !self.security.enable_auth && !bind_addr.ip().is_loopback() {
            return Err(anyhow::anyhow!(
                "Authentication must be enabled when binding to a non-loopback address"
            ));
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
        if self.security.enable_auth {
            if self.security.api_key.trim().is_empty() {
                return Err(anyhow::anyhow!(
                    "API authentication is enabled but no API key is configured"
                ));
            }
            if self.security.api_key == DEFAULT_API_KEY_PLACEHOLDER {
                return Err(anyhow::anyhow!(
                    "Default API key placeholder must be replaced before enabling auth"
                ));
            }
        }
        match self.logging.level.as_str() {
            "info" | "warn" | "error" | "shutdown" | "off" => {}
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
            let mut field_names = HashSet::new();
            if !schema.fields.iter().any(|field| field.name == "id") {
                return Err(anyhow::anyhow!(
                    "Model '{}' must have an 'id' field",
                    model_name
                ));
            }
            for field in &schema.fields {
                if !field_names.insert(&field.name) {
                    return Err(anyhow::anyhow!(
                        "Model '{}' has duplicate field '{}'",
                        model_name,
                        field.name
                    ));
                }
                if !crate::database::validation::validate_supported_field_type(&field.field_type) {
                    return Err(anyhow::anyhow!(
                        "Model '{}' field '{}' has unsupported type '{}'",
                        model_name,
                        field.name,
                        field.field_type
                    ));
                }
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_model_schema(&self, model_name: &str) -> Option<&ModelSchema> {
        self.models.get(model_name)
    }
}
