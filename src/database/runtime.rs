use anyhow::Result;
use dashmap::mapref::entry::Entry;
use std::sync::Arc;

use crate::database::types::{ModelRuntime, NyroDB};
use crate::database::validation::SchemaPlan;
use crate::storage::LogStorage;

impl NyroDB {
    pub(crate) fn get_runtime(&self, model_name: &str) -> Result<Arc<ModelRuntime>> {
        if let Some(runtime) = self.runtimes.get(model_name) {
            return Ok(runtime.clone());
        }

        match self.runtimes.entry(model_name.to_string()) {
            Entry::Occupied(existing_runtime) => Ok(existing_runtime.get().clone()),
            Entry::Vacant(empty_slot) => {
                let schema = self
                    .config
                    .models
                    .get(model_name)
                    .ok_or_else(|| {
                        anyhow::anyhow!("Model '{}' not defined in configuration", model_name)
                    })?
                    .clone();
                let schema = Arc::new(schema);
                let schema_plan = Arc::new(SchemaPlan::from_schema(&schema)?);
                let storage = Arc::new(LogStorage::new(
                    model_name,
                    &self.config.storage,
                    &self.config.logging,
                    &schema,
                )?);
                let runtime = Arc::new(ModelRuntime {
                    schema_plan,
                    storage,
                });
                empty_slot.insert(runtime.clone());
                Ok(runtime)
            }
        }
    }

    pub fn get_storage(&self, model_name: &str) -> Result<Arc<LogStorage>> {
        Ok(self.get_runtime(model_name)?.storage.clone())
    }
}
