use anyhow::Result;
use serde_json::{Map, Value};

use crate::config::{ModelField, ModelSchema, NyroConfig};

pub(crate) fn validate_data(
    config: &NyroConfig,
    model_name: &str,
    obj: &Map<String, Value>,
) -> Result<()> {
    let schema = config
        .models
        .get(model_name)
        .ok_or_else(|| anyhow::anyhow!("Model '{}' not defined in configuration", model_name))?;

    for field in &schema.fields {
        match obj.get(&field.name) {
            Some(value) => validate_field_value(field, value)?,
            None if field.required => {
                return Err(anyhow::anyhow!("Missing required field: '{}'", field.name));
            }
            None => {}
        }
    }

    Ok(())
}

pub(crate) fn filter_data_owned(
    schema: &ModelSchema,
    mut obj: Map<String, Value>,
) -> Map<String, Value> {
    let mut filtered = Map::with_capacity(schema.fields.len());
    for field in &schema.fields {
        if let Some(value) = obj.remove(&field.name) {
            filtered.insert(field.name.clone(), value);
        }
    }
    filtered
}

pub(crate) fn validate_supported_field_type(field_type: &str) -> bool {
    matches!(
        field_type,
        "string" | "bool" | "u64" | "u32" | "i64" | "f64" | "object" | "array"
    )
}

fn validate_field_value(field: &ModelField, value: &Value) -> Result<()> {
    let valid = match field.field_type.as_str() {
        "string" => value.is_string(),
        "bool" => value.is_boolean(),
        "u64" => value.as_u64().is_some(),
        "u32" => value
            .as_u64()
            .is_some_and(|number| u32::try_from(number).is_ok()),
        "i64" => value.as_i64().is_some(),
        "f64" => value.as_f64().is_some(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        unsupported => return Err(anyhow::anyhow!("Unsupported field type: '{}'", unsupported)),
    };

    if valid {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Invalid type for field '{}': expected {}",
            field.name,
            field.field_type
        ))
    }
}
