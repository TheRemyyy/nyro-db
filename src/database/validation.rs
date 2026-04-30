use anyhow::Result;
use serde_json::{Map, Value};

use crate::config::ModelSchema;

pub(crate) struct SchemaPlan {
    fields: Vec<FieldPlan>,
}

struct FieldPlan {
    name: String,
    required: bool,
    kind: FieldKind,
}

#[derive(Clone, Copy)]
enum FieldKind {
    String,
    Bool,
    U64,
    U32,
    I64,
    F64,
    Object,
    Array,
}

impl SchemaPlan {
    pub(crate) fn from_schema(schema: &ModelSchema) -> Result<Self> {
        let fields = schema
            .fields
            .iter()
            .map(|field| {
                Ok(FieldPlan {
                    name: field.name.clone(),
                    required: field.required,
                    kind: FieldKind::from_str(&field.field_type)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { fields })
    }

    pub(crate) fn validate_and_filter_owned(
        &self,
        mut obj: Map<String, Value>,
    ) -> Result<(u64, Map<String, Value>)> {
        let exact_field_count = obj.len() == self.fields.len();
        let mut all_schema_fields_present = true;
        let mut id = None;

        for field in &self.fields {
            match obj.get(&field.name) {
                Some(value) => {
                    field.kind.validate(&field.name, value)?;
                    if field.name == "id" {
                        id = value.as_u64();
                    }
                }
                None if field.required => {
                    return Err(anyhow::anyhow!("Missing required field: '{}'", field.name));
                }
                None => all_schema_fields_present = false,
            }
        }

        let id = id.ok_or_else(|| anyhow::anyhow!("Missing or invalid 'id' field"))?;
        if exact_field_count && all_schema_fields_present {
            return Ok((id, obj));
        }

        let mut filtered = Map::with_capacity(self.fields.len());
        for field in &self.fields {
            if let Some(value) = obj.remove(&field.name) {
                filtered.insert(field.name.clone(), value);
            }
        }
        Ok((id, filtered))
    }
}

impl FieldKind {
    fn from_str(field_type: &str) -> Result<Self> {
        match field_type {
            "string" => Ok(Self::String),
            "bool" => Ok(Self::Bool),
            "u64" => Ok(Self::U64),
            "u32" => Ok(Self::U32),
            "i64" => Ok(Self::I64),
            "f64" => Ok(Self::F64),
            "object" => Ok(Self::Object),
            "array" => Ok(Self::Array),
            unsupported => Err(anyhow::anyhow!("Unsupported field type: '{}'", unsupported)),
        }
    }

    fn validate(self, field_name: &str, value: &Value) -> Result<()> {
        let valid = match self {
            Self::String => value.is_string(),
            Self::Bool => value.is_boolean(),
            Self::U64 => value.as_u64().is_some(),
            Self::U32 => value
                .as_u64()
                .is_some_and(|number| u32::try_from(number).is_ok()),
            Self::I64 => value.as_i64().is_some(),
            Self::F64 => value.as_f64().is_some(),
            Self::Object => value.is_object(),
            Self::Array => value.is_array(),
        };

        if valid {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Invalid type for field '{}'", field_name))
        }
    }
}

pub(crate) fn validate_supported_field_type(field_type: &str) -> bool {
    matches!(
        field_type,
        "string" | "bool" | "u64" | "u32" | "i64" | "f64" | "object" | "array"
    )
}
