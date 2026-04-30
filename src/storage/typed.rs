use anyhow::Result;
use serde_json::{Map, Number, Value};

use crate::config::ModelSchema;

#[derive(Clone)]
pub(crate) struct FieldCodec {
    pub(crate) name: String,
    field_type: FieldType,
}

#[derive(Clone, Copy)]
enum FieldType {
    String,
    Bool,
    U64,
    U32,
    I64,
    F64,
    Json,
}

pub(crate) fn field_codecs_from_schema(schema: &ModelSchema) -> Vec<FieldCodec> {
    schema
        .fields
        .iter()
        .map(|field| FieldCodec {
            name: field.name.clone(),
            field_type: match field.field_type.as_str() {
                "string" => FieldType::String,
                "bool" => FieldType::Bool,
                "u64" => FieldType::U64,
                "u32" => FieldType::U32,
                "i64" => FieldType::I64,
                "f64" => FieldType::F64,
                "object" | "array" => FieldType::Json,
                _ => FieldType::Json,
            },
        })
        .collect()
}

pub(crate) fn encode_typed_payload(data: &Value, field_codecs: &[FieldCodec]) -> Option<Vec<u8>> {
    let object = data.as_object()?;
    if object.len() != field_codecs.len() {
        return None;
    }

    let mut payload = Vec::with_capacity(object.len() * 8);
    for field in field_codecs {
        let value = object.get(&field.name)?;
        match field.field_type {
            FieldType::String => {
                let bytes = value.as_str()?.as_bytes();
                let len = u32::try_from(bytes.len()).ok()?;
                payload.extend_from_slice(&len.to_le_bytes());
                payload.extend_from_slice(bytes);
            }
            FieldType::Bool => payload.push(u8::from(value.as_bool()?)),
            FieldType::U64 => payload.extend_from_slice(&value.as_u64()?.to_le_bytes()),
            FieldType::U32 => {
                let number = u32::try_from(value.as_u64()?).ok()?;
                payload.extend_from_slice(&number.to_le_bytes());
            }
            FieldType::I64 => payload.extend_from_slice(&value.as_i64()?.to_le_bytes()),
            FieldType::F64 => payload.extend_from_slice(&value.as_f64()?.to_le_bytes()),
            FieldType::Json => return None,
        }
    }

    Some(payload)
}

pub(crate) fn decode_typed_payload(data: &[u8], field_codecs: &[FieldCodec]) -> Result<Vec<u8>> {
    let mut cursor = 0;
    let mut object = Map::with_capacity(field_codecs.len());

    for field in field_codecs {
        let value = match field.field_type {
            FieldType::String => {
                let len = read_u32(data, &mut cursor)? as usize;
                let bytes = read_bytes(data, &mut cursor, len)?;
                Value::String(std::str::from_utf8(bytes)?.to_string())
            }
            FieldType::Bool => Value::Bool(read_u8(data, &mut cursor)? != 0),
            FieldType::U64 => Value::Number(Number::from(read_u64(data, &mut cursor)?)),
            FieldType::U32 => Value::Number(Number::from(read_u32(data, &mut cursor)?)),
            FieldType::I64 => Value::Number(Number::from(read_i64(data, &mut cursor)?)),
            FieldType::F64 => Number::from_f64(read_f64(data, &mut cursor)?)
                .map(Value::Number)
                .ok_or_else(|| anyhow::anyhow!("Invalid f64 value in typed log entry"))?,
            FieldType::Json => return Err(anyhow::anyhow!("JSON field cannot use typed codec")),
        };
        object.insert(field.name.clone(), value);
    }

    if cursor != data.len() {
        return Err(anyhow::anyhow!("Typed log entry has trailing bytes"));
    }
    serde_json::to_vec(&Value::Object(object)).map_err(Into::into)
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8> {
    let bytes = read_bytes(data, cursor, 1)?;
    Ok(bytes[0])
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(data, cursor)?))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_array(data, cursor)?))
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_array(data, cursor)?))
}

fn read_f64(data: &[u8], cursor: &mut usize) -> Result<f64> {
    Ok(f64::from_le_bytes(read_array(data, cursor)?))
}

fn read_array<const SIZE: usize>(data: &[u8], cursor: &mut usize) -> Result<[u8; SIZE]> {
    let bytes = read_bytes(data, cursor, SIZE)?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("Corrupt typed log entry"))
}

fn read_bytes<'a>(data: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor.saturating_add(len);
    if end > data.len() {
        return Err(anyhow::anyhow!("Corrupt typed log entry"));
    }
    let bytes = &data[*cursor..end];
    *cursor = end;
    Ok(bytes)
}
