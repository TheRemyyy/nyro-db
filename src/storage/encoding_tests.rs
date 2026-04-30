use super::encoding::{
    decode_raw_entry, encode_compact_typed_raw_entry, encode_json_raw_entry,
    encode_typed_raw_entry, RawEntry,
};
use super::typed::{encode_compact_typed_payload, encode_typed_payload, field_codecs_from_schema};
use crate::config::{ModelField, ModelSchema};
use serde_json::{json, Value};

#[test]
fn decodes_json_raw_entry_format() -> anyhow::Result<()> {
    let encoded = encode_json_raw_entry(123, 0, br#"{"id":1}"#);
    let decoded = decode_raw_entry(&encoded, &[])?;

    assert_eq!(decoded.timestamp, 123);
    assert_eq!(decoded.operation, 0);
    assert_eq!(decoded.data, br#"{"id":1}"#);
    Ok(())
}

#[test]
fn decodes_typed_raw_entry_format() -> anyhow::Result<()> {
    let codecs = field_codecs_from_schema(&test_schema());
    let value = json!({
        "id": 7,
        "email": "a@nyro.local",
        "hash_password": "hash_7",
        "created_at": 99
    });
    let payload = encode_typed_payload(&value, &codecs)
        .ok_or_else(|| anyhow::anyhow!("expected typed payload"))?;
    let encoded = encode_typed_raw_entry(456, 0, &payload);
    let decoded = decode_raw_entry(&encoded, &codecs)?;
    let decoded_value: Value = serde_json::from_slice(&decoded.data)?;

    assert_eq!(decoded.timestamp, 456);
    assert_eq!(decoded.operation, 0);
    assert_eq!(decoded_value, value);
    Ok(())
}

#[test]
fn decodes_compact_typed_raw_entry_format() -> anyhow::Result<()> {
    let codecs = field_codecs_from_schema(&test_schema());
    let value = json!({
        "id": 8,
        "email": "compact@nyro.local",
        "hash_password": "hash_8",
        "created_at": 100
    });
    let payload = encode_compact_typed_payload(&value, &codecs)
        .ok_or_else(|| anyhow::anyhow!("expected compact typed payload"))?;
    let encoded = encode_compact_typed_raw_entry(457, 0, &payload);
    let decoded = decode_raw_entry(&encoded, &codecs)?;
    let decoded_value: Value = serde_json::from_slice(&decoded.data)?;

    assert_eq!(decoded.timestamp, 457);
    assert_eq!(decoded.operation, 0);
    assert_eq!(decoded_value, value);
    Ok(())
}

#[test]
fn decodes_legacy_bincode_raw_entry_format() -> anyhow::Result<()> {
    let encoded = bincode::serialize(&RawEntry {
        timestamp: 789,
        operation: 1,
        data: br#"{"id":2}"#.to_vec(),
    })?;
    let decoded = decode_raw_entry(&encoded, &[])?;

    assert_eq!(decoded.timestamp, 789);
    assert_eq!(decoded.operation, 1);
    assert_eq!(decoded.data, br#"{"id":2}"#);
    Ok(())
}

fn test_schema() -> ModelSchema {
    ModelSchema {
        fields: vec![
            field("id", "u64"),
            field("email", "string"),
            field("hash_password", "string"),
            field("created_at", "u64"),
        ],
    }
}

fn field(name: &str, field_type: &str) -> ModelField {
    ModelField {
        name: name.to_string(),
        field_type: field_type.to_string(),
        required: true,
        indexed: false,
    }
}
