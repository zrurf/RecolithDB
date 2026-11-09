use recolith_common::FieldId;
use recolith_common::bytes::put_uvarint;
use recolith_schema::{Field, RecordSchema, Type};

use super::wire::field_wire;
use crate::error::{Result, StorageError};
use crate::value::{RecordData, Value};

/// Encodes known fields and preserves unknown raw entries.
///
/// # Errors
///
/// Returns [`StorageError`] for missing required fields, unknown fields, or type mismatches.
pub fn encode_fields(schema: &RecordSchema, data: &RecordData) -> Result<Vec<u8>> {
    for field in schema.fields() {
        if !field.optional() && data.get(field.id()).is_none() {
            return Err(StorageError::MissingField(field.id()));
        }
    }

    let mut output = Vec::new();

    for (field_id, value) in data.values() {
        let field = schema.field(*field_id).ok_or_else(|| {
            StorageError::InvalidValue(format!("field {field_id} is not in the schema"))
        })?;
        output.extend(encode_field(field, value)?);
    }

    for (field_id, entry) in data.unknown() {
        if schema.field(*field_id).is_some() {
            return Err(StorageError::InvalidValue(format!(
                "unknown entry for known field {field_id}"
            )));
        }
        output.extend_from_slice(entry);
    }

    Ok(output)
}

fn encode_field(field: &Field, value: &Value) -> Result<Vec<u8>> {
    let encoded_value = encode_value(field.id(), field.ty(), value)?;
    let mut output = Vec::with_capacity(encoded_value.len() + 8);

    put_uvarint(u64::from(field.id().get()), &mut output);
    output.push(field_wire(field.ty()));
    put_uvarint(encoded_value.len() as u64, &mut output);
    output.extend(encoded_value);

    Ok(output)
}

fn encode_value(field: FieldId, ty: &Type, value: &Value) -> Result<Vec<u8>> {
    match (ty, value) {
        (Type::Bool, Value::Bool(value)) => Ok(varint(if *value { 1 } else { 0 })),
        (Type::I32, Value::I32(value)) => Ok(value.to_le_bytes().to_vec()),
        (Type::I64, Value::I64(value)) => Ok(value.to_le_bytes().to_vec()),
        (Type::U32, Value::U32(value)) => Ok(varint(u64::from(*value))),
        (Type::U64, Value::U64(value)) => Ok(varint(*value)),
        (Type::F32, Value::F32(value)) => Ok(value.to_bits().to_le_bytes().to_vec()),
        (Type::F64, Value::F64(value)) => Ok(value.to_bits().to_le_bytes().to_vec()),
        (Type::String, Value::String(value)) => Ok(length_prefixed(value.as_bytes())),
        (Type::Bytes, Value::Bytes(value)) => Ok(length_prefixed(value)),
        (Type::Timestamp, Value::Timestamp(value)) => Ok(varint(value.as_micros())),
        (Type::Duration, Value::Duration(value)) => Ok(varint(*value)),
        (Type::Enum(schema), Value::Enum(index)) => {
            if *index as usize >= schema.variants().len() {
                return Err(StorageError::InvalidValue(format!(
                    "enum index {index} is out of range for {}",
                    schema.name()
                )));
            }
            Ok(varint(u64::from(*index)))
        }
        (Type::List(inner), Value::List(values)) => {
            let mut output = varint(values.len() as u64);
            for value in values {
                let encoded = encode_value(field, inner, value)?;
                put_uvarint(encoded.len() as u64, &mut output);
                output.extend(encoded);
            }
            Ok(output)
        }
        (Type::Map(key_type, value_type), Value::Map(entries)) => {
            let mut output = varint(entries.len() as u64);
            for (key, value) in entries {
                for encoded in [
                    encode_value(field, key_type, key)?,
                    encode_value(field, value_type, value)?,
                ] {
                    put_uvarint(encoded.len() as u64, &mut output);
                    output.extend(encoded);
                }
            }
            Ok(output)
        }
        (Type::Record(schema), Value::Record(data)) => encode_fields(schema, data),
        _ => Err(StorageError::TypeMismatch {
            field,
            expected: ty.to_string(),
        }),
    }
}

fn varint(value: u64) -> Vec<u8> {
    let mut output = Vec::with_capacity(10);
    put_uvarint(value, &mut output);
    output
}

fn length_prefixed(bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(bytes.len() + 4);
    put_uvarint(bytes.len() as u64, &mut output);
    output.extend_from_slice(bytes);
    output
}
