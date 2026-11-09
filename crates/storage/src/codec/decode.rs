use recolith_common::bytes::get_uvarint;
use recolith_common::{FieldId, Timestamp};
use recolith_schema::{RecordSchema, Type};

use super::wire::field_wire;
use crate::error::{Result, StorageError};
use crate::value::{RecordData, Value};

/// Decodes known fields and preserves unknown raw entries.
///
/// # Errors
///
/// Returns [`StorageError`] for truncated input, duplicate fields, type mismatches,
/// invalid values, or missing required fields.
pub fn decode_fields(schema: &RecordSchema, payload: &[u8]) -> Result<RecordData> {
    let mut data = RecordData::new();
    let mut offset = 0;

    while offset < payload.len() {
        let entry_start = offset;
        let field_id = read_field_id(payload, &mut offset)?;

        if offset >= payload.len() {
            return Err(StorageError::InvalidValue(
                "truncated field wire type".to_owned(),
            ));
        }
        let wire = payload[offset];
        offset += 1;

        let value_len = usize::try_from(read_varint(payload, &mut offset)?)
            .map_err(|_| StorageError::InvalidValue("field length overflows usize".to_owned()))?;
        let value_end = offset
            .checked_add(value_len)
            .ok_or_else(|| StorageError::InvalidValue("field length overflows".to_owned()))?;
        if value_end > payload.len() {
            return Err(StorageError::InvalidValue(
                "field value exceeds payload".to_owned(),
            ));
        }
        let value_bytes = &payload[offset..value_end];
        offset = value_end;

        if let Some(field) = schema.field(field_id) {
            let expected = field_wire(field.ty());
            if wire != expected {
                return Err(StorageError::TypeMismatch {
                    field: field_id,
                    expected: field.ty().to_string(),
                });
            }
            if data.get(field_id).is_some() {
                return Err(StorageError::DuplicateField(field_id));
            }

            data.insert(field_id, decode_value(field_id, field.ty(), value_bytes)?);
        } else {
            if data.unknown().contains_key(&field_id) {
                return Err(StorageError::DuplicateField(field_id));
            }
            data.insert_unknown(field_id, payload[entry_start..value_end].to_vec());
        }
    }

    for field in schema.fields() {
        if !field.optional() && data.get(field.id()).is_none() {
            return Err(StorageError::MissingField(field.id()));
        }
    }

    Ok(data)
}

fn read_field_id(payload: &[u8], offset: &mut usize) -> Result<FieldId> {
    let value = u32::try_from(read_varint(payload, offset)?)
        .map_err(|_| StorageError::InvalidValue("field ID overflows u32".to_owned()))?;

    Ok(FieldId::new(value))
}

fn read_varint(payload: &[u8], offset: &mut usize) -> Result<u64> {
    let (value, consumed) = get_uvarint(&payload[*offset..])
        .map_err(|error| StorageError::InvalidValue(error.to_string()))?;
    *offset += consumed;

    Ok(value)
}

fn decode_value(field: FieldId, ty: &Type, bytes: &[u8]) -> Result<Value> {
    match ty {
        Type::Bool => match read_varint_exact(bytes)? {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            value => Err(StorageError::InvalidValue(format!(
                "boolean field {field} has value {value}"
            ))),
        },
        Type::I32 => Ok(Value::I32(i32::from_le_bytes(fixed_array(bytes, field)?))),
        Type::I64 => Ok(Value::I64(i64::from_le_bytes(fixed_array(bytes, field)?))),
        Type::U32 => {
            let value = u32::try_from(read_varint_exact(bytes)?)
                .map_err(|_| StorageError::InvalidValue(format!("u32 field {field} overflows")))?;
            Ok(Value::U32(value))
        }
        Type::U64 => Ok(Value::U64(read_varint_exact(bytes)?)),
        Type::F32 => Ok(Value::F32(f32::from_bits(u32::from_le_bytes(fixed_array(
            bytes, field,
        )?)))),
        Type::F64 => Ok(Value::F64(f64::from_bits(u64::from_le_bytes(fixed_array(
            bytes, field,
        )?)))),
        Type::String => {
            let bytes = read_length_prefixed(bytes, field)?;
            let value = String::from_utf8(bytes.to_vec()).map_err(|_| {
                StorageError::InvalidValue(format!("string field {field} is not UTF-8"))
            })?;
            Ok(Value::String(value))
        }
        Type::Bytes => Ok(Value::Bytes(read_length_prefixed(bytes, field)?.to_vec())),
        Type::Timestamp => Ok(Value::Timestamp(Timestamp::from_micros(read_varint_exact(
            bytes,
        )?))),
        Type::Duration => Ok(Value::Duration(read_varint_exact(bytes)?)),
        Type::Enum(schema) => {
            let index = u32::try_from(read_varint_exact(bytes)?)
                .map_err(|_| StorageError::InvalidValue(format!("enum field {field} overflows")))?;
            if index as usize >= schema.variants().len() {
                return Err(StorageError::InvalidValue(format!(
                    "enum field {field} has out-of-range index {index}"
                )));
            }
            Ok(Value::Enum(index))
        }
        Type::List(inner) => {
            let (count, mut offset) = read_count(bytes, field)?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let encoded = read_blob(bytes, &mut offset, field)?;
                values.push(decode_value(field, inner, encoded)?);
            }
            if offset != bytes.len() {
                return Err(StorageError::InvalidValue(format!(
                    "list field {field} has trailing bytes"
                )));
            }
            Ok(Value::List(values))
        }
        Type::Map(key_type, value_type) => {
            let (count, mut offset) = read_count(bytes, field)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let encoded_key = read_blob(bytes, &mut offset, field)?;
                let key = decode_value(field, key_type, encoded_key)?;
                let encoded_value = read_blob(bytes, &mut offset, field)?;
                let value = decode_value(field, value_type, encoded_value)?;
                entries.push((key, value));
            }
            if offset != bytes.len() {
                return Err(StorageError::InvalidValue(format!(
                    "map field {field} has trailing bytes"
                )));
            }
            Ok(Value::Map(entries))
        }
        Type::Record(schema) => Ok(Value::Record(Box::new(decode_fields(schema, bytes)?))),
    }
}

fn read_blob<'a>(bytes: &'a [u8], offset: &mut usize, field: FieldId) -> Result<&'a [u8]> {
    let length = usize::try_from(read_varint(bytes, offset)?)
        .map_err(|_| StorageError::InvalidValue(format!("field {field} length overflows")))?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| StorageError::InvalidValue(format!("field {field} length overflows")))?;
    if end > bytes.len() {
        return Err(StorageError::InvalidValue(format!(
            "field {field} length exceeds collection"
        )));
    }

    let blob = &bytes[*offset..end];
    *offset = end;
    Ok(blob)
}

fn read_count(bytes: &[u8], field: FieldId) -> Result<(usize, usize)> {
    let (value, consumed) =
        get_uvarint(bytes).map_err(|error| StorageError::InvalidValue(error.to_string()))?;
    let count = usize::try_from(value)
        .map_err(|_| StorageError::InvalidValue("count overflows usize".to_owned()))?;
    if count > bytes.len() {
        return Err(StorageError::InvalidValue(format!(
            "collection field {field} count exceeds payload"
        )));
    }

    Ok((count, consumed))
}

fn read_varint_exact(bytes: &[u8]) -> Result<u64> {
    let (value, consumed) =
        get_uvarint(bytes).map_err(|error| StorageError::InvalidValue(error.to_string()))?;
    if consumed != bytes.len() {
        return Err(StorageError::InvalidValue(
            "varint field has trailing bytes".to_owned(),
        ));
    }

    Ok(value)
}

fn read_length_prefixed(bytes: &[u8], field: FieldId) -> Result<&[u8]> {
    let (length, consumed) =
        get_uvarint(bytes).map_err(|error| StorageError::InvalidValue(error.to_string()))?;
    let length = usize::try_from(length)
        .map_err(|_| StorageError::InvalidValue(format!("field {field} length overflows")))?;
    let end = consumed
        .checked_add(length)
        .ok_or_else(|| StorageError::InvalidValue(format!("field {field} length overflows")))?;
    if end != bytes.len() {
        return Err(StorageError::InvalidValue(format!(
            "field {field} length does not match payload"
        )));
    }

    Ok(&bytes[consumed..end])
}

fn fixed_array<const N: usize>(bytes: &[u8], field: FieldId) -> Result<[u8; N]> {
    bytes.try_into().map_err(|_| {
        StorageError::InvalidValue(format!(
            "field {field} expects {N} bytes, got {}",
            bytes.len()
        ))
    })
}
