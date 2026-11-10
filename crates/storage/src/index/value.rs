use crate::error::{Result, StorageError};
use crate::value::Value;
use recolith_common::bytes::put_ordered_bytes as encode_ordered_bytes;

const TAG_BOOL: u8 = 0x01;
const TAG_I32: u8 = 0x02;
const TAG_I64: u8 = 0x03;
const TAG_U32: u8 = 0x04;
const TAG_U64: u8 = 0x05;
const TAG_STRING: u8 = 0x06;
const TAG_BYTES: u8 = 0x07;
const TAG_TIMESTAMP: u8 = 0x08;
const TAG_DURATION: u8 = 0x09;
const TAG_ENUM: u8 = 0x0A;

/// Returns the encoded length of the first index-key component.
///
/// # Errors
///
/// Returns [`StorageError::InvalidValue`] for truncated or unknown encodings.
pub fn index_value_len(input: &[u8]) -> Result<usize> {
    let tag = *input
        .first()
        .ok_or_else(|| StorageError::InvalidValue("index value is empty".to_owned()))?;
    let length = match tag {
        TAG_BOOL => 2,
        TAG_I32 | TAG_U32 | TAG_ENUM => 5,
        TAG_I64 | TAG_U64 | TAG_TIMESTAMP | TAG_DURATION => 9,
        TAG_STRING | TAG_BYTES => 1 + ordered_len(&input[1..])?,
        _ => {
            return Err(StorageError::InvalidValue(format!(
                "unknown index tag {tag:#x}"
            )));
        }
    };
    if length > input.len() {
        return Err(StorageError::InvalidValue(
            "index value is truncated".to_owned(),
        ));
    }

    Ok(length)
}

/// Returns the number of complete index-key components in `input`.
///
/// # Errors
///
/// Returns [`StorageError::InvalidValue`] for truncated or unknown encodings.
pub fn index_values_count(input: &[u8]) -> Result<usize> {
    let mut offset = 0;
    let mut count = 0;
    while offset < input.len() {
        let length = index_value_len(&input[offset..])?;
        offset += length;
        count += 1;
    }

    Ok(count)
}

/// Returns the encoded length of `count` index-key components.
///
/// # Errors
///
/// Returns [`StorageError::InvalidValue`] for truncated or unknown encodings.
pub fn index_values_len(input: &[u8], count: usize) -> Result<usize> {
    let mut offset = 0;
    for _ in 0..count {
        let length = index_value_len(&input[offset..])?;
        offset += length;
    }

    Ok(offset)
}

fn ordered_len(input: &[u8]) -> Result<usize> {
    let mut offset = 0;
    while offset < input.len() {
        match input[offset] {
            0 => {
                let next = input.get(offset + 1).ok_or_else(|| {
                    StorageError::InvalidValue("index string is truncated".to_owned())
                })?;
                match *next {
                    0 => return Ok(offset + 2),
                    0xFF => offset += 2,
                    _ => {
                        return Err(StorageError::InvalidValue(
                            "invalid index string escape".to_owned(),
                        ));
                    }
                }
            }
            _ => offset += 1,
        }
    }

    Err(StorageError::InvalidValue(
        "index string is not terminated".to_owned(),
    ))
}

/// Encodes a scalar value into an ordered index-key component.
///
/// Fixed-width types are stored big-endian with signed values biased by their
/// sign bit. Strings and bytes are NUL-terminated with embedded NUL bytes
/// escaped as `0x00 0xFF`, so component order matches logical order and
/// concatenated components stay unambiguous.
///
/// # Errors
///
/// Returns [`StorageError::UnindexableValue`] for floats, lists, maps, and
/// nested records.
pub fn encode_index_value(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Bool(value) => Ok(vec![TAG_BOOL, u8::from(*value)]),
        Value::I32(value) => {
            let mut output = Vec::with_capacity(5);
            output.push(TAG_I32);
            output.extend_from_slice(&((*value as u32) ^ 0x8000_0000).to_be_bytes());
            Ok(output)
        }
        Value::I64(value) => {
            let mut output = Vec::with_capacity(9);
            output.push(TAG_I64);
            output.extend_from_slice(&((*value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
            Ok(output)
        }
        Value::U32(value) => {
            let mut output = Vec::with_capacity(5);
            output.push(TAG_U32);
            output.extend_from_slice(&value.to_be_bytes());
            Ok(output)
        }
        Value::U64(value) => {
            let mut output = Vec::with_capacity(9);
            output.push(TAG_U64);
            output.extend_from_slice(&value.to_be_bytes());
            Ok(output)
        }
        Value::String(value) => {
            let mut output = Vec::with_capacity(value.len() + 3);
            output.push(TAG_STRING);
            encode_ordered_bytes(value.as_bytes(), &mut output);
            Ok(output)
        }
        Value::Bytes(value) => {
            let mut output = Vec::with_capacity(value.len() + 3);
            output.push(TAG_BYTES);
            encode_ordered_bytes(value, &mut output);
            Ok(output)
        }
        Value::Timestamp(timestamp) => {
            let mut output = Vec::with_capacity(9);
            output.push(TAG_TIMESTAMP);
            output.extend_from_slice(&timestamp.as_micros().to_be_bytes());
            Ok(output)
        }
        Value::Duration(value) => {
            let mut output = Vec::with_capacity(9);
            output.push(TAG_DURATION);
            output.extend_from_slice(&value.to_be_bytes());
            Ok(output)
        }
        Value::Enum(value) => {
            let mut output = Vec::with_capacity(5);
            output.push(TAG_ENUM);
            output.extend_from_slice(&value.to_be_bytes());
            Ok(output)
        }
        Value::F32(_) => Err(StorageError::UnindexableValue("f32")),
        Value::F64(_) => Err(StorageError::UnindexableValue("f64")),
        Value::List(_) => Err(StorageError::UnindexableValue("list")),
        Value::Map(_) => Err(StorageError::UnindexableValue("map")),
        Value::Record(_) => Err(StorageError::UnindexableValue("record")),
    }
}
