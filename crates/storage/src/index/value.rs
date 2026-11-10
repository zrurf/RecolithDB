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
