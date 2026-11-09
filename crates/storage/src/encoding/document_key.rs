use recolith_model::{DocumentKeySchema, KeyType, KeyValue};

use crate::encoding::ordered::encode_ordered_bytes;
use crate::error::{Result, StorageError};

/// Maximum canonical document key length in bytes.
pub const MAX_DOCUMENT_KEY_LEN: usize = 256;

/// Encodes typed document key values into canonical ordered bytes.
///
/// Fixed-width components are big-endian and signed integers are biased by
/// their sign bit. Strings and bytes are self-terminating, so composite keys
/// stay unambiguous.
///
/// # Errors
///
/// Returns [`StorageError::InvalidKey`] for an empty schema, a value count or
/// type mismatch, or a key longer than [`MAX_DOCUMENT_KEY_LEN`].
pub fn encode_document_key(schema: &DocumentKeySchema, values: &[KeyValue]) -> Result<Vec<u8>> {
    if schema.fields().is_empty() {
        return Err(StorageError::InvalidKey(
            "document key schema has no fields".to_owned(),
        ));
    }
    if schema.fields().len() != values.len() {
        return Err(StorageError::InvalidKey(format!(
            "document key expects {} values, got {}",
            schema.fields().len(),
            values.len()
        )));
    }

    let mut output = Vec::new();
    for (field, value) in schema.fields().iter().zip(values) {
        match (field.ty(), value) {
            (KeyType::U64, KeyValue::U64(value)) => {
                output.extend_from_slice(&value.to_be_bytes());
            }
            (KeyType::I64, KeyValue::I64(value)) => {
                output.extend_from_slice(&((*value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
            }
            (KeyType::String, KeyValue::String(value)) => {
                encode_ordered_bytes(value.as_bytes(), &mut output);
            }
            (KeyType::Bytes, KeyValue::Bytes(value)) => {
                encode_ordered_bytes(value, &mut output);
            }
            (KeyType::Uuid, KeyValue::Uuid(value)) => {
                output.extend_from_slice(value);
            }
            (expected, _) => {
                return Err(StorageError::InvalidKey(format!(
                    "key field {} expects {expected}",
                    field.name()
                )));
            }
        }
    }

    if output.len() > MAX_DOCUMENT_KEY_LEN {
        return Err(StorageError::InvalidKey(format!(
            "document key is {} bytes, limit is {MAX_DOCUMENT_KEY_LEN}",
            output.len()
        )));
    }

    Ok(output)
}
