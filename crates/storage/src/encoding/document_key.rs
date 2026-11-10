use recolith_model::{DocumentKeySchema, KeyValue};

use crate::error::{Result, StorageError};

pub use recolith_model::MAX_DOCUMENT_KEY_LEN;

/// Encodes typed document key values into canonical ordered bytes.
///
/// # Errors
///
/// Returns [`StorageError::InvalidKey`] for an invalid schema, value mismatch,
/// or key longer than [`MAX_DOCUMENT_KEY_LEN`].
pub fn encode_document_key(schema: &DocumentKeySchema, values: &[KeyValue]) -> Result<Vec<u8>> {
    schema
        .encode(values)
        .map_err(|error| StorageError::InvalidKey(error.to_string()))
}
