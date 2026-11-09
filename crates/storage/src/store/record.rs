use recolith_backend::{Snapshot, Transaction};
use recolith_schema::RecordSchema;

use crate::codec::{decode_fields, encode_fields};
use crate::error::{Result, StorageError};
use crate::record::{RecordHeader, decode_record, encode_record};
use crate::value::RecordData;

/// Owned decoded record frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRecord {
    header: RecordHeader,
    payload: Vec<u8>,
}

impl StoredRecord {
    /// Returns the record header.
    #[must_use]
    pub fn header(&self) -> &RecordHeader {
        &self.header
    }

    /// Returns the payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Writes a framed record through a backend transaction.
///
/// # Errors
///
/// Returns [`crate::StorageError`] when framing or the backend write fails.
pub fn put_record<T: Transaction>(
    transaction: &mut T,
    key: &[u8],
    header: &RecordHeader,
    payload: &[u8],
) -> Result<()> {
    let frame = encode_record(header, payload)?;
    transaction.put(key, frame);

    Ok(())
}

/// Reads a framed record through a backend transaction.
///
/// # Errors
///
/// Returns [`crate::StorageError`] when the backend or frame is invalid.
pub fn get_record<T: Transaction>(transaction: &mut T, key: &[u8]) -> Result<Option<StoredRecord>> {
    transaction.get(key)?.map(decode_owned).transpose()
}

/// Reads a framed record through a backend snapshot.
///
/// # Errors
///
/// Returns [`crate::StorageError`] when the backend or frame is invalid.
pub fn get_record_from_snapshot<S: Snapshot>(
    snapshot: &S,
    key: &[u8],
) -> Result<Option<StoredRecord>> {
    snapshot.get(key)?.map(decode_owned).transpose()
}

/// Encodes fields and writes a framed record.
///
/// # Errors
///
/// Returns [`crate::StorageError`] for field encoding, framing, or backend failures.
pub fn write_fields<T: Transaction>(
    transaction: &mut T,
    key: &[u8],
    header: &RecordHeader,
    schema: &RecordSchema,
    data: &RecordData,
) -> Result<()> {
    ensure_schema(header, schema)?;
    let payload = encode_fields(schema, data)?;
    put_record(transaction, key, header, &payload)
}

/// Reads a framed record and decodes its fields.
///
/// # Errors
///
/// Returns [`crate::StorageError`] for backend, framing, or field decoding failures.
pub fn read_fields<T: Transaction>(
    transaction: &mut T,
    key: &[u8],
    schema: &RecordSchema,
) -> Result<Option<(RecordHeader, RecordData)>> {
    let Some(stored) = get_record(transaction, key)? else {
        return Ok(None);
    };
    ensure_schema(stored.header(), schema)?;
    let data = decode_fields(schema, stored.payload())?;

    Ok(Some((stored.header, data)))
}

/// Reads a framed record from a snapshot and decodes its fields.
///
/// # Errors
///
/// Returns [`crate::StorageError`] for backend, framing, or field decoding failures.
pub fn read_fields_from_snapshot<S: Snapshot>(
    snapshot: &S,
    key: &[u8],
    schema: &RecordSchema,
) -> Result<Option<(RecordHeader, RecordData)>> {
    let Some(stored) = get_record_from_snapshot(snapshot, key)? else {
        return Ok(None);
    };
    ensure_schema(stored.header(), schema)?;
    let data = decode_fields(schema, stored.payload())?;

    Ok(Some((stored.header, data)))
}

/// Deletes a record through a backend transaction.
pub fn delete_record<T: Transaction>(transaction: &mut T, key: &[u8]) {
    transaction.delete(key);
}

pub(crate) fn ensure_schema(header: &RecordHeader, schema: &RecordSchema) -> Result<()> {
    if header.schema_id() != schema.id() || header.schema_version() != schema.version() {
        return Err(StorageError::SchemaMismatch {
            record_schema: header.schema_id(),
            record_version: header.schema_version(),
            expected_schema: schema.id(),
            expected_version: schema.version(),
        });
    }

    Ok(())
}

fn decode_owned(frame: Vec<u8>) -> Result<StoredRecord> {
    let decoded = decode_record(&frame)?;

    Ok(StoredRecord {
        header: decoded.header().clone(),
        payload: decoded.payload().to_vec(),
    })
}
