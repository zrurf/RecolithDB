use recolith_backend::{KeyRange, ScanOptions, Snapshot, Transaction};
use recolith_common::{BucketId, IndexId, PartitionId};

use crate::error::{Result, StorageError};
use crate::index::{encode_index_entry_prefix, encode_index_key};

/// Writes an index entry through a backend transaction.
pub fn put_index_entry<T: Transaction>(
    transaction: &mut T,
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    encoded_values: &[u8],
    record_key: &[u8],
) {
    let key = encode_index_key(bucket, partition, index, encoded_values, record_key);
    transaction.put(&key, Vec::new());
}

/// Writes a unique index entry, rejecting a different record that already owns the value.
///
/// # Errors
///
/// Returns [`StorageError::UniqueViolation`] when another record already owns the value,
/// or a backend error when the scan or write fails.
pub fn put_unique_index_entry<T: Transaction>(
    transaction: &mut T,
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    encoded_values: &[u8],
    record_key: &[u8],
) -> Result<()> {
    let prefix = encode_index_entry_prefix(bucket, partition, index, encoded_values);
    let range = KeyRange::prefix(&prefix);
    let entries = transaction.scan(&range, ScanOptions::default())?;

    for (key, _) in entries {
        if key.get(prefix.len()..) != Some(record_key) {
            return Err(StorageError::UniqueViolation {
                index,
                value: encoded_values.to_vec(),
            });
        }
    }

    let key = encode_index_key(bucket, partition, index, encoded_values, record_key);
    transaction.put(&key, Vec::new());

    Ok(())
}

/// Deletes an index entry through a backend transaction.
pub fn delete_index_entry<T: Transaction>(
    transaction: &mut T,
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    encoded_values: &[u8],
    record_key: &[u8],
) {
    let key = encode_index_key(bucket, partition, index, encoded_values, record_key);
    transaction.delete(&key);
}

/// Scans an index value prefix and returns the matching record keys.
///
/// # Errors
///
/// Returns [`crate::StorageError`] when the backend scan fails.
pub fn scan_index<S: Snapshot>(
    snapshot: &S,
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    encoded_values: &[u8],
    options: ScanOptions,
) -> Result<Vec<Vec<u8>>> {
    let prefix = encode_index_entry_prefix(bucket, partition, index, encoded_values);
    let range = KeyRange::prefix(&prefix);
    let entries = snapshot.scan(&range, options)?;

    Ok(entries
        .into_iter()
        .map(|(key, _)| key[prefix.len()..].to_vec())
        .collect())
}
