use std::ops::Bound;

use recolith_backend::{KeyRange, ScanOptions, Snapshot, Transaction};
use recolith_common::{BucketId, IndexId, PartitionId};

use crate::error::{Result, StorageError};
use crate::index::{
    encode_index_entry_prefix, encode_index_key, encode_index_prefix, index_values_count,
    index_values_len, prefix_end,
};

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

/// Scans an index value prefix and strips `components` encoded values.
///
/// # Errors
///
/// Returns [`crate::StorageError`] when the backend scan fails or an index key
/// is malformed.
pub fn scan_index_components<S: Snapshot>(
    snapshot: &S,
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    encoded_values: &[u8],
    components: usize,
    options: ScanOptions,
) -> Result<Vec<Vec<u8>>> {
    let prefix = encode_index_entry_prefix(bucket, partition, index, encoded_values);
    let prefix_components = index_values_count(encoded_values)?;
    if prefix_components > components {
        return Err(StorageError::InvalidValue(
            "index prefix has more components than the index".to_owned(),
        ));
    }
    let remaining = components - prefix_components;
    let entries = snapshot.scan(&KeyRange::prefix(&prefix), options)?;
    let mut record_keys = Vec::with_capacity(entries.len());
    for (key, _) in entries {
        let rest = key.get(prefix.len()..).ok_or_else(|| {
            StorageError::InvalidKey("index key is shorter than its prefix".to_owned())
        })?;
        let values_len = index_values_len(rest, remaining)?;
        let record_key = rest
            .get(values_len..)
            .ok_or_else(|| StorageError::InvalidKey("index key has no record key".to_owned()))?;
        record_keys.push(record_key.to_vec());
    }

    Ok(record_keys)
}

/// Scans an index over an encoded value range.
///
/// Lower and upper bounds are encoded index values paired with an inclusive
/// flag. The returned values are record keys.
///
/// # Errors
///
/// Returns [`crate::StorageError`] when the backend scan fails or an index key
/// is malformed.
#[allow(clippy::too_many_arguments)]
pub fn scan_index_range<S: Snapshot>(
    snapshot: &S,
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    lower: Option<(&[u8], bool)>,
    upper: Option<(&[u8], bool)>,
    components: usize,
    options: ScanOptions,
) -> Result<Vec<Vec<u8>>> {
    let prefix = encode_index_prefix(bucket, partition, index);
    let mut start = Bound::Included(prefix.clone());
    let mut end = prefix_end(&prefix).map_or(Bound::Unbounded, Bound::Excluded);

    if let Some((encoded, inclusive)) = lower {
        let mut key = prefix.clone();
        key.extend_from_slice(encoded);
        start = if inclusive {
            Bound::Included(key)
        } else {
            match prefix_end(&key) {
                Some(next) => Bound::Included(next),
                None => return Ok(Vec::new()),
            }
        };
    }
    if let Some((encoded, inclusive)) = upper {
        let mut key = prefix.clone();
        key.extend_from_slice(encoded);
        end = if inclusive {
            prefix_end(&key).map_or(end, Bound::Excluded)
        } else {
            Bound::Excluded(key)
        };
    }

    let entries = snapshot.scan(&KeyRange::new(start, end), options)?;
    let mut record_keys = Vec::with_capacity(entries.len());
    for (key, _) in entries {
        let rest = key.get(prefix.len()..).ok_or_else(|| {
            StorageError::InvalidKey("index key is shorter than its prefix".to_owned())
        })?;
        let values_len = index_values_len(rest, components)?;
        let record_key = rest
            .get(values_len..)
            .ok_or_else(|| StorageError::InvalidKey("index key has no record key".to_owned()))?;
        record_keys.push(record_key.to_vec());
    }

    Ok(record_keys)
}
