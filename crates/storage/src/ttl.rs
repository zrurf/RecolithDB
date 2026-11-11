//! Document expiry keys and helpers.

use recolith_backend::Snapshot;
use recolith_common::{CollectionId, PartitionId};

use crate::{Result, StorageError};

const TTL_PREFIX: &[u8] = b"\0recolith/ttl/";
const TTL_INDEX_PREFIX: &[u8] = b"\0recolith/ttl-index/";
const INDEX_HEADER_LEN: usize = 8 + 8 + 4;
const TTL_VALUE_LEN: usize = 8;

/// Encodes the expiry metadata key for one document.
#[must_use]
pub fn encode_ttl_key(
    collection: CollectionId,
    partition: PartitionId,
    document_key: &[u8],
) -> Vec<u8> {
    let mut output = Vec::with_capacity(TTL_PREFIX.len() + 12 + document_key.len());
    output.extend_from_slice(TTL_PREFIX);
    output.extend_from_slice(&collection.get().to_be_bytes());
    output.extend_from_slice(&partition.get().to_be_bytes());
    output.extend_from_slice(document_key);

    output
}

/// Encodes the ordered expiry index key for one document.
#[must_use]
pub fn encode_ttl_index_key(
    expires_at_micros: u64,
    collection: CollectionId,
    partition: PartitionId,
    document_key: &[u8],
) -> Vec<u8> {
    let mut output =
        Vec::with_capacity(TTL_INDEX_PREFIX.len() + INDEX_HEADER_LEN + document_key.len());
    output.extend_from_slice(TTL_INDEX_PREFIX);
    output.extend_from_slice(&expires_at_micros.to_be_bytes());
    output.extend_from_slice(&collection.get().to_be_bytes());
    output.extend_from_slice(&partition.get().to_be_bytes());
    output.extend_from_slice(document_key);

    output
}

/// Encodes an expiry timestamp value.
#[must_use]
pub const fn encode_ttl_value(expires_at_micros: u64) -> [u8; TTL_VALUE_LEN] {
    expires_at_micros.to_be_bytes()
}

/// Decodes an expiry timestamp value.
#[must_use]
pub fn decode_ttl_value(value: &[u8]) -> Option<u64> {
    if value.len() != TTL_VALUE_LEN {
        return None;
    }

    Some(u64::from_be_bytes(value.try_into().expect("eight bytes")))
}

/// Decoded expiry index entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TtlIndexEntry {
    expires_at_micros: u64,
    collection: CollectionId,
    partition: PartitionId,
    document_key: Vec<u8>,
}

impl TtlIndexEntry {
    /// Returns the absolute expiry timestamp.
    #[must_use]
    pub const fn expires_at_micros(&self) -> u64 {
        self.expires_at_micros
    }

    /// Returns the collection identifier.
    #[must_use]
    pub const fn collection(&self) -> CollectionId {
        self.collection
    }

    /// Returns the partition index.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the document key.
    #[must_use]
    pub fn document_key(&self) -> &[u8] {
        &self.document_key
    }
}

/// Decodes an expiry index key.
#[must_use]
pub fn decode_ttl_index_key(key: &[u8]) -> Option<TtlIndexEntry> {
    let rest = key.strip_prefix(TTL_INDEX_PREFIX)?;
    if rest.len() < INDEX_HEADER_LEN {
        return None;
    }
    let expires_at_micros = u64::from_be_bytes(rest[..8].try_into().expect("eight bytes"));
    let collection = CollectionId::new(u64::from_be_bytes(
        rest[8..16].try_into().expect("eight bytes"),
    ));
    let partition = PartitionId::new(u32::from_be_bytes(
        rest[16..20].try_into().expect("four bytes"),
    ));

    Some(TtlIndexEntry {
        expires_at_micros,
        collection,
        partition,
        document_key: rest[INDEX_HEADER_LEN..].to_vec(),
    })
}

/// Returns the prefix shared by every expiry index entry.
#[must_use]
pub const fn ttl_index_prefix() -> &'static [u8] {
    TTL_INDEX_PREFIX
}

/// Returns whether a document is expired at `now_micros`.
///
/// # Errors
///
/// Returns [`StorageError`] when the snapshot read fails or the expiry record is
/// malformed.
pub fn is_expired<S: Snapshot>(
    snapshot: &S,
    collection: CollectionId,
    partition: PartitionId,
    document_key: &[u8],
    now_micros: u64,
) -> Result<bool> {
    let key = encode_ttl_key(collection, partition, document_key);
    let Some(value) = snapshot.get(&key)? else {
        return Ok(false);
    };
    let expires_at = decode_ttl_value(&value)
        .ok_or_else(|| StorageError::InvalidValue("malformed document expiry record".to_owned()))?;

    Ok(expires_at <= now_micros)
}
