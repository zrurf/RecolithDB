use recolith_common::bytes::put_uvarint;
use recolith_common::{BucketId, FragmentId, PartitionId};

/// Encodes the prefix shared by every record in a bucket.
#[must_use]
pub fn encode_bucket_prefix(bucket: BucketId) -> Vec<u8> {
    bucket.get().to_be_bytes().to_vec()
}

/// Encodes the prefix shared by every record in a bucket partition.
#[must_use]
pub fn encode_partition_prefix(bucket: BucketId, partition: PartitionId) -> Vec<u8> {
    let mut output = Vec::with_capacity(12);
    output.extend_from_slice(&bucket.get().to_be_bytes());
    output.extend_from_slice(&partition.get().to_be_bytes());
    output
}

/// Encodes the prefix shared by every fragment of a document.
#[must_use]
pub fn encode_document_prefix(
    bucket: BucketId,
    partition: PartitionId,
    document_key: &[u8],
) -> Vec<u8> {
    let mut output = encode_partition_prefix(bucket, partition);
    put_uvarint(document_key.len() as u64, &mut output);
    output.extend_from_slice(document_key);
    output
}

/// Encodes an ordered record key.
#[must_use]
pub fn encode_record_key(
    bucket: BucketId,
    partition: PartitionId,
    document_key: &[u8],
    fragment: FragmentId,
    subkey: &[u8],
) -> Vec<u8> {
    let mut output = encode_document_prefix(bucket, partition, document_key);
    output.extend_from_slice(&fragment.get().to_be_bytes());
    put_uvarint(subkey.len() as u64, &mut output);
    output.extend_from_slice(subkey);
    output
}

/// Bucket and partition read from the head of a record key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordKeyHead {
    bucket: BucketId,
    partition: PartitionId,
}

impl RecordKeyHead {
    /// Returns the bucket identifier.
    #[must_use]
    pub const fn bucket(self) -> BucketId {
        self.bucket
    }

    /// Returns the partition index.
    #[must_use]
    pub const fn partition(self) -> PartitionId {
        self.partition
    }
}

/// Reads the bucket and partition prefix of a record key.
///
/// # Errors
///
/// Returns [`StorageError::InvalidKey`] when the key is shorter than its fixed prefix.
pub fn decode_record_key_head(key: &[u8]) -> crate::Result<RecordKeyHead> {
    let bytes = key.get(..12).ok_or_else(|| {
        crate::StorageError::InvalidKey("record key is shorter than its prefix".to_owned())
    })?;
    let bucket = u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes"));
    let partition = u32::from_be_bytes(bytes[8..].try_into().expect("four bytes"));

    Ok(RecordKeyHead {
        bucket: BucketId::new(bucket),
        partition: PartitionId::new(partition),
    })
}
