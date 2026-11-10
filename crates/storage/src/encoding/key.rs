use recolith_common::bytes::{get_uvarint, put_uvarint};
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

/// Fully decoded record key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRecordKey {
    head: RecordKeyHead,
    document_key: Vec<u8>,
    fragment: FragmentId,
    subkey: Vec<u8>,
}

impl DecodedRecordKey {
    /// Returns the bucket and partition head.
    #[must_use]
    pub const fn head(&self) -> RecordKeyHead {
        self.head
    }

    /// Returns the document key.
    #[must_use]
    pub fn document_key(&self) -> &[u8] {
        &self.document_key
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }

    /// Returns the fragment subkey.
    #[must_use]
    pub fn subkey(&self) -> &[u8] {
        &self.subkey
    }
}

/// Decodes a complete record key.
///
/// Index keys share the bucket and partition prefix but include an index
/// identifier and encoded values instead of a document key, so they fail this
/// structural check.
///
/// # Errors
///
/// Returns [`crate::StorageError::InvalidKey`] when the key is truncated,
/// malformed, or has trailing bytes.
pub fn decode_record_key(key: &[u8]) -> crate::Result<DecodedRecordKey> {
    let head = decode_record_key_head(key)?;
    let mut offset = 12;
    let (document_key_len, consumed) = get_uvarint(&key[offset..])
        .map_err(|error| crate::StorageError::InvalidKey(error.to_string()))?;
    offset += consumed;
    let document_key_len = usize::try_from(document_key_len)
        .map_err(|_| crate::StorageError::InvalidKey("document key length overflows".to_owned()))?;
    let document_end = offset.checked_add(document_key_len).ok_or_else(|| {
        crate::StorageError::InvalidKey("document key length overflows".to_owned())
    })?;
    let document_key = key
        .get(offset..document_end)
        .ok_or_else(|| crate::StorageError::InvalidKey("document key is truncated".to_owned()))?;
    offset = document_end;

    let fragment_bytes = key
        .get(offset..offset + 4)
        .ok_or_else(|| crate::StorageError::InvalidKey("fragment id is truncated".to_owned()))?;
    let fragment = FragmentId::new(u32::from_be_bytes(
        fragment_bytes.try_into().expect("four bytes"),
    ));
    offset += 4;

    let (subkey_len, consumed) = get_uvarint(&key[offset..])
        .map_err(|error| crate::StorageError::InvalidKey(error.to_string()))?;
    offset += consumed;
    let subkey_len = usize::try_from(subkey_len)
        .map_err(|_| crate::StorageError::InvalidKey("subkey length overflows".to_owned()))?;
    let subkey_end = offset
        .checked_add(subkey_len)
        .ok_or_else(|| crate::StorageError::InvalidKey("subkey length overflows".to_owned()))?;
    if subkey_end != key.len() {
        return Err(crate::StorageError::InvalidKey(
            "record key length does not match".to_owned(),
        ));
    }

    Ok(DecodedRecordKey {
        head,
        document_key: document_key.to_vec(),
        fragment,
        subkey: key[offset..subkey_end].to_vec(),
    })
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
