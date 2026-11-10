use recolith_common::{BucketId, IndexId, PartitionId};

use crate::encoding::key::encode_partition_prefix;

pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }

    None
}

/// Encodes the prefix shared by every entry of one index in a bucket partition.
#[must_use]
pub fn encode_index_prefix(bucket: BucketId, partition: PartitionId, index: IndexId) -> Vec<u8> {
    let mut output = encode_partition_prefix(bucket, partition);
    output.extend_from_slice(&index.get().to_be_bytes());
    output
}

/// Encodes the prefix shared by all entries of one index value.
#[must_use]
pub fn encode_index_entry_prefix(
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    encoded_values: &[u8],
) -> Vec<u8> {
    let mut output = encode_index_prefix(bucket, partition, index);
    output.extend_from_slice(encoded_values);
    output
}

/// Encodes an index entry key, ending with the primary record key.
#[must_use]
pub fn encode_index_key(
    bucket: BucketId,
    partition: PartitionId,
    index: IndexId,
    encoded_values: &[u8],
    record_key: &[u8],
) -> Vec<u8> {
    let mut output = encode_index_entry_prefix(bucket, partition, index, encoded_values);
    output.extend_from_slice(record_key);
    output
}
