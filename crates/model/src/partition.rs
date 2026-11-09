use recolith_common::{BucketId, PartitionId};

/// Physical partition of a bucket.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BucketPartition {
    bucket_id: BucketId,
    partition_id: PartitionId,
}

impl BucketPartition {
    /// Creates a bucket partition reference.
    #[must_use]
    pub const fn new(bucket_id: BucketId, partition_id: PartitionId) -> Self {
        Self {
            bucket_id,
            partition_id,
        }
    }

    /// Returns the owning bucket identifier.
    #[must_use]
    pub const fn bucket_id(self) -> BucketId {
        self.bucket_id
    }

    /// Returns the partition index.
    #[must_use]
    pub const fn partition_id(self) -> PartitionId {
        self.partition_id
    }
}
