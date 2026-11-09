use recolith_common::{BackendId, BucketId};

/// Placement of one bucket on a backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPlacement {
    bucket_id: BucketId,
    backend_id: BackendId,
    affinity_group: String,
    partition_count: u32,
}

impl BucketPlacement {
    /// Creates a bucket placement.
    #[must_use]
    pub fn new(
        bucket_id: BucketId,
        backend_id: BackendId,
        affinity_group: impl Into<String>,
        partition_count: u32,
    ) -> Self {
        Self {
            bucket_id,
            backend_id,
            affinity_group: affinity_group.into(),
            partition_count,
        }
    }

    /// Returns the placed bucket.
    #[must_use]
    pub fn bucket_id(&self) -> BucketId {
        self.bucket_id
    }

    /// Returns the assigned backend.
    #[must_use]
    pub fn backend_id(&self) -> BackendId {
        self.backend_id
    }

    /// Returns the affinity group.
    #[must_use]
    pub fn affinity_group(&self) -> &str {
        &self.affinity_group
    }

    /// Returns the partition count.
    #[must_use]
    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }
}
