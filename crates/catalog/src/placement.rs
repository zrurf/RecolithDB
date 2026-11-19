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
    pub const fn partition_count(&self) -> u32 {
        self.partition_count
    }
}

/// Role of one backend inside a bucket's replica set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaRole {
    /// Accepts writes for the bucket.
    Leader,
    /// Holds a replica of the bucket and rejects writes.
    Follower,
}

/// One backend inside a bucket's replica set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaMember {
    backend_id: BackendId,
    role: ReplicaRole,
}

impl ReplicaMember {
    /// Creates a replica member.
    #[must_use]
    pub const fn new(backend_id: BackendId, role: ReplicaRole) -> Self {
        Self { backend_id, role }
    }

    /// Returns the member backend.
    #[must_use]
    pub const fn backend_id(&self) -> BackendId {
        self.backend_id
    }

    /// Returns the member role.
    #[must_use]
    pub const fn role(&self) -> ReplicaRole {
        self.role
    }
}

/// Replica membership of one bucket across backends.
///
/// A replica set and a legacy single-backend placement are mutually
/// exclusive for the same bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaSet {
    bucket_id: BucketId,
    affinity_group: String,
    members: Vec<ReplicaMember>,
}

impl ReplicaSet {
    /// Creates a replica set without validation.
    #[must_use]
    pub fn new(
        bucket_id: BucketId,
        affinity_group: impl Into<String>,
        members: Vec<ReplicaMember>,
    ) -> Self {
        Self {
            bucket_id,
            affinity_group: affinity_group.into(),
            members,
        }
    }

    /// Returns the bucket the set belongs to.
    #[must_use]
    pub const fn bucket_id(&self) -> BucketId {
        self.bucket_id
    }

    /// Returns the affinity group.
    #[must_use]
    pub fn affinity_group(&self) -> &str {
        &self.affinity_group
    }

    /// Returns the set members.
    #[must_use]
    pub fn members(&self) -> &[ReplicaMember] {
        &self.members
    }

    /// Returns the leader backend.
    ///
    /// Validation guarantees exactly one leader, so this falls back to the
    /// first member only for unvalidated constructions.
    #[must_use]
    pub fn leader(&self) -> BackendId {
        self.members
            .iter()
            .find(|member| member.role() == ReplicaRole::Leader)
            .map_or_else(
                || {
                    self.members
                        .first()
                        .map_or(BackendId::new(0), ReplicaMember::backend_id)
                },
                ReplicaMember::backend_id,
            )
    }
}
