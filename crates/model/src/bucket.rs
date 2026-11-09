use recolith_common::{BucketId, SchemaId, SchemaVersion};

/// Write policy for a bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketPolicy {
    /// Only the document API may write.
    Strict,
    /// Direct record writes are allowed and local indexes are maintained.
    Managed,
    /// Direct record access only; no document view.
    Raw,
}

/// Logical, schema-homogeneous record container.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    id: BucketId,
    name: String,
    partition_count: u32,
    policy: BucketPolicy,
    schema_id: SchemaId,
    schema_version: SchemaVersion,
}

impl Bucket {
    /// Creates a bucket descriptor.
    #[must_use]
    pub fn new(
        id: BucketId,
        name: impl Into<String>,
        partition_count: u32,
        policy: BucketPolicy,
        schema_id: SchemaId,
        schema_version: SchemaVersion,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            partition_count,
            policy,
            schema_id,
            schema_version,
        }
    }

    /// Returns the bucket identifier.
    #[must_use]
    pub fn id(&self) -> BucketId {
        self.id
    }

    /// Returns the bucket name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the fixed partition count.
    #[must_use]
    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }

    /// Returns the write policy.
    #[must_use]
    pub fn policy(&self) -> BucketPolicy {
        self.policy
    }

    /// Returns the bound schema identifier.
    #[must_use]
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Returns the bound schema version.
    #[must_use]
    pub fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }
}
