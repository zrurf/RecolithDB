use recolith_common::{BucketId, FragmentId};

/// Cardinality of a document fragment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentCardinality {
    /// At most one record per document.
    One,
    /// Zero or more records per document, distinguished by a subkey.
    Many,
}

/// Logical part of a document mapped to a bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fragment {
    id: FragmentId,
    name: String,
    bucket_id: BucketId,
    cardinality: FragmentCardinality,
}

impl Fragment {
    /// Creates a fragment descriptor.
    #[must_use]
    pub fn new(
        id: FragmentId,
        name: impl Into<String>,
        bucket_id: BucketId,
        cardinality: FragmentCardinality,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            bucket_id,
            cardinality,
        }
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub fn id(&self) -> FragmentId {
        self.id
    }

    /// Returns the fragment name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the owning bucket identifier.
    #[must_use]
    pub fn bucket_id(&self) -> BucketId {
        self.bucket_id
    }

    /// Returns the cardinality.
    #[must_use]
    pub fn cardinality(&self) -> FragmentCardinality {
        self.cardinality
    }
}
