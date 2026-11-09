use recolith_common::{CollectionId, FragmentId, PartitionId};

/// Request to read one document with an explicit fragment selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentReadRequest {
    collection_id: CollectionId,
    document_key: Vec<u8>,
    partition: PartitionId,
    fragments: Vec<FragmentId>,
}

impl DocumentReadRequest {
    /// Creates a document read request. An empty fragment list means all fragments.
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        document_key: impl Into<Vec<u8>>,
        partition: PartitionId,
    ) -> Self {
        Self {
            collection_id,
            document_key: document_key.into(),
            partition,
            fragments: Vec::new(),
        }
    }

    /// Adds one fragment to the selection.
    #[must_use]
    pub fn with_fragment(mut self, fragment: FragmentId) -> Self {
        self.fragments.push(fragment);
        self
    }

    /// Returns the collection identifier.
    #[must_use]
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Returns the document key.
    #[must_use]
    pub fn document_key(&self) -> &[u8] {
        &self.document_key
    }

    /// Returns the partition index.
    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the requested fragments.
    #[must_use]
    pub fn fragments(&self) -> &[FragmentId] {
        &self.fragments
    }
}
