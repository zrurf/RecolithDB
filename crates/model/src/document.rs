use recolith_common::CollectionId;

/// Canonical document key bytes.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DocumentKey(Vec<u8>);

impl DocumentKey {
    /// Creates a document key from canonical bytes.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the canonical key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Virtual aggregate identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    collection_id: CollectionId,
    key: DocumentKey,
}

impl Document {
    /// Creates a document reference.
    #[must_use]
    pub fn new(collection_id: CollectionId, key: DocumentKey) -> Self {
        Self { collection_id, key }
    }

    /// Returns the owning collection identifier.
    #[must_use]
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Returns the document key.
    #[must_use]
    pub fn key(&self) -> &DocumentKey {
        &self.key
    }
}
