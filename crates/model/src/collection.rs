use recolith_common::{CollectionId, KeyspaceId};

use crate::fragment::Fragment;
use crate::key::DocumentKeySchema;

/// Logical set of documents with a fragment mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Collection {
    id: CollectionId,
    keyspace_id: KeyspaceId,
    name: String,
    affinity_group: String,
    key: DocumentKeySchema,
    fragments: Vec<Fragment>,
}

impl Collection {
    /// Creates a collection descriptor.
    #[must_use]
    pub fn new(
        id: CollectionId,
        keyspace_id: KeyspaceId,
        name: impl Into<String>,
        affinity_group: impl Into<String>,
        key: DocumentKeySchema,
        fragments: Vec<Fragment>,
    ) -> Self {
        Self {
            id,
            keyspace_id,
            name: name.into(),
            affinity_group: affinity_group.into(),
            key,
            fragments,
        }
    }

    /// Returns the collection identifier.
    #[must_use]
    pub fn id(&self) -> CollectionId {
        self.id
    }

    /// Returns the owning keyspace identifier.
    #[must_use]
    pub fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    /// Returns the collection name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the affinity group name.
    #[must_use]
    pub fn affinity_group(&self) -> &str {
        &self.affinity_group
    }

    /// Returns the document key schema.
    #[must_use]
    pub fn key_schema(&self) -> &DocumentKeySchema {
        &self.key
    }

    /// Returns the fragment mapping.
    #[must_use]
    pub fn fragments(&self) -> &[Fragment] {
        &self.fragments
    }
}
