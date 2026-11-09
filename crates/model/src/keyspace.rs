use recolith_common::KeyspaceId;

/// Tenant, game, or environment namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Keyspace {
    id: KeyspaceId,
    name: String,
}

impl Keyspace {
    /// Creates a keyspace descriptor.
    #[must_use]
    pub fn new(id: KeyspaceId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }

    /// Returns the keyspace identifier.
    #[must_use]
    pub fn id(&self) -> KeyspaceId {
        self.id
    }

    /// Returns the keyspace name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}
