use recolith_common::{FieldId, IndexId};

/// Supported local index kinds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexKind {
    /// Equality lookup.
    Equality,
    /// Ordered range lookup.
    Range,
    /// Composite key lookup.
    Composite,
    /// Partition-local uniqueness.
    Unique,
}

/// Logical index definition.
///
/// `fields` are ordered: the first field is the leading field used for
/// equality and prefix lookups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDefinition {
    id: IndexId,
    name: String,
    kind: IndexKind,
    fields: Vec<FieldId>,
}

impl IndexDefinition {
    /// Creates an index definition.
    #[must_use]
    pub fn new(
        id: IndexId,
        name: impl Into<String>,
        kind: IndexKind,
        fields: Vec<FieldId>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            kind,
            fields,
        }
    }

    /// Returns the index identifier.
    #[must_use]
    pub fn id(&self) -> IndexId {
        self.id
    }

    /// Returns the index name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the index kind.
    #[must_use]
    pub fn kind(&self) -> IndexKind {
        self.kind
    }

    /// Returns the ordered index fields.
    #[must_use]
    pub fn fields(&self) -> &[FieldId] {
        &self.fields
    }

    /// Returns the leading field used for prefix lookups.
    #[must_use]
    pub fn leading_field(&self) -> Option<FieldId> {
        self.fields.first().copied()
    }
}
