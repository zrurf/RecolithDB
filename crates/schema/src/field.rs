use recolith_common::FieldId;

use crate::types::Type;

/// Schema field definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field {
    id: FieldId,
    name: String,
    ty: Type,
    optional: bool,
    unique: bool,
}

impl Field {
    /// Creates a field definition.
    #[must_use]
    pub fn new(id: FieldId, name: impl Into<String>, ty: Type, optional: bool) -> Self {
        Self {
            id,
            name: name.into(),
            ty,
            optional,
            unique: false,
        }
    }

    /// Marks the field as globally unique.
    #[must_use]
    pub const fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Returns the immutable field ID.
    #[must_use]
    pub fn id(&self) -> FieldId {
        self.id
    }

    /// Returns the field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field type.
    #[must_use]
    pub fn ty(&self) -> &Type {
        &self.ty
    }

    /// Returns whether the field may be absent.
    #[must_use]
    pub const fn optional(&self) -> bool {
        self.optional
    }

    /// Returns whether the field carries a global unique reservation.
    #[must_use]
    pub const fn unique(&self) -> bool {
        self.unique
    }
}
