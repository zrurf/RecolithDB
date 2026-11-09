use recolith_common::FieldId;

use crate::types::Type;

/// Schema field definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Field {
    id: FieldId,
    name: String,
    ty: Type,
    optional: bool,
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
        }
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
    pub fn optional(&self) -> bool {
        self.optional
    }
}
