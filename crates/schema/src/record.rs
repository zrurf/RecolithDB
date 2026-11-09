use recolith_common::{SchemaId, SchemaVersion};

use crate::error::Result;
use crate::field::Field;
use crate::validate::validate_record;

/// Versioned record schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordSchema {
    id: SchemaId,
    version: SchemaVersion,
    name: String,
    fields: Vec<Field>,
}

impl RecordSchema {
    /// Creates a record schema.
    #[must_use]
    pub fn new(
        id: SchemaId,
        version: SchemaVersion,
        name: impl Into<String>,
        fields: Vec<Field>,
    ) -> Self {
        Self {
            id,
            version,
            name: name.into(),
            fields,
        }
    }

    /// Returns the schema identifier.
    #[must_use]
    pub fn id(&self) -> SchemaId {
        self.id
    }

    /// Returns the schema version.
    #[must_use]
    pub fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Returns the record name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the fields.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Returns a field by immutable ID.
    #[must_use]
    pub fn field(&self, id: recolith_common::FieldId) -> Option<&Field> {
        self.fields.iter().find(|field| field.id() == id)
    }

    /// Validates this record and its nested types.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SchemaError`] for invalid names, IDs, types, or duplicates.
    pub fn validate(&self) -> Result<()> {
        validate_record(self)
    }
}
