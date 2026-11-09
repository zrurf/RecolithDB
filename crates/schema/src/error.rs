use std::fmt;

use recolith_common::FieldId;

/// Schema validation and IDL error.
#[derive(Debug)]
pub enum SchemaError {
    /// A record, field, or enum name is empty.
    EmptyName(&'static str),
    /// Field IDs must be positive.
    InvalidFieldId(FieldId),
    /// Two fields use the same ID.
    DuplicateFieldId(FieldId),
    /// Two fields use the same name.
    DuplicateFieldName(String),
    /// A record has no fields.
    EmptyRecord(String),
    /// A type is invalid in its context.
    InvalidType(String),
    /// An enum has no variants.
    EmptyEnum(String),
    /// Two enum variants use the same name.
    DuplicateEnumVariant(String),
    /// IDL text is malformed.
    Idl { offset: usize, message: String },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName(kind) => write!(formatter, "{kind} name must not be empty"),
            Self::InvalidFieldId(id) => write!(formatter, "field id must be positive: {id}"),
            Self::DuplicateFieldId(id) => write!(formatter, "duplicate field id: {id}"),
            Self::DuplicateFieldName(name) => write!(formatter, "duplicate field name: {name}"),
            Self::EmptyRecord(name) => {
                write!(formatter, "record {name} must have at least one field")
            }
            Self::InvalidType(message) => write!(formatter, "invalid type: {message}"),
            Self::EmptyEnum(name) => {
                write!(formatter, "enum {name} must have at least one variant")
            }
            Self::DuplicateEnumVariant(name) => {
                write!(formatter, "duplicate enum variant: {name}")
            }
            Self::Idl { offset, message } => {
                write!(formatter, "IDL error at byte {offset}: {message}")
            }
        }
    }
}

impl std::error::Error for SchemaError {}

/// Result alias for schema operations.
pub type Result<T> = std::result::Result<T, SchemaError>;
