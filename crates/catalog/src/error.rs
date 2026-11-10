use std::fmt;

use recolith_backend::BackendError;
use recolith_schema::SchemaError;

/// Catalog and schema-registry error.
#[derive(Debug)]
pub enum CatalogError {
    /// A metadata object already exists.
    Duplicate(&'static str),
    /// A metadata object exists with a different definition.
    Conflict(String),
    /// A metadata object is still referenced by another object.
    Referenced(String),
    /// A referenced metadata object does not exist.
    NotFound(&'static str),
    /// A collection references an unknown bucket or keyspace.
    InvalidReference(String),
    /// An index definition is invalid for its bucket.
    InvalidIndex(String),
    /// A placement record is invalid.
    InvalidPlacement(String),
    /// A persisted metadata value is malformed or unsupported.
    InvalidMetadata(String),
    /// A schema is invalid.
    Schema(SchemaError),
    /// A schema change requires a layout migration.
    SchemaRequiresMigration,
    /// A schema version is not newer than the current version.
    SchemaVersionConflict,
    /// Backend access failed.
    Backend(BackendError),
    /// An internal lock was poisoned.
    LockPoisoned,
}

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(kind) => write!(formatter, "duplicate {kind}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::Referenced(message) => write!(formatter, "object is still referenced: {message}"),
            Self::NotFound(kind) => write!(formatter, "missing {kind}"),
            Self::InvalidReference(message) => write!(formatter, "invalid reference: {message}"),
            Self::InvalidIndex(message) => write!(formatter, "invalid index: {message}"),
            Self::InvalidPlacement(message) => write!(formatter, "invalid placement: {message}"),
            Self::InvalidMetadata(message) => write!(formatter, "invalid metadata: {message}"),
            Self::Schema(error) => write!(formatter, "schema error: {error}"),
            Self::SchemaRequiresMigration => {
                formatter.write_str("schema change requires data migration")
            }
            Self::SchemaVersionConflict => {
                formatter.write_str("schema version must increase monotonically")
            }
            Self::Backend(error) => write!(formatter, "backend error: {error}"),
            Self::LockPoisoned => formatter.write_str("catalog lock poisoned"),
        }
    }
}

impl std::error::Error for CatalogError {}

impl From<SchemaError> for CatalogError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<BackendError> for CatalogError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

/// Result alias for catalog operations.
pub type Result<T> = std::result::Result<T, CatalogError>;
