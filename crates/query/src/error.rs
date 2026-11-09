use std::fmt;

use recolith_backend::BackendError;
use recolith_catalog::CatalogError;
use recolith_storage::StorageError;

/// Query binding or execution error.
#[derive(Debug)]
pub enum QueryError {
    /// Backend snapshot or operation failed.
    Backend(BackendError),
    /// Catalog lookup or metadata failure.
    Catalog(CatalogError),
    /// Storage assembly or encoding failure.
    Storage(StorageError),
    /// A requested collection, fragment, bucket, or schema is missing.
    NotFound(&'static str),
    /// Request is internally inconsistent.
    InvalidRequest(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(error) => write!(formatter, "backend error: {error}"),
            Self::Catalog(error) => write!(formatter, "catalog error: {error}"),
            Self::Storage(error) => write!(formatter, "storage error: {error}"),
            Self::NotFound(kind) => write!(formatter, "missing {kind}"),
            Self::InvalidRequest(message) => write!(formatter, "invalid request: {message}"),
        }
    }
}

impl std::error::Error for QueryError {}

impl From<BackendError> for QueryError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

impl From<CatalogError> for QueryError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<StorageError> for QueryError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// Result alias for query operations.
pub type Result<T> = std::result::Result<T, QueryError>;
