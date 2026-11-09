use std::fmt;

/// Normalized backend failure.
#[derive(Debug)]
pub enum BackendError {
    /// The transaction conflicts with a committed write.
    Conflict,
    /// The backend is closed or shutting down.
    Closed,
    /// Persisted data is corrupt.
    Corruption(String),
    /// The underlying storage reported an I/O failure.
    Io(String),
    /// The backend does not implement the requested operation.
    Unsupported(&'static str),
    /// An invariant or unmapped failure.
    Other(String),
}

impl fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict => formatter.write_str("transaction conflict"),
            Self::Closed => formatter.write_str("backend is closed"),
            Self::Corruption(message) => write!(formatter, "corruption: {message}"),
            Self::Io(message) => write!(formatter, "I/O error: {message}"),
            Self::Unsupported(operation) => write!(formatter, "unsupported operation: {operation}"),
            Self::Other(message) => write!(formatter, "backend error: {message}"),
        }
    }
}

impl std::error::Error for BackendError {}

/// Result alias for backend operations.
pub type Result<T> = std::result::Result<T, BackendError>;
