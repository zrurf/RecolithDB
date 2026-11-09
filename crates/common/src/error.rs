use std::fmt;

/// Common error type for foundational crates.
#[derive(Debug)]
pub enum Error {
    /// Input is syntactically or semantically invalid.
    InvalidInput(String),
    /// The requested object does not exist.
    NotFound(String),
    /// The operation conflicts with current state.
    Conflict(String),
    /// An invariant or dependency failed unexpectedly.
    Internal(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(formatter, "invalid input: {message}"),
            Self::NotFound(message) => write!(formatter, "not found: {message}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::Internal(message) => write!(formatter, "internal error: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// Result alias used by foundational crates.
pub type Result<T> = std::result::Result<T, Error>;
