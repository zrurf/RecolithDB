use std::fmt;

use recolith_backend::BackendError;
use recolith_common::CommitTs;
use recolith_storage::StorageError;

/// Transaction-layer error.
#[derive(Debug)]
pub enum TransactionError {
    /// Backend failure.
    Backend(BackendError),
    /// Atomic operation encountered a non-u64 value.
    InvalidAtomicValue { length: usize },
    /// Atomic increment overflowed.
    AtomicOverflow,
    /// Reservation transition is not allowed.
    InvalidReservationTransition {
        from: crate::unique::ReservationState,
        to: crate::unique::ReservationState,
    },
    /// Commit-clock record is malformed.
    InvalidCommitClock { length: usize },
    /// Storage encoding or record operation failed.
    Storage(StorageError),
    /// Optimistic version check failed.
    VersionConflict {
        expected: CommitTs,
        actual: Option<CommitTs>,
    },
    /// A batch write contained no fragments.
    EmptyWriteBatch,
}

impl fmt::Display for TransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(error) => write!(formatter, "backend error: {error}"),
            Self::InvalidAtomicValue { length } => {
                write!(formatter, "atomic value must be 8 bytes, got {length}")
            }
            Self::AtomicOverflow => formatter.write_str("atomic increment overflow"),
            Self::InvalidReservationTransition { from, to } => {
                write!(formatter, "invalid reservation transition: {from} -> {to}")
            }
            Self::InvalidCommitClock { length } => {
                write!(formatter, "commit clock must be 8 bytes, got {length}")
            }
            Self::Storage(error) => write!(formatter, "storage error: {error}"),
            Self::VersionConflict { expected, actual } => match actual {
                Some(actual) => write!(
                    formatter,
                    "version conflict: expected {expected}, found {actual}"
                ),
                None => write!(
                    formatter,
                    "version conflict: expected {expected}, record is absent"
                ),
            },
            Self::EmptyWriteBatch => formatter.write_str("write batch must contain a fragment"),
        }
    }
}

impl std::error::Error for TransactionError {}

impl From<BackendError> for TransactionError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

impl From<StorageError> for TransactionError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// Result alias for transaction operations.
pub type Result<T> = std::result::Result<T, TransactionError>;
