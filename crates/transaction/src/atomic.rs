use recolith_backend::StorageBackend;

use crate::error::{Result, TransactionError};
use crate::local::LocalTransaction;

/// Atomic operation over an 8-byte big-endian integer value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AtomicOp {
    /// Add a signed delta to a counter.
    Increment { key: Vec<u8>, delta: i64 },
    /// Replace the value when it equals `expected`.
    CompareAndSwap {
        key: Vec<u8>,
        expected: u64,
        value: u64,
    },
}

impl AtomicOp {
    /// Creates an increment operation.
    #[must_use]
    pub fn increment(key: impl Into<Vec<u8>>, delta: i64) -> Self {
        Self::Increment {
            key: key.into(),
            delta,
        }
    }

    /// Creates a compare-and-swap operation.
    #[must_use]
    pub fn compare_and_swap(key: impl Into<Vec<u8>>, expected: u64, value: u64) -> Self {
        Self::CompareAndSwap {
            key: key.into(),
            expected,
            value,
        }
    }
}

/// Applies an atomic operation inside a local transaction.
///
/// The current scaffold uses an 8-byte big-endian counter representation.
/// Schema-aware value encoding will replace it when the record value codec lands.
///
/// # Errors
///
/// Returns [`TransactionError`] for backend failures, malformed values, or overflow.
pub fn apply_atomic<B: StorageBackend>(
    transaction: &mut LocalTransaction<B>,
    operation: AtomicOp,
) -> Result<Option<u64>> {
    match operation {
        AtomicOp::Increment { key, delta } => {
            let current = read_counter(transaction, &key)?;
            let next = if delta >= 0 {
                current.checked_add(delta.unsigned_abs())
            } else {
                current.checked_sub(delta.unsigned_abs())
            }
            .ok_or(TransactionError::AtomicOverflow)?;

            transaction.put(&key, next.to_be_bytes().to_vec());
            Ok(Some(next))
        }
        AtomicOp::CompareAndSwap {
            key,
            expected,
            value,
        } => {
            let current = read_counter(transaction, &key)?;
            if current != expected {
                return Ok(None);
            }

            transaction.put(&key, value.to_be_bytes().to_vec());
            Ok(Some(value))
        }
    }
}

fn read_counter<B: StorageBackend>(
    transaction: &mut LocalTransaction<B>,
    key: &[u8],
) -> Result<u64> {
    let Some(value) = transaction.get(key)? else {
        return Ok(0);
    };
    let bytes: [u8; 8] =
        value
            .as_slice()
            .try_into()
            .map_err(|_| TransactionError::InvalidAtomicValue {
                length: value.len(),
            })?;

    Ok(u64::from_be_bytes(bytes))
}
