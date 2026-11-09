use recolith_backend::Transaction;
use recolith_common::CommitTs;
use recolith_storage::CommitClock;

use crate::error::{Result, TransactionError};

/// Reserved key for the persistent commit clock.
pub const DEFAULT_CLOCK_KEY: &[u8] = b"\0recolith/commit_clock";

/// Commit clock backed by a reserved key in the backend.
#[derive(Debug)]
pub struct PersistentCommitClock {
    key: Vec<u8>,
    clock: CommitClock,
}

impl PersistentCommitClock {
    /// Creates a persistent clock using `key`.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            clock: CommitClock::default(),
        }
    }

    /// Creates a persistent clock using [`DEFAULT_CLOCK_KEY`].
    #[must_use]
    pub fn with_default_key() -> Self {
        Self::new(DEFAULT_CLOCK_KEY)
    }

    /// Loads the persisted high-water mark and advances the in-memory clock.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::InvalidCommitClock`] for a malformed record,
    /// or a backend error if the read fails.
    pub fn load<T: Transaction>(&self, transaction: &mut T) -> Result<CommitTs> {
        let Some(value) = transaction.get(&self.key)? else {
            return Ok(self.clock.high_water_mark());
        };
        let bytes: [u8; 8] =
            value
                .as_slice()
                .try_into()
                .map_err(|_| TransactionError::InvalidCommitClock {
                    length: value.len(),
                })?;
        let timestamp = CommitTs::new(u64::from_be_bytes(bytes));
        self.clock.observe(timestamp);

        Ok(timestamp)
    }

    /// Reserves the next timestamp and stages the new high-water mark.
    ///
    /// # Errors
    ///
    /// Backend errors are returned from the staging write.
    pub fn reserve<T: Transaction>(&self, transaction: &mut T) -> Result<CommitTs> {
        let timestamp = self.clock.reserve();
        transaction.put(&self.key, timestamp.get().to_be_bytes().to_vec());

        Ok(timestamp)
    }

    /// Returns the in-memory high-water mark.
    #[must_use]
    pub fn high_water_mark(&self) -> CommitTs {
        self.clock.high_water_mark()
    }
}
