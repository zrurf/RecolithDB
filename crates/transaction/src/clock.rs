use std::sync::Mutex;
use std::thread;

use recolith_backend::{BackendError, StorageBackend, Transaction, TxnOptions};
use recolith_common::CommitTs;
use recolith_storage::CommitClock;

use crate::error::{Result, TransactionError};

/// Reserved key for the persistent commit clock.
pub const DEFAULT_CLOCK_KEY: &[u8] = b"\0recolith/commit_clock";
/// Default number of timestamps reserved in one clock transaction.
pub const DEFAULT_TIMESTAMP_BATCH: u64 = 1024;

const CLOCK_ALLOCATION_RETRIES: usize = 16;

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

#[derive(Debug, Default)]
struct BatchState {
    next: u64,
    end: u64,
}

/// Commit clock that reserves timestamp ranges from the backend.
///
/// The backend high-water mark is advanced to the end of a reserved range
/// before any timestamp is returned. A crash can therefore skip unused
/// timestamps, but can never reuse one.
#[derive(Debug)]
pub struct BatchedCommitClock {
    key: Vec<u8>,
    batch_size: u64,
    state: Mutex<BatchState>,
}

impl BatchedCommitClock {
    /// Creates a batched clock using `key`.
    ///
    /// A zero `batch_size` is treated as one.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, batch_size: u64) -> Self {
        Self {
            key: key.into(),
            batch_size: batch_size.max(1),
            state: Mutex::new(BatchState::default()),
        }
    }

    /// Creates a batched clock using [`DEFAULT_CLOCK_KEY`].
    #[must_use]
    pub fn with_default_key() -> Self {
        Self::new(DEFAULT_CLOCK_KEY, DEFAULT_TIMESTAMP_BATCH)
    }

    /// Returns the next timestamp in the current range or reserves a new range.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError`] for a malformed clock record, exhausted
    /// timestamp space, poisoned allocation state, or backend failure.
    pub fn reserve<B: StorageBackend>(&self, backend: &B) -> Result<CommitTs> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TransactionError::ClockLockPoisoned)?;
        if state.next < state.end {
            let timestamp = state.next;
            state.next += 1;
            return Ok(CommitTs::new(timestamp));
        }

        for _ in 0..CLOCK_ALLOCATION_RETRIES {
            let mut transaction = backend.begin(TxnOptions::default())?;
            let current = read_clock(&mut transaction, &self.key)?;
            let start = current
                .checked_add(1)
                .ok_or(TransactionError::CommitClockOverflow)?;
            let end = start
                .checked_add(self.batch_size)
                .ok_or(TransactionError::CommitClockOverflow)?;
            transaction.put(&self.key, (end - 1).to_be_bytes().to_vec());
            match transaction.commit() {
                Ok(_) => {
                    state.next = start + 1;
                    state.end = end;
                    return Ok(CommitTs::new(start));
                }
                Err(BackendError::Conflict) => thread::yield_now(),
                Err(error) => return Err(error.into()),
            }
        }

        Err(BackendError::Conflict.into())
    }

    /// Returns the configured range size.
    #[must_use]
    pub const fn batch_size(&self) -> u64 {
        self.batch_size
    }
}

fn read_clock<T: Transaction>(transaction: &mut T, key: &[u8]) -> Result<u64> {
    let Some(value) = transaction.get(key)? else {
        return Ok(0);
    };
    let bytes: [u8; 8] =
        value
            .as_slice()
            .try_into()
            .map_err(|_| TransactionError::InvalidCommitClock {
                length: value.len(),
            })?;

    Ok(u64::from_be_bytes(bytes))
}
