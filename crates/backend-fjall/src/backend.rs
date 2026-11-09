use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fjall::{
    Keyspace, KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, PersistMode,
};
use recolith_backend::{BackendStats, Capabilities, Result, StorageBackend, TxnOptions};

use crate::error::map_error;
use crate::snapshot::FjallSnapshot;
use crate::transaction::FjallTransaction;

const KEYSPACE: &str = "recolith";

/// Fjall-backed ordered transactional store.
pub struct FjallBackend {
    database: OptimisticTxDatabase,
    keyspace: OptimisticTxKeyspace,
    sequence: Arc<AtomicU64>,
}

impl FjallBackend {
    /// Opens or creates a fjall database at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`recolith_backend::BackendError`] when the database cannot be
    /// opened or the keyspace cannot be created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let database = OptimisticTxDatabase::builder(path)
            .open()
            .map_err(map_error)?;
        let keyspace = database
            .keyspace(KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(map_error)?;

        Ok(Self {
            database,
            keyspace,
            sequence: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Forces the journal to durable storage.
    ///
    /// # Errors
    ///
    /// Returns [`recolith_backend::BackendError`] when the journal cannot be persisted.
    pub fn persist(&self) -> Result<()> {
        self.database
            .persist(PersistMode::SyncAll)
            .map_err(map_error)
    }
}

impl StorageBackend for FjallBackend {
    type Snapshot = FjallSnapshot;
    type Transaction = FjallTransaction;

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot_isolation: true,
            multi_key_atomic: true,
            conflict_detection: true,
            range_scan: true,
            reverse_scan: true,
            group_commit: false,
            checkpoint: false,
        }
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(FjallSnapshot::new(
            self.database.read_tx(),
            self.keyspace.clone(),
        ))
    }

    fn begin(&self, options: TxnOptions) -> Result<Self::Transaction> {
        let transaction = self.database.write_tx().map_err(map_error)?;

        Ok(FjallTransaction::new(
            self.database.clone(),
            self.keyspace.clone(),
            transaction,
            Arc::clone(&self.sequence),
            options,
        ))
    }

    fn stats(&self) -> BackendStats {
        let keyspace: &Keyspace = self.keyspace.as_ref();

        BackendStats {
            entries: keyspace.approximate_len() as u64,
            sequence: self.sequence.load(Ordering::SeqCst),
        }
    }
}

impl std::fmt::Debug for FjallBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FjallBackend")
    }
}
