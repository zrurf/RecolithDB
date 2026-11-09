use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fjall::{
    OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, PersistMode, Readable, Slice,
};
use recolith_backend::{
    BackendError, CommitInfo, Durability, KeyRange, KeyValue, Result, ScanOptions, Transaction,
    TxnOptions,
};

use crate::error::map_error;
use crate::scan;

/// Fjall optimistic transaction.
pub struct FjallTransaction {
    database: OptimisticTxDatabase,
    keyspace: OptimisticTxKeyspace,
    transaction: OptimisticWriteTx,
    sequence: Arc<AtomicU64>,
    sync: bool,
    deferred_error: Option<BackendError>,
}

impl FjallTransaction {
    pub(crate) fn new(
        database: OptimisticTxDatabase,
        keyspace: OptimisticTxKeyspace,
        transaction: OptimisticWriteTx,
        sequence: Arc<AtomicU64>,
        options: TxnOptions,
    ) -> Self {
        Self {
            database,
            keyspace,
            transaction,
            sequence,
            sync: options.durability == Durability::Sync,
            deferred_error: None,
        }
    }

    /// Marks the key as read so optimistic commit detects overlapping writers.
    fn fence(&mut self, key: &[u8]) {
        if self.deferred_error.is_some() {
            return;
        }
        if let Err(error) = self.transaction.get(&self.keyspace, key) {
            self.deferred_error = Some(map_error(error));
        }
    }
}

impl Transaction for FjallTransaction {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.transaction
            .get(&self.keyspace, key)
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(map_error)
    }

    fn scan(&mut self, range: &KeyRange, options: ScanOptions) -> Result<Vec<KeyValue>> {
        scan::collect(
            self.transaction
                .range::<&[u8], _>(&self.keyspace, scan::bounds(range)),
            options,
        )
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.fence(key);
        if self.deferred_error.is_none() {
            self.transaction
                .insert(&self.keyspace, Slice::new(key), Slice::from(value));
        }
    }

    fn delete(&mut self, key: &[u8]) {
        self.fence(key);
        if self.deferred_error.is_none() {
            self.transaction.remove(&self.keyspace, Slice::new(key));
        }
    }

    fn commit(self) -> Result<CommitInfo> {
        let Self {
            database,
            transaction,
            sequence,
            sync,
            deferred_error,
            ..
        } = self;

        if let Some(error) = deferred_error {
            transaction.rollback();
            return Err(error);
        }

        match transaction.commit().map_err(map_error)? {
            Ok(()) => {
                if sync {
                    database.persist(PersistMode::SyncAll).map_err(map_error)?;
                }
                let sequence = sequence.fetch_add(1, Ordering::SeqCst) + 1;

                Ok(CommitInfo { sequence })
            }
            Err(_) => Err(BackendError::Conflict),
        }
    }

    fn rollback(self) {
        self.transaction.rollback();
    }
}

impl std::fmt::Debug for FjallTransaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FjallTransaction")
    }
}
