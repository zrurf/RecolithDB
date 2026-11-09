use std::fmt;

use recolith_backend::{CommitInfo, KeyRange, KeyValue, ScanOptions, StorageBackend, Transaction};

use crate::error::Result;

/// Thin typed wrapper over a backend transaction.
pub struct LocalTransaction<B: StorageBackend> {
    inner: B::Transaction,
}

impl<B: StorageBackend> LocalTransaction<B> {
    /// Wraps an already-begun backend transaction.
    #[must_use]
    pub fn new(inner: B::Transaction) -> Self {
        Self { inner }
    }

    /// Reads one key.
    ///
    /// # Errors
    ///
    /// Returns the backend error if the read fails.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.get(key)?)
    }

    /// Scans an ordered range.
    ///
    /// # Errors
    ///
    /// Returns the backend error if the scan fails.
    pub fn scan(&mut self, range: &KeyRange, options: ScanOptions) -> Result<Vec<KeyValue>> {
        Ok(self.inner.scan(range, options)?)
    }

    /// Stages a write.
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.inner.put(key, value);
    }

    /// Stages a deletion.
    pub fn delete(&mut self, key: &[u8]) {
        self.inner.delete(key);
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Returns the backend error if the commit fails.
    pub fn commit(self) -> Result<CommitInfo> {
        Ok(self.inner.commit()?)
    }

    /// Rolls back the transaction.
    pub fn rollback(self) {
        self.inner.rollback();
    }
}

impl<B: StorageBackend> fmt::Debug for LocalTransaction<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LocalTransaction")
    }
}
