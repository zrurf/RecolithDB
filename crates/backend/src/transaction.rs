use crate::error::Result;
use crate::key::{KeyRange, KeyValue, ScanOptions};
use crate::options::CommitInfo;

/// Read-write transaction with snapshot isolation.
pub trait Transaction {
    /// Reads one key, observing this transaction's pending writes.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Scans an ordered range, observing this transaction's pending writes.
    fn scan(&mut self, range: &KeyRange, options: ScanOptions) -> Result<Vec<KeyValue>>;

    /// Stages a value write.
    fn put(&mut self, key: &[u8], value: Vec<u8>);

    /// Stages a deletion.
    fn delete(&mut self, key: &[u8]);

    /// Commits atomically.
    fn commit(self) -> Result<CommitInfo>;

    /// Discards all staged writes.
    fn rollback(self);
}
