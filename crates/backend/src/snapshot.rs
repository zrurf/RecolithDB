use crate::error::Result;
use crate::key::{KeyRange, KeyValue, ScanOptions};

/// Stable read view over a backend.
pub trait Snapshot {
    /// Reads one key from the snapshot.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Scans an ordered range from the snapshot.
    fn scan(&self, range: &KeyRange, options: ScanOptions) -> Result<Vec<KeyValue>>;
}
