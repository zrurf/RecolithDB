use std::path::Path;

use crate::capabilities::Capabilities;
use crate::error::{BackendError, Result};
use crate::options::TxnOptions;
use crate::snapshot::Snapshot;
use crate::stats::BackendStats;
use crate::transaction::Transaction;

/// Ordered transactional KV engine contract.
pub trait StorageBackend: Send + Sync + 'static {
    /// Snapshot type.
    type Snapshot: Snapshot + Send + 'static;

    /// Transaction type.
    type Transaction: Transaction + Send + 'static;

    /// Returns the backend's capabilities.
    fn capabilities(&self) -> Capabilities;

    /// Opens a stable read snapshot.
    fn snapshot(&self) -> Result<Self::Snapshot>;

    /// Begins a read-write transaction.
    fn begin(&self, options: TxnOptions) -> Result<Self::Transaction>;

    /// Writes a checkpoint to `path` when supported.
    fn checkpoint(&self, _path: &Path) -> Result<()> {
        Err(BackendError::Unsupported("checkpoint"))
    }

    /// Returns backend statistics.
    fn stats(&self) -> BackendStats;
}
