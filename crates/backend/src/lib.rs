//! Ordered transactional KV storage backend contract.
#![forbid(unsafe_code)]

mod backend;
pub mod capabilities;
pub mod error;
pub mod key;
pub mod options;
pub mod snapshot;
pub mod stats;
pub mod transaction;

pub use backend::StorageBackend;
pub use capabilities::Capabilities;
pub use error::{BackendError, Result};
pub use key::{KeyRange, KeyValue, ScanOptions};
pub use options::{CommitInfo, Durability, TxnOptions};
pub use snapshot::Snapshot;
pub use stats::BackendStats;
pub use transaction::Transaction;
