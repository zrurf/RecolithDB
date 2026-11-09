use fjall::{OptimisticTxKeyspace, Readable};
use recolith_backend::{KeyRange, KeyValue, Result, ScanOptions, Snapshot};

use crate::error::map_error;
use crate::scan;

/// Stable fjall read snapshot.
pub struct FjallSnapshot {
    snapshot: fjall::Snapshot,
    keyspace: OptimisticTxKeyspace,
}

impl FjallSnapshot {
    pub(crate) fn new(snapshot: fjall::Snapshot, keyspace: OptimisticTxKeyspace) -> Self {
        Self { snapshot, keyspace }
    }
}

impl Snapshot for FjallSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.snapshot
            .get(&self.keyspace, key)
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(map_error)
    }

    fn scan(&self, range: &KeyRange, options: ScanOptions) -> Result<Vec<KeyValue>> {
        scan::collect(
            self.snapshot
                .range::<&[u8], _>(&self.keyspace, scan::bounds(range)),
            options,
        )
    }
}

impl std::fmt::Debug for FjallSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FjallSnapshot")
    }
}
