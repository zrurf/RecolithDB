use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use recolith_backend::{
    BackendError, BackendStats, Capabilities, CommitInfo, KeyRange, KeyValue, Result, ScanOptions,
    Snapshot, StorageBackend, Transaction, TxnOptions,
};

#[derive(Clone, Debug, Default)]
struct State {
    entries: BTreeMap<Vec<u8>, Entry>,
    sequence: u64,
}

#[derive(Clone, Debug)]
struct Entry {
    value: Option<Vec<u8>>,
    sequence: u64,
}

/// In-memory ordered transactional backend.
#[derive(Debug)]
pub struct MemoryBackend {
    state: Arc<Mutex<State>>,
    capabilities: Capabilities,
}

impl MemoryBackend {
    /// Creates an empty in-memory backend.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            capabilities: Capabilities {
                snapshot_isolation: true,
                multi_key_atomic: true,
                conflict_detection: true,
                range_scan: true,
                reverse_scan: true,
                group_commit: false,
                checkpoint: false,
            },
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        lock_state(self.state.as_ref())
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MemoryBackend {
    type Snapshot = MemorySnapshot;
    type Transaction = MemoryTransaction;

    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        let state = self.lock()?;

        Ok(MemorySnapshot {
            state: state.clone(),
        })
    }

    fn begin(&self, _options: TxnOptions) -> Result<Self::Transaction> {
        let state = self.lock()?;

        Ok(MemoryTransaction {
            base: state.clone(),
            state: Arc::clone(&self.state),
            writes: BTreeMap::new(),
            reads: BTreeSet::new(),
        })
    }

    fn stats(&self) -> BackendStats {
        match self.lock() {
            Ok(state) => BackendStats {
                entries: state.entries.len() as u64,
                sequence: state.sequence,
            },
            Err(_) => BackendStats::default(),
        }
    }
}

/// Stable in-memory snapshot.
#[derive(Clone, Debug)]
pub struct MemorySnapshot {
    state: State,
}

impl Snapshot for MemorySnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .state
            .entries
            .get(key)
            .and_then(|entry| entry.value.clone()))
    }

    fn scan(&self, range: &KeyRange, options: ScanOptions) -> Result<Vec<KeyValue>> {
        Ok(scan_entries(&self.state.entries, range, options))
    }
}

/// In-memory read-write transaction.
#[derive(Debug)]
pub struct MemoryTransaction {
    base: State,
    state: Arc<Mutex<State>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    reads: BTreeSet<Vec<u8>>,
}

impl Transaction for MemoryTransaction {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.writes.get(key).cloned() {
            self.reads.insert(key.to_vec());
            return Ok(value);
        }

        let value = self
            .base
            .entries
            .get(key)
            .and_then(|entry| entry.value.clone());
        self.reads.insert(key.to_vec());

        Ok(value)
    }

    fn scan(&mut self, range: &KeyRange, options: ScanOptions) -> Result<Vec<KeyValue>> {
        let mut entries = self.base.entries.clone();
        for (key, value) in &self.writes {
            entries.insert(
                key.clone(),
                Entry {
                    value: value.clone(),
                    sequence: 0,
                },
            );
        }

        let result = scan_entries(&entries, range, options);
        for (key, _) in &result {
            self.reads.insert(key.clone());
        }

        Ok(result)
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        self.writes.insert(key.to_vec(), Some(value));
    }

    fn delete(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    fn commit(self) -> Result<CommitInfo> {
        let mut state = lock_state(self.state.as_ref())?;

        for key in self.reads.iter().chain(self.writes.keys()) {
            if let Some(entry) = state.entries.get(key)
                && entry.sequence > self.base.sequence
            {
                return Err(BackendError::Conflict);
            }
        }

        let sequence = state.sequence + 1;
        for (key, value) in self.writes {
            state.entries.insert(key, Entry { value, sequence });
        }
        state.sequence = sequence;

        Ok(CommitInfo { sequence })
    }

    fn rollback(self) {}
}

fn lock_state(state: &Mutex<State>) -> Result<MutexGuard<'_, State>> {
    state
        .lock()
        .map_err(|_| BackendError::Other("memory backend lock poisoned".to_owned()))
}

fn scan_entries(
    entries: &BTreeMap<Vec<u8>, Entry>,
    range: &KeyRange,
    options: ScanOptions,
) -> Vec<KeyValue> {
    let mut result: Vec<KeyValue> = entries
        .iter()
        .filter(|(key, _)| range.contains(key))
        .filter_map(|(key, entry)| {
            entry
                .value
                .as_ref()
                .map(|value| (key.clone(), value.clone()))
        })
        .collect();

    if options.reverse {
        result.reverse();
    }
    if let Some(limit) = options.limit {
        result.truncate(limit);
    }

    result
}
