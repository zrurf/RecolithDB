//! Reusable black-box contract checks for storage backends.

use recolith_backend::{
    BackendError, KeyRange, ScanOptions, Snapshot, StorageBackend, Transaction, TxnOptions,
};

/// Runs every applicable conformance check against a backend.
pub fn run_all<B: StorageBackend>(backend: &B) {
    commits_multiple_keys_atomically(backend);
    snapshot_remains_stable_across_commits(backend);
    if backend.capabilities().conflict_detection {
        detects_write_write_conflicts(backend);
    }
    scans_prefixes_in_key_order(backend);
    if backend.capabilities().reverse_scan {
        scans_prefixes_in_reverse_order(backend);
    }
    deletes_are_visible_after_commit(backend);
}

/// Two staged keys become visible together after commit.
pub fn commits_multiple_keys_atomically<B: StorageBackend>(backend: &B) {
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

    transaction.put(b"atomic:1", b"alex".to_vec());
    transaction.put(b"atomic:2", b"sam".to_vec());
    transaction.commit().expect("commit");

    let snapshot = backend.snapshot().expect("snapshot");
    assert_eq!(
        snapshot.get(b"atomic:1").expect("get"),
        Some(b"alex".to_vec())
    );
    assert_eq!(
        snapshot.get(b"atomic:2").expect("get"),
        Some(b"sam".to_vec())
    );
}

/// A snapshot does not observe later commits.
pub fn snapshot_remains_stable_across_commits<B: StorageBackend>(backend: &B) {
    let snapshot = backend.snapshot().expect("snapshot");
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

    transaction.put(b"snapshot:1", b"alex".to_vec());
    transaction.commit().expect("commit");

    assert_eq!(snapshot.get(b"snapshot:1").expect("get"), None);
}

/// Two overlapping writers cannot both commit.
pub fn detects_write_write_conflicts<B: StorageBackend>(backend: &B) {
    let mut first = backend.begin(TxnOptions::default()).expect("begin");
    let mut second = backend.begin(TxnOptions::default()).expect("begin");

    first.put(b"conflict:1", b"alex".to_vec());
    second.put(b"conflict:1", b"sam".to_vec());
    first.commit().expect("first commit");

    assert!(matches!(second.commit(), Err(BackendError::Conflict)));
}

/// Prefix scans return keys in ascending order and honor the limit.
pub fn scans_prefixes_in_key_order<B: StorageBackend>(backend: &B) {
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

    transaction.put(b"scan:a:2", b"second".to_vec());
    transaction.put(b"scan:a:1", b"first".to_vec());
    transaction.put(b"scan:b:1", b"other".to_vec());
    transaction.commit().expect("commit");

    let snapshot = backend.snapshot().expect("snapshot");
    let result = snapshot
        .scan(
            &KeyRange::prefix(b"scan:a:"),
            ScanOptions {
                limit: Some(1),
                reverse: false,
            },
        )
        .expect("scan");

    assert_eq!(result, vec![(b"scan:a:1".to_vec(), b"first".to_vec())]);
}

/// Reverse prefix scans return keys in descending order.
pub fn scans_prefixes_in_reverse_order<B: StorageBackend>(backend: &B) {
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

    transaction.put(b"reverse:1", b"one".to_vec());
    transaction.put(b"reverse:2", b"two".to_vec());
    transaction.put(b"reverse:3", b"three".to_vec());
    transaction.commit().expect("commit");

    let snapshot = backend.snapshot().expect("snapshot");
    let result = snapshot
        .scan(
            &KeyRange::prefix(b"reverse:"),
            ScanOptions {
                limit: Some(2),
                reverse: true,
            },
        )
        .expect("scan");

    assert_eq!(
        result,
        vec![
            (b"reverse:3".to_vec(), b"three".to_vec()),
            (b"reverse:2".to_vec(), b"two".to_vec()),
        ]
    );
}

/// A committed delete is invisible to later reads.
pub fn deletes_are_visible_after_commit<B: StorageBackend>(backend: &B) {
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    transaction.put(b"delete:1", b"alex".to_vec());
    transaction.commit().expect("commit");

    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    transaction.delete(b"delete:1");
    transaction.commit().expect("commit");

    let snapshot = backend.snapshot().expect("snapshot");
    assert_eq!(snapshot.get(b"delete:1").expect("get"), None);
}
