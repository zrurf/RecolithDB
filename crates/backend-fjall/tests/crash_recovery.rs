mod common;

use std::process::Command;

use recolith_backend::{Durability, Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_backend_fjall::FjallBackend;

use common::TestDir;

const CRASH_DIR: &str = "RECOLITH_FJALL_CRASH_DIR";

#[test]
fn crash_child_entry() {
    let Some(path) = std::env::var_os(CRASH_DIR) else {
        return;
    };
    let backend = FjallBackend::open(path).expect("open child");
    let mut committed = backend
        .begin(TxnOptions {
            durability: Durability::Sync,
        })
        .expect("begin committed");
    committed.put(b"crash:committed", b"yes".to_vec());
    committed.commit().expect("commit");

    let mut staged = backend.begin(TxnOptions::default()).expect("begin staged");
    staged.put(b"crash:uncommitted", b"no".to_vec());

    std::process::exit(17);
}

#[test]
fn recovers_committed_and_discards_uncommitted_after_exit() {
    if std::env::var_os(CRASH_DIR).is_some() {
        return;
    }
    let dir = TestDir::new("crash");
    let status = Command::new(std::env::current_exe().expect("test executable"))
        .arg("--exact")
        .arg("crash_child_entry")
        .arg("--nocapture")
        .env(CRASH_DIR, dir.path())
        .status()
        .expect("spawn child");

    assert!(
        !status.success(),
        "child process must exit without unwinding"
    );

    let backend = FjallBackend::open(dir.path()).expect("reopen");
    let snapshot = backend.snapshot().expect("snapshot");

    assert_eq!(
        snapshot.get(b"crash:committed").expect("committed"),
        Some(b"yes".to_vec())
    );
    assert_eq!(
        snapshot.get(b"crash:uncommitted").expect("uncommitted"),
        None
    );
}
