mod common;

use recolith_backend::{Durability, Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_backend_fjall::FjallBackend;

use common::TestDir;

#[test]
fn fjall_backend_satisfies_contract() {
    let dir = TestDir::new("conformance");
    let backend = FjallBackend::open(dir.path()).expect("open");

    recolith_backend_conformance::run_all(&backend);
}

#[test]
fn reopens_with_durable_data() {
    let dir = TestDir::new("reopen");
    {
        let backend = FjallBackend::open(dir.path()).expect("open");
        let mut transaction = backend
            .begin(TxnOptions {
                durability: Durability::Sync,
            })
            .expect("begin");
        transaction.put(b"persist:1", b"alex".to_vec());
        transaction.commit().expect("commit");
    }

    let backend = FjallBackend::open(dir.path()).expect("reopen");
    let snapshot = backend.snapshot().expect("snapshot");

    assert_eq!(
        snapshot.get(b"persist:1").expect("get"),
        Some(b"alex".to_vec())
    );
}
