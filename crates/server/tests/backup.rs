use std::fs;
use std::path::PathBuf;

use recolith_backend::{KeyRange, ScanOptions, Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_backend_memory::MemoryBackend;
use recolith_server::{BackupError, backup, restore, verify};

#[test]
fn backup_verify_restore_roundtrip() {
    let dir = temp_dir("roundtrip");
    let output = dir.join("data.backup");
    let source = MemoryBackend::new();
    let mut transaction = source.begin(TxnOptions::default()).expect("begin");
    transaction.put(b"alpha", b"one".to_vec());
    transaction.put(b"binary\0key", vec![0, 1, 2, 255]);
    transaction.put(b"empty", Vec::new());
    transaction.commit().expect("commit");

    let backed_up = backup(&source, &output).expect("backup");
    assert_eq!(backed_up.entries, 3);
    let verified = verify(&output).expect("verify");
    assert_eq!(verified, backed_up);

    let target = MemoryBackend::new();
    let restored = restore(&target, &output).expect("restore");
    assert_eq!(restored, backed_up);

    let source_entries = source
        .snapshot()
        .expect("source snapshot")
        .scan(&KeyRange::prefix(b""), ScanOptions::default())
        .expect("source scan");
    let target_entries = target
        .snapshot()
        .expect("target snapshot")
        .scan(&KeyRange::prefix(b""), ScanOptions::default())
        .expect("target scan");
    assert_eq!(target_entries, source_entries);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_existing_output_and_invalid_magic() {
    let dir = temp_dir("invalid");
    let output = dir.join("exists.backup");
    fs::write(&output, b"existing").expect("write");
    assert!(matches!(
        backup(&MemoryBackend::new(), &output),
        Err(BackupError::OutputExists(_))
    ));

    let invalid = dir.join("invalid.backup");
    fs::write(&invalid, b"not-a-backup").expect("write");
    assert!(matches!(verify(&invalid), Err(BackupError::InvalidMagic)));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_corrupt_truncated_and_trailing_backups() {
    let dir = temp_dir("corrupt");
    let output = dir.join("data.backup");
    let source = MemoryBackend::new();
    let mut transaction = source.begin(TxnOptions::default()).expect("begin");
    transaction.put(b"alpha", b"one".to_vec());
    transaction.commit().expect("commit");
    backup(&source, &output).expect("backup");

    let original = fs::read(&output).expect("read backup");
    let corrupt = dir.join("corrupt.backup");
    let mut bytes = original.clone();
    bytes[24] ^= 0x01;
    fs::write(&corrupt, bytes).expect("write corrupt");
    assert!(matches!(
        verify(&corrupt),
        Err(BackupError::Checksum { .. })
    ));

    let truncated = dir.join("truncated.backup");
    fs::write(&truncated, &original[..original.len() / 2]).expect("write truncated");
    assert!(verify(&truncated).is_err());

    let trailing = dir.join("trailing.backup");
    let mut bytes = original;
    bytes.push(0);
    fs::write(&trailing, bytes).expect("write trailing");
    assert!(matches!(verify(&trailing), Err(BackupError::TrailingData)));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_non_empty_restore_target() {
    let dir = temp_dir("target");
    let output = dir.join("data.backup");
    let source = MemoryBackend::new();
    let mut transaction = source.begin(TxnOptions::default()).expect("begin");
    transaction.put(b"alpha", b"one".to_vec());
    transaction.commit().expect("commit");
    backup(&source, &output).expect("backup");

    let target = MemoryBackend::new();
    let mut transaction = target.begin(TxnOptions::default()).expect("begin");
    transaction.put(b"existing", b"value".to_vec());
    transaction.commit().expect("commit");
    assert!(matches!(
        restore(&target, &output),
        Err(BackupError::TargetNotEmpty)
    ));

    let _ = fs::remove_dir_all(dir);
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("recolith-backup-{label}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("create temp dir");

    path
}

#[test]
fn backup_streams_large_entry_sets() {
    let dir = temp_dir("streaming");
    let output = dir.join("large.backup");
    let source = MemoryBackend::new();
    let mut transaction = source.begin(TxnOptions::default()).expect("begin");
    for index in 0..2_500_u32 {
        transaction.put(
            format!("key-{index:05}").as_bytes(),
            index.to_be_bytes().to_vec(),
        );
    }
    transaction.commit().expect("commit");

    let backed_up = backup(&source, &output).expect("backup");
    assert_eq!(backed_up.entries, 2_500);
    assert_eq!(verify(&output).expect("verify"), backed_up);

    let target = MemoryBackend::new();
    assert_eq!(restore(&target, &output).expect("restore"), backed_up);
    let entries = target
        .snapshot()
        .expect("snapshot")
        .scan(&KeyRange::prefix(b""), ScanOptions::default())
        .expect("scan");
    assert_eq!(entries.len(), 2_500);

    let _ = fs::remove_dir_all(dir);
}
