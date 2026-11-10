mod atomic {
    use recolith_backend::{StorageBackend, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_transaction::{AtomicOp, LocalTransaction, apply_atomic};

    #[test]
    fn increments_counter() {
        let backend = MemoryBackend::new();
        let mut transaction: LocalTransaction<MemoryBackend> = LocalTransaction::new(
            backend
                .begin(TxnOptions::default())
                .expect("begin transaction"),
        );

        assert_eq!(
            apply_atomic(&mut transaction, AtomicOp::increment("counter", 5)).expect("increment"),
            Some(5)
        );
        assert_eq!(
            apply_atomic(&mut transaction, AtomicOp::increment("counter", -2)).expect("increment"),
            Some(3)
        );
    }

    #[test]
    fn compare_and_swap_checks_current_value() {
        let backend = MemoryBackend::new();
        let mut transaction: LocalTransaction<MemoryBackend> = LocalTransaction::new(
            backend
                .begin(TxnOptions::default())
                .expect("begin transaction"),
        );

        assert_eq!(
            apply_atomic(
                &mut transaction,
                AtomicOp::compare_and_swap("counter", 1, 7)
            )
            .expect("cas"),
            None
        );
        assert_eq!(
            apply_atomic(
                &mut transaction,
                AtomicOp::compare_and_swap("counter", 0, 7)
            )
            .expect("cas"),
            Some(7)
        );
    }
}

mod clock {
    use recolith_backend::{StorageBackend, Transaction, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_common::CommitTs;
    use recolith_transaction::PersistentCommitClock;

    #[test]
    fn loads_and_advances_clock() {
        let backend = MemoryBackend::new();
        let clock = PersistentCommitClock::with_default_key();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

        assert_eq!(
            clock.load(&mut transaction).expect("load"),
            CommitTs::new(0)
        );
        assert_eq!(
            clock.reserve(&mut transaction).expect("reserve"),
            CommitTs::new(1)
        );
        transaction.commit().expect("commit");

        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        let reloaded = PersistentCommitClock::with_default_key();
        assert_eq!(
            reloaded.load(&mut transaction).expect("load"),
            CommitTs::new(1)
        );
        assert_eq!(
            reloaded.reserve(&mut transaction).expect("reserve"),
            CommitTs::new(2)
        );
    }
}

mod local {
    use recolith_backend::{Snapshot, StorageBackend, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_transaction::LocalTransaction;

    #[test]
    fn commits_staged_writes() {
        let backend = MemoryBackend::new();
        let transaction = backend.begin(TxnOptions::default()).expect("begin");
        let mut transaction: LocalTransaction<MemoryBackend> = LocalTransaction::new(transaction);

        transaction.put(b"player:1", b"alex".to_vec());
        transaction.commit().expect("commit");

        let snapshot = backend.snapshot().expect("snapshot");
        assert_eq!(
            snapshot.get(b"player:1").expect("get"),
            Some(b"alex".to_vec())
        );
    }
}

mod local_write {
    use recolith_backend::{ScanOptions, Snapshot, StorageBackend, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_common::{
        BucketId, CommitTs, FieldId, FragmentId, IndexId, PartitionId, SchemaId, SchemaVersion,
    };
    use recolith_schema::{Field, RecordSchema, Type};
    use recolith_storage::{
        RecordData, StorageError, Value, encode_record_key, read_fields_from_snapshot, scan_index,
    };
    use recolith_transaction::{
        FragmentWrite, IndexUpdate, LocalWritePath, PersistentCommitClock, TransactionError,
    };

    fn schema() -> RecordSchema {
        RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "player",
            vec![Field::new(FieldId::new(1), "name", Type::String, false)],
        )
    }

    fn record_key(subkey: &[u8]) -> Vec<u8> {
        encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            b"player",
            FragmentId::new(1),
            subkey,
        )
    }

    #[test]
    fn writes_record_with_persistent_timestamp() {
        let backend = MemoryBackend::new();
        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("alex".to_owned()));

        let commit_ts = path
            .write_record(b"player:1", &schema(), &data)
            .expect("write");
        path.commit().expect("commit");

        assert_eq!(commit_ts, CommitTs::new(1));
        let snapshot = backend.snapshot().expect("snapshot");
        let (header, decoded) = read_fields_from_snapshot(&snapshot, b"player:1", &schema())
            .expect("read")
            .expect("record");
        assert_eq!(header.commit_ts(), commit_ts);
        assert_eq!(decoded, data);
    }

    #[test]
    fn enforces_optimistic_version_check() {
        let backend = MemoryBackend::new();
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("alex".to_owned()));

        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        let first = path
            .write_record(b"player:1", &schema(), &data)
            .expect("write");
        path.commit().expect("commit");

        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        let stale = path.write_record_if_match(b"player:1", &schema(), &data, CommitTs::new(0));
        assert!(matches!(
            stale,
            Err(TransactionError::VersionConflict { .. })
        ));
        path.rollback();

        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        let second = path
            .write_record_if_match(b"player:1", &schema(), &data, first)
            .expect("write");
        path.commit().expect("commit");

        assert_eq!(second, CommitTs::new(2));
    }

    #[test]
    fn maintains_unique_index_with_record_write() {
        let backend = MemoryBackend::new();
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("alex".to_owned()));
        let update = IndexUpdate::new(IndexId::new(1), None, b"name:alex".to_vec(), true);

        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        path.write_record_with_indexes(&record_key(b"1"), &schema(), &data, &[update])
            .expect("write");
        path.commit().expect("commit");

        let snapshot = backend.snapshot().expect("snapshot");
        let keys = scan_index(
            &snapshot,
            BucketId::new(1),
            PartitionId::new(0),
            IndexId::new(1),
            b"name:alex",
            ScanOptions::default(),
        )
        .expect("scan");
        assert_eq!(keys, vec![record_key(b"1")]);
    }

    #[test]
    fn rejects_duplicate_unique_index_claim() {
        let backend = MemoryBackend::new();
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("alex".to_owned()));
        let update = IndexUpdate::new(IndexId::new(1), None, b"name:alex".to_vec(), true);

        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        path.write_record_with_indexes(
            &record_key(b"1"),
            &schema(),
            &data,
            std::slice::from_ref(&update),
        )
        .expect("write");
        path.commit().expect("commit");

        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        let result = path.write_record_with_indexes(&record_key(b"2"), &schema(), &data, &[update]);
        assert!(matches!(
            result,
            Err(TransactionError::Storage(
                StorageError::UniqueViolation { .. }
            ))
        ));
        path.rollback();

        let snapshot = backend.snapshot().expect("snapshot");
        assert_eq!(snapshot.get(&record_key(b"2")).expect("get"), None);
    }

    #[test]
    fn writes_fragments_with_one_commit_timestamp() {
        let backend = MemoryBackend::new();
        let schema = schema();
        let mut profile = RecordData::new();
        profile.insert(FieldId::new(1), Value::String("alex".to_owned()));
        let mut equipment = RecordData::new();
        equipment.insert(FieldId::new(1), Value::String("sword".to_owned()));

        let mut path = LocalWritePath::begin(
            &backend,
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )
        .expect("begin");
        let commit_ts = path
            .write_fragments(&[
                FragmentWrite::new(b"player:1:profile", schema.clone(), profile.clone()),
                FragmentWrite::new(b"player:1:equipment", schema.clone(), equipment),
            ])
            .expect("write");
        path.commit().expect("commit");

        assert_eq!(commit_ts, CommitTs::new(1));
        let snapshot = backend.snapshot().expect("snapshot");
        let (profile_header, profile_data) =
            read_fields_from_snapshot(&snapshot, b"player:1:profile", &schema)
                .expect("profile")
                .expect("profile record");
        let (equipment_header, _) =
            read_fields_from_snapshot(&snapshot, b"player:1:equipment", &schema)
                .expect("equipment")
                .expect("equipment record");

        assert_eq!(profile_header.commit_ts(), commit_ts);
        assert_eq!(equipment_header.commit_ts(), commit_ts);
        assert_eq!(profile_data, profile);
    }
}

mod unique {
    use recolith_transaction::{ReservationState, UniqueReservation};

    #[test]
    fn confirms_reserved_claim() {
        let mut reservation = UniqueReservation::new(b"alex".to_vec(), b"player:1".to_vec(), 10);

        reservation.confirm().expect("confirm");
        assert_eq!(reservation.state(), ReservationState::Committed);
    }

    #[test]
    fn rejects_confirm_after_release() {
        let mut reservation = UniqueReservation::new(b"alex".to_vec(), b"player:1".to_vec(), 10);

        reservation.release().expect("release");
        assert!(reservation.confirm().is_err());
    }
}

mod batched_clock {
    use std::collections::HashSet;
    use std::sync::Arc;

    use recolith_backend::{StorageBackend, Transaction, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_common::CommitTs;
    use recolith_transaction::{BatchedCommitClock, DEFAULT_CLOCK_KEY, TransactionError};

    #[test]
    fn reserves_ranges_and_persists_high_water() {
        let backend = MemoryBackend::new();
        let clock = BatchedCommitClock::new(DEFAULT_CLOCK_KEY, 4);

        assert_eq!(clock.reserve(&backend).expect("first"), CommitTs::new(1));
        assert_eq!(clock.reserve(&backend).expect("second"), CommitTs::new(2));

        let reloaded = BatchedCommitClock::new(DEFAULT_CLOCK_KEY, 4);
        assert_eq!(
            reloaded.reserve(&backend).expect("reloaded"),
            CommitTs::new(5)
        );
        assert_eq!(clock.reserve(&backend).expect("third"), CommitTs::new(3));
    }

    #[test]
    fn exhausts_and_reserves_another_range() {
        let backend = MemoryBackend::new();
        let clock = BatchedCommitClock::new(DEFAULT_CLOCK_KEY, 2);

        assert_eq!(clock.reserve(&backend).expect("one"), CommitTs::new(1));
        assert_eq!(clock.reserve(&backend).expect("two"), CommitTs::new(2));
        assert_eq!(clock.reserve(&backend).expect("three"), CommitTs::new(3));
    }

    #[test]
    fn concurrent_reservations_are_unique() {
        let backend = MemoryBackend::new();
        let clock = Arc::new(BatchedCommitClock::new(DEFAULT_CLOCK_KEY, 8));

        let reservations = std::thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    let backend = &backend;
                    let clock = Arc::clone(&clock);
                    scope.spawn(move || {
                        (0..100)
                            .map(|_| clock.reserve(backend).expect("reserve").get())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();

            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("thread"))
                .collect::<Vec<_>>()
        });

        assert_eq!(reservations.len(), 800);
        assert_eq!(
            reservations.iter().copied().collect::<HashSet<_>>().len(),
            800
        );
        assert_eq!(reservations.iter().copied().min(), Some(1));
        assert_eq!(reservations.iter().copied().max(), Some(800));
    }

    #[test]
    fn rejects_malformed_clock_record() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        transaction.put(DEFAULT_CLOCK_KEY, vec![1, 2, 3]);
        transaction.commit().expect("commit");

        let clock = BatchedCommitClock::with_default_key();
        assert!(matches!(
            clock.reserve(&backend),
            Err(TransactionError::InvalidCommitClock { .. })
        ));
    }

    #[test]
    fn rejects_clock_overflow() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        transaction.put(DEFAULT_CLOCK_KEY, u64::MAX.to_be_bytes().to_vec());
        transaction.commit().expect("commit");

        let clock = BatchedCommitClock::with_default_key();
        assert!(matches!(
            clock.reserve(&backend),
            Err(TransactionError::CommitClockOverflow)
        ));
    }

    #[test]
    fn zero_batch_size_still_allocates() {
        let backend = MemoryBackend::new();
        let clock = BatchedCommitClock::new(DEFAULT_CLOCK_KEY, 0);

        assert_eq!(clock.batch_size(), 1);
        assert_eq!(clock.reserve(&backend).expect("reserve"), CommitTs::new(1));
    }
}

mod independent_batched_clocks {
    use std::collections::HashSet;
    use std::sync::Arc;

    use recolith_backend_memory::MemoryBackend;
    use recolith_transaction::{BatchedCommitClock, DEFAULT_CLOCK_KEY};

    #[test]
    fn independent_allocators_do_not_reuse_timestamps() {
        let backend = Arc::new(MemoryBackend::new());
        let first = Arc::new(BatchedCommitClock::new(DEFAULT_CLOCK_KEY, 4));
        let second = Arc::new(BatchedCommitClock::new(DEFAULT_CLOCK_KEY, 4));

        let reservations = std::thread::scope(|scope| {
            let handles = [first, second]
                .into_iter()
                .map(|clock| {
                    let backend = Arc::clone(&backend);
                    scope.spawn(move || {
                        (0..50)
                            .map(|_| clock.reserve(backend.as_ref()).expect("reserve").get())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();

            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("thread"))
                .collect::<Vec<_>>()
        });

        assert_eq!(reservations.len(), 100);
        assert_eq!(
            reservations.iter().copied().collect::<HashSet<_>>().len(),
            100
        );
        assert_eq!(reservations.iter().copied().min(), Some(1));
        let maximum = reservations.iter().copied().max().expect("maximum");
        assert!(
            (100..=104).contains(&maximum),
            "unexpected maximum {maximum}"
        );
    }
}
