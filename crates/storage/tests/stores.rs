mod record_store {
    use recolith_backend::{StorageBackend, Transaction, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_common::{CommitTs, FieldId, SchemaId, SchemaVersion};
    use recolith_schema::{Field, RecordSchema, Type};
    use recolith_storage::{
        RecordData, RecordHeader, StorageError, Value, read_fields_from_snapshot, write_fields,
    };

    fn schema_with_version(version: u32) -> RecordSchema {
        RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(version),
            "player",
            vec![Field::new(FieldId::new(1), "name", Type::String, false)],
        )
    }

    fn schema() -> RecordSchema {
        schema_with_version(1)
    }

    #[test]
    fn writes_and_reads_fields() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("alex".to_owned()));

        write_fields(
            &mut transaction,
            b"player:1",
            &RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(7)),
            &schema(),
            &data,
        )
        .expect("write");
        transaction.commit().expect("commit");

        let snapshot = backend.snapshot().expect("snapshot");
        let (header, decoded) = read_fields_from_snapshot(&snapshot, b"player:1", &schema())
            .expect("read")
            .expect("record");
        assert_eq!(header.commit_ts(), CommitTs::new(7));
        assert_eq!(decoded, data);
    }

    #[test]
    fn rejects_write_with_mismatched_schema_header() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("alex".to_owned()));

        let result = write_fields(
            &mut transaction,
            b"player:1",
            &RecordHeader::new(SchemaId::new(1), SchemaVersion::new(2), CommitTs::new(7)),
            &schema(),
            &data,
        );

        assert!(matches!(result, Err(StorageError::SchemaMismatch { .. })));
    }

    #[test]
    fn rejects_read_with_mismatched_schema_version() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("alex".to_owned()));
        write_fields(
            &mut transaction,
            b"player:1",
            &RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(7)),
            &schema(),
            &data,
        )
        .expect("write");
        transaction.commit().expect("commit");

        let snapshot = backend.snapshot().expect("snapshot");
        let result = read_fields_from_snapshot(&snapshot, b"player:1", &schema_with_version(2));

        assert!(matches!(result, Err(StorageError::SchemaMismatch { .. })));
    }
}

mod index_store {
    use recolith_backend::{ScanOptions, StorageBackend, Transaction, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_common::{BucketId, IndexId, PartitionId};
    use recolith_storage::{StorageError, put_index_entry, put_unique_index_entry, scan_index};

    fn bucket() -> BucketId {
        BucketId::new(1)
    }

    fn partition() -> PartitionId {
        PartitionId::new(0)
    }

    #[test]
    fn indexes_and_scans_record_keys() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

        put_index_entry(
            &mut transaction,
            bucket(),
            partition(),
            IndexId::new(1),
            b"level:10",
            b"player:1",
        );
        put_index_entry(
            &mut transaction,
            bucket(),
            partition(),
            IndexId::new(1),
            b"level:10",
            b"player:2",
        );
        put_index_entry(
            &mut transaction,
            bucket(),
            partition(),
            IndexId::new(1),
            b"level:10",
            b"player:3",
        );
        transaction.commit().expect("commit");

        let snapshot = backend.snapshot().expect("snapshot");
        let keys = scan_index(
            &snapshot,
            bucket(),
            partition(),
            IndexId::new(1),
            b"level:10",
            ScanOptions::default(),
        )
        .expect("scan");
        let other_partition = scan_index(
            &snapshot,
            bucket(),
            PartitionId::new(1),
            IndexId::new(1),
            b"level:10",
            ScanOptions::default(),
        )
        .expect("scan");

        assert_eq!(
            keys,
            vec![
                b"player:1".to_vec(),
                b"player:2".to_vec(),
                b"player:3".to_vec()
            ]
        );
        assert!(other_partition.is_empty());
    }

    #[test]
    fn enforces_unique_index_ownership() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

        put_unique_index_entry(
            &mut transaction,
            bucket(),
            partition(),
            IndexId::new(1),
            b"name:alex",
            b"player:1",
        )
        .expect("first claim");
        put_unique_index_entry(
            &mut transaction,
            bucket(),
            partition(),
            IndexId::new(1),
            b"name:alex",
            b"player:1",
        )
        .expect("idempotent claim");

        let result = put_unique_index_entry(
            &mut transaction,
            bucket(),
            partition(),
            IndexId::new(1),
            b"name:alex",
            b"player:2",
        );

        assert!(matches!(result, Err(StorageError::UniqueViolation { .. })));
    }

    #[test]
    fn unique_index_is_partition_local() {
        let backend = MemoryBackend::new();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");

        put_unique_index_entry(
            &mut transaction,
            bucket(),
            PartitionId::new(0),
            IndexId::new(1),
            b"name:alex",
            b"player:1",
        )
        .expect("first partition");
        put_unique_index_entry(
            &mut transaction,
            bucket(),
            PartitionId::new(1),
            IndexId::new(1),
            b"name:alex",
            b"player:2",
        )
        .expect("second partition");
    }
}

mod document {
    use recolith_backend::{StorageBackend, Transaction, TxnOptions};
    use recolith_backend_memory::MemoryBackend;
    use recolith_common::{
        BucketId, CommitTs, FieldId, FragmentId, IndexId, PartitionId, SchemaId, SchemaVersion,
    };
    use recolith_schema::{Field, RecordSchema, Type};
    use recolith_storage::{
        FragmentMode, FragmentReader, RecordData, RecordHeader, Value, encode_index_value,
        encode_record_key, put_index_entry, read_document, write_fields,
    };

    fn schema() -> RecordSchema {
        RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "fragment",
            vec![Field::new(FieldId::new(1), "value", Type::U32, false)],
        )
    }

    fn data(value: u32) -> RecordData {
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::U32(value));
        data
    }

    #[test]
    fn assembles_one_and_many_fragments() {
        let backend = MemoryBackend::new();
        let schema = schema();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        let header = RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(1));

        write_fields(
            &mut transaction,
            &encode_record_key(
                BucketId::new(1),
                PartitionId::new(0),
                b"alex",
                FragmentId::new(1),
                b"",
            ),
            &header,
            &schema,
            &data(10),
        )
        .expect("one");
        for subkey in [b"1".as_slice(), b"2".as_slice()] {
            write_fields(
                &mut transaction,
                &encode_record_key(
                    BucketId::new(2),
                    PartitionId::new(0),
                    b"alex",
                    FragmentId::new(2),
                    subkey,
                ),
                &header,
                &schema,
                &data(20),
            )
            .expect("many");
        }
        transaction.commit().expect("commit");

        let snapshot = backend.snapshot().expect("snapshot");
        let fragments = vec![
            FragmentReader::new(
                BucketId::new(1),
                PartitionId::new(0),
                FragmentId::new(1),
                FragmentMode::One,
                schema.clone(),
            ),
            FragmentReader::new(
                BucketId::new(2),
                PartitionId::new(0),
                FragmentId::new(2),
                FragmentMode::Many,
                schema,
            ),
        ];
        let assembled = read_document(&snapshot, b"alex", &fragments).expect("assemble");

        assert_eq!(assembled.len(), 2);
        assert_eq!(assembled[&FragmentId::new(1)][0].subkey(), b"");
        assert_eq!(assembled[&FragmentId::new(1)][0].data(), &data(10));
        assert_eq!(assembled[&FragmentId::new(2)].len(), 2);
        assert_eq!(assembled[&FragmentId::new(2)][0].subkey(), b"1");
        assert_eq!(assembled[&FragmentId::new(2)][1].subkey(), b"2");
    }

    #[test]
    fn reads_many_fragment_through_index() {
        let backend = MemoryBackend::new();
        let schema = schema();
        let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
        let header = RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(1));

        for (document, subkey, value) in [
            (b"alex".as_slice(), b"1".as_slice(), 10),
            (b"alex".as_slice(), b"2".as_slice(), 20),
            (b"bob".as_slice(), b"1".as_slice(), 20),
        ] {
            let key = encode_record_key(
                BucketId::new(2),
                PartitionId::new(0),
                document,
                FragmentId::new(2),
                subkey,
            );
            write_fields(&mut transaction, &key, &header, &schema, &data(value)).expect("record");
            if value == 20 {
                put_index_entry(
                    &mut transaction,
                    BucketId::new(2),
                    PartitionId::new(0),
                    IndexId::new(1),
                    &encode_index_value(&Value::U32(value)).expect("index value"),
                    &key,
                );
            }
        }
        transaction.commit().expect("commit");

        let snapshot = backend.snapshot().expect("snapshot");
        let reader = FragmentReader::new(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(2),
            FragmentMode::Many,
            schema,
        )
        .with_index(
            IndexId::new(1),
            encode_index_value(&Value::U32(20)).expect("value"),
        );
        let assembled = read_document(&snapshot, b"alex", &[reader]).expect("assemble");

        assert_eq!(assembled[&FragmentId::new(2)].len(), 1);
        assert_eq!(assembled[&FragmentId::new(2)][0].subkey(), b"2");
        assert_eq!(assembled[&FragmentId::new(2)][0].data(), &data(20));
    }
}
