use std::sync::Arc;

use recolith_backend::{StorageBackend, Transaction, TxnOptions};
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::{Catalog, load_catalog, save_catalog};
use recolith_common::{
    BucketId, CollectionId, CommitTs, FieldId, FragmentId, IndexId, KeyspaceId, PartitionId,
    SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{
    DocumentStore, FieldMapping, MigrationConversion, MigrationSource, OfflineMigration,
    SchemaMigration, migration_in_progress,
};
use recolith_storage::{
    RecordData, RecordHeader, Value, encode_index_value, encode_record_key, put_index_entry,
    scan_index, write_fields,
};

fn schema(version: u32, fields: Vec<Field>) -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(version),
        "profile",
        fields,
    )
}

fn catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .register_schema(schema(
            1,
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
            ],
        ))
        .expect("schema v1");
    catalog
        .register_schema(schema(
            2,
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U64, false),
                Field::new(FieldId::new(3), "nickname", Type::String, false),
            ],
        ))
        .expect("schema v2");
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(1),
                "player_profile",
                16,
                BucketPolicy::Strict,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("bucket");
    catalog
        .create_collection(Collection::new(
            CollectionId::new(1),
            KeyspaceId::new(1),
            "player",
            "player",
            DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
            vec![Fragment::new(
                FragmentId::new(1),
                "profile",
                BucketId::new(1),
                FragmentCardinality::One,
            )],
        ))
        .expect("collection");
    catalog
        .create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(1),
                "by-level",
                IndexKind::Range,
                vec![FieldId::new(2)],
            ),
        )
        .expect("index");

    catalog
}

fn backend(catalog: &Catalog) -> MemoryBackend {
    let backend = MemoryBackend::new();
    save_catalog(&backend, catalog).expect("save catalog");
    let schema = schema(
        1,
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, false),
        ],
    );
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    for (document, name, level) in [(b"one".as_slice(), "alex", 10_u32), (b"two", "sam", 20)] {
        let key = encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            document,
            FragmentId::new(1),
            &[],
        );
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String(name.to_owned()));
        data.insert(FieldId::new(2), Value::U32(level));
        write_fields(
            &mut transaction,
            &key,
            &RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(1)),
            &schema,
            &data,
        )
        .expect("write");
        put_index_entry(
            &mut transaction,
            BucketId::new(1),
            PartitionId::new(0),
            IndexId::new(1),
            &encode_index_value(&Value::U32(level)).expect("index value"),
            &key,
        );
    }
    transaction.commit().expect("commit");

    backend
}

fn migration() -> SchemaMigration {
    SchemaMigration {
        mappings: vec![
            FieldMapping {
                to: FieldId::new(2),
                source: MigrationSource::Field(FieldId::new(2)),
                conversion: MigrationConversion::U32ToU64,
            },
            FieldMapping {
                to: FieldId::new(3),
                source: MigrationSource::Default(Value::String("migrated".to_owned())),
                conversion: MigrationConversion::None,
            },
        ],
    }
}

#[test]
fn migrates_all_records_and_indexes_in_batches() {
    let catalog = catalog();
    let backend = backend(&catalog);
    let probe = backend.clone();
    let migrator = OfflineMigration::open(
        backend,
        BucketId::new(1),
        SchemaVersion::new(2),
        migration(),
    )
    .expect("open");
    assert!(migration_in_progress(&probe).expect("marker"));

    let stats = migrator.migrate(1).expect("migrate");
    assert_eq!(stats.migrated, 2);
    assert!(!migration_in_progress(&probe).expect("marker"));
}

#[test]
fn resumes_after_partial_batches() {
    let catalog = catalog();
    let backend = backend(&catalog);
    let probe = backend.clone();
    let mut migrator = OfflineMigration::open(
        backend.clone(),
        BucketId::new(1),
        SchemaVersion::new(2),
        migration(),
    )
    .expect("open");
    migrator.run_batch(1).expect("first batch");
    drop(migrator);
    assert!(migration_in_progress(&probe).expect("marker"));

    let migrator = OfflineMigration::open(
        backend,
        BucketId::new(1),
        SchemaVersion::new(2),
        migration(),
    )
    .expect("reopen");
    let stats = migrator.migrate(1).expect("resume");
    assert_eq!(stats.migrated + stats.skipped, 2);
    assert!(!migration_in_progress(&probe).expect("marker"));
}

#[test]
fn completed_migration_updates_catalog_and_indexes() {
    let catalog = catalog();
    let backend = backend(&catalog);
    let probe = backend.clone();
    let stats = OfflineMigration::open(
        backend.clone(),
        BucketId::new(1),
        SchemaVersion::new(2),
        migration(),
    )
    .expect("open")
    .migrate(1)
    .expect("migrate");
    assert_eq!(stats.migrated, 2);

    let loaded = load_catalog(&probe).expect("load");
    assert_eq!(
        loaded.snapshot().expect("snapshot").buckets[&BucketId::new(1)].schema_version(),
        SchemaVersion::new(2)
    );
    assert!(!migration_in_progress(&probe).expect("marker"));

    let snapshot = probe.snapshot().expect("snapshot");
    let old = scan_index(
        &snapshot,
        BucketId::new(1),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::U32(10)).expect("old"),
        recolith_backend::ScanOptions::default(),
    )
    .expect("old index");
    assert!(old.is_empty());
    let new = scan_index(
        &snapshot,
        BucketId::new(1),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::U64(10)).expect("new"),
        recolith_backend::ScanOptions::default(),
    )
    .expect("new index");
    assert_eq!(new.len(), 1);

    let store = DocumentStore::new(Arc::new(loaded), Arc::new(backend));
    let request =
        recolith_query::DocumentReadRequest::new(CollectionId::new(1), b"one", PartitionId::new(0))
            .with_fragment(FragmentId::new(1));
    let fragments = store.read(&request).expect("read");
    let record = &fragments[&FragmentId::new(1)][0];
    assert_eq!(record.header().schema_version(), SchemaVersion::new(2));
    assert_eq!(record.data().get(FieldId::new(2)), Some(&Value::U64(10)));
    assert_eq!(
        record.data().get(FieldId::new(3)),
        Some(&Value::String("migrated".to_owned()))
    );
}
