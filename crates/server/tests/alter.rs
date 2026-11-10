use std::sync::Arc;

use recolith_backend::{ScanOptions, StorageBackend};
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::{Catalog, CatalogAlter};
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, PartitionId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_query::DocumentReadRequest;
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{
    DocumentError, DocumentStore, DocumentWrite, FieldMapping, FragmentWriteRequest,
    MigrationConversion, MigrationSource, SchemaMigration,
};
use recolith_storage::{RecordData, Value, encode_index_value};

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
                Field::new(FieldId::new(2), "level", Type::U32, false),
                Field::new(FieldId::new(3), "nickname", Type::String, true),
            ],
        ))
        .expect("schema v2");
    catalog
        .register_schema(schema(
            3,
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
                Field::new(FieldId::new(3), "nickname", Type::String, false),
            ],
        ))
        .expect("schema v3");
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

fn data(name: &str, level: u32) -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String(name.to_owned()));
    data.insert(FieldId::new(2), Value::U32(level));
    data
}

fn write(store: &DocumentStore<MemoryBackend>, key: &[u8], data: RecordData) {
    store
        .write(
            &DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0)).with_fragment(
                FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), data),
            ),
        )
        .expect("write");
}

fn read(store: &DocumentStore<MemoryBackend>, key: &[u8]) -> RecordData {
    let request = DocumentReadRequest::new(CollectionId::new(1), key, PartitionId::new(0))
        .with_fragment(FragmentId::new(1));
    store.read(&request).expect("read")[&FragmentId::new(1)][0]
        .data()
        .clone()
}

#[test]
fn switches_compatible_schema_and_reads_old_records() {
    let backend = Arc::new(MemoryBackend::new());
    let store = DocumentStore::new(Arc::new(catalog()), backend);
    write(&store, b"one", data("alex", 10));

    store
        .alter_catalog(
            CatalogAlter::BucketSchema {
                bucket: BucketId::new(1),
                version: SchemaVersion::new(2),
            },
            None,
            None,
        )
        .expect("alter schema");

    let old = read(&store, b"one");
    assert_eq!(
        old.get(FieldId::new(1)),
        Some(&Value::String("alex".to_owned()))
    );
    assert_eq!(old.get(FieldId::new(3)), None);

    let mut updated = data("sam", 20);
    updated.insert(FieldId::new(3), Value::String("s".to_owned()));
    write(&store, b"two", updated);

    let current = read(&store, b"two");
    assert_eq!(
        current.get(FieldId::new(3)),
        Some(&Value::String("s".to_owned()))
    );
}

#[test]
fn requires_and_applies_explicit_migration() {
    let backend = Arc::new(MemoryBackend::new());
    let store = DocumentStore::new(Arc::new(catalog()), backend);
    write(&store, b"one", data("alex", 10));

    let alter = CatalogAlter::BucketSchema {
        bucket: BucketId::new(1),
        version: SchemaVersion::new(3),
    };
    assert!(matches!(
        store.alter_catalog(alter.clone(), None, None),
        Err(DocumentError::MigrationRequired)
    ));

    let migration = SchemaMigration {
        mappings: vec![FieldMapping {
            to: FieldId::new(3),
            source: MigrationSource::Default(Value::String("migrated".to_owned())),
            conversion: MigrationConversion::None,
        }],
    };
    store
        .alter_catalog(alter, Some(&migration), None)
        .expect("migrate");

    let request = DocumentReadRequest::new(CollectionId::new(1), b"one", PartitionId::new(0))
        .with_fragment(FragmentId::new(1));
    let fragments = store.read(&request).expect("read");
    let record = &fragments[&FragmentId::new(1)][0];
    assert_eq!(
        record.data().get(FieldId::new(3)),
        Some(&Value::String("migrated".to_owned()))
    );
    assert_eq!(record.header().schema_version(), SchemaVersion::new(3));
}

#[test]
fn redefines_index_and_rebuilds_entries() {
    let backend = Arc::new(MemoryBackend::new());
    let store = DocumentStore::new(Arc::new(catalog()), backend.clone());
    write(&store, b"one", data("alex", 10));
    write(&store, b"two", data("sam", 20));

    store
        .alter_catalog(
            CatalogAlter::Index {
                bucket: BucketId::new(1),
                index: IndexId::new(1),
                definition: IndexDefinition::new(
                    IndexId::new(1),
                    "by-name",
                    IndexKind::Equality,
                    vec![FieldId::new(1)],
                ),
            },
            None,
            None,
        )
        .expect("redefine index");

    let snapshot = backend.snapshot().expect("snapshot");
    let level = encode_index_value(&Value::U32(10)).expect("level");
    let old = recolith_storage::scan_index(
        &snapshot,
        BucketId::new(1),
        PartitionId::new(0),
        IndexId::new(1),
        &level,
        ScanOptions::default(),
    )
    .expect("old index scan");
    assert!(old.is_empty());

    let name = encode_index_value(&Value::String("alex".to_owned())).expect("name");
    let new = recolith_storage::scan_index(
        &snapshot,
        BucketId::new(1),
        PartitionId::new(0),
        IndexId::new(1),
        &name,
        ScanOptions::default(),
    )
    .expect("new index scan");
    assert_eq!(new.len(), 1);
    assert_eq!(
        new[0],
        recolith_storage::encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            b"one",
            FragmentId::new(1),
            &[],
        )
    );
}
