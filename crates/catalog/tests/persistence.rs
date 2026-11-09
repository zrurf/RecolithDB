use recolith_backend::{Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_backend_memory::MemoryBackend;
use recolith_common::{
    BackendId, BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};

use recolith_catalog::store::CATALOG_PREFIX;
use recolith_catalog::{BucketPlacement, Catalog, load_catalog, save_catalog};

fn schema(version: u32, optional: bool) -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(version),
        "player",
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, optional),
        ],
    )
}

fn populated_catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .register_schema(schema(1, false))
        .expect("schema v1");
    catalog.register_schema(schema(2, true)).expect("schema v2");
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
                SchemaVersion::new(2),
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
                "by_level",
                IndexKind::Equality,
                vec![FieldId::new(2)],
            ),
        )
        .expect("index");
    catalog
        .place_bucket(BucketPlacement::new(
            BucketId::new(1),
            BackendId::new(1),
            "player",
            16,
        ))
        .expect("placement");

    catalog
}

#[test]
fn saves_and_loads_catalog_snapshot() {
    let backend = MemoryBackend::new();
    let catalog = populated_catalog();
    save_catalog(&backend, &catalog).expect("save");

    let loaded = load_catalog(&backend).expect("load");
    let snapshot = loaded.snapshot().expect("snapshot");

    assert_eq!(snapshot.generation, catalog.generation());
    assert_eq!(snapshot.keyspaces.len(), 1);
    assert_eq!(snapshot.buckets.len(), 1);
    assert_eq!(snapshot.collections.len(), 1);
    assert_eq!(snapshot.schemas.schemas.len(), 2);
    assert_eq!(
        snapshot
            .collection_by_name("player")
            .expect("collection")
            .key_schema()
            .field("player_id")
            .expect("key field")
            .ty(),
        KeyType::U64
    );
    assert_eq!(
        snapshot
            .index_for_field(BucketId::new(1), FieldId::new(2))
            .expect("index")
            .name(),
        "by_level"
    );
    assert_eq!(
        snapshot.placements[&BucketId::new(1)].backend_id(),
        BackendId::new(1)
    );
}

#[test]
fn loads_empty_backend_as_empty_catalog() {
    let backend = MemoryBackend::new();
    let catalog = load_catalog(&backend).expect("load");

    assert_eq!(catalog.generation().get(), 0);
    assert!(catalog.snapshot().expect("snapshot").collections.is_empty());
}

#[test]
fn save_removes_unknown_catalog_keys() {
    let backend = MemoryBackend::new();
    let catalog = populated_catalog();
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    let mut stale = CATALOG_PREFIX.to_vec();
    stale.extend_from_slice(b"stale");
    transaction.put(&stale, vec![1]);
    transaction.commit().expect("commit");

    save_catalog(&backend, &catalog).expect("save");

    let snapshot = backend.snapshot().expect("snapshot");
    assert_eq!(snapshot.get(&stale).expect("get"), None);
}

#[test]
fn rejects_malformed_metadata() {
    let backend = MemoryBackend::new();
    let catalog = populated_catalog();
    save_catalog(&backend, &catalog).expect("save");
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    let mut key = CATALOG_PREFIX.to_vec();
    key.extend_from_slice(b"generation");
    transaction.put(&key, vec![9, 9, 9]);
    transaction.commit().expect("commit");

    assert!(load_catalog(&backend).is_err());
}
