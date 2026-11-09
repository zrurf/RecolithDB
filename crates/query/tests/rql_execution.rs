use std::sync::Arc;

use recolith_backend::TxnOptions;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, PartitionId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, Keyspace,
};
use recolith_query::{
    BindParameters, QueryEngine, RqlLiteral, bind_document_query, parse_document_query,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{RecordData, Value, encode_document_key, encode_record_key};
use recolith_transaction::{LocalWritePath, PersistentCommitClock};

fn profile_schema() -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "profile",
        vec![
            Field::new(FieldId::new(1), "player_id", Type::U64, false),
            Field::new(FieldId::new(2), "name", Type::String, false),
            Field::new(FieldId::new(3), "level", Type::U32, false),
        ],
    )
}

fn item_schema() -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(2),
        SchemaVersion::new(1),
        "item",
        vec![
            Field::new(FieldId::new(1), "slot", Type::U32, false),
            Field::new(FieldId::new(2), "item_id", Type::U64, false),
        ],
    )
}

fn key_schema() -> DocumentKeySchema {
    DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
}

fn catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");
    catalog.register_schema(profile_schema()).expect("profile");
    catalog.register_schema(item_schema()).expect("item");
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
        .expect("profile bucket");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(2),
                "player_items",
                16,
                BucketPolicy::Strict,
                SchemaId::new(2),
                SchemaVersion::new(1),
            ),
        )
        .expect("item bucket");
    catalog
        .create_collection(Collection::new(
            CollectionId::new(1),
            KeyspaceId::new(1),
            "player",
            "player",
            key_schema(),
            vec![
                Fragment::new(
                    FragmentId::new(1),
                    "profile",
                    BucketId::new(1),
                    FragmentCardinality::One,
                ),
                Fragment::new(
                    FragmentId::new(2),
                    "items",
                    BucketId::new(2),
                    FragmentCardinality::Many,
                ),
            ],
        ))
        .expect("collection");

    catalog
}

fn backend() -> (MemoryBackend, Vec<u8>) {
    let backend = MemoryBackend::new();
    let document_key =
        encode_document_key(&key_schema(), &[recolith_model::KeyValue::U64(42)]).expect("key");

    let mut profile = RecordData::new();
    profile.insert(FieldId::new(1), Value::U64(42));
    profile.insert(FieldId::new(2), Value::String("alex".to_owned()));
    profile.insert(FieldId::new(3), Value::U32(12));

    let mut path = LocalWritePath::begin(
        &backend,
        PersistentCommitClock::with_default_key(),
        TxnOptions::default(),
    )
    .expect("begin");
    path.write_record(
        &encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            &document_key,
            FragmentId::new(1),
            b"",
        ),
        &profile_schema(),
        &profile,
    )
    .expect("profile");
    for (slot, item_id) in [(1, 100), (2, 200)] {
        let mut item = RecordData::new();
        item.insert(FieldId::new(1), Value::U32(slot));
        item.insert(FieldId::new(2), Value::U64(item_id));
        path.write_record(
            &encode_record_key(
                BucketId::new(2),
                PartitionId::new(0),
                &document_key,
                FragmentId::new(2),
                slot.to_string().as_bytes(),
            ),
            &item_schema(),
            &item,
        )
        .expect("item");
    }
    path.commit().expect("commit");

    (backend, document_key)
}

#[test]
fn executes_bound_rql_end_to_end() {
    let catalog = catalog();
    let (backend, document_key) = backend();
    let snapshot = catalog.snapshot().expect("snapshot");
    let engine = QueryEngine::new(Arc::new(catalog), Arc::new(backend));
    let parsed = parse_document_query(
        "SELECT profile.name, items.item_id
         FROM player
         WHERE player_id = :id AND items.slot = 1
         INCLUDE FRAGMENTS profile, items",
    )
    .expect("parse");
    let mut parameters = BindParameters::new();
    parameters.insert("id", RqlLiteral::Integer(42));

    let query =
        bind_document_query(&snapshot, &parsed, PartitionId::new(0), &parameters).expect("bind");
    assert_eq!(query.document_key(), document_key);
    let fragments = engine
        .execute(&engine.plan(&query).expect("plan"))
        .expect("execute");

    let profile = &fragments[&FragmentId::new(1)][0].data();
    assert_eq!(
        profile.get(FieldId::new(2)),
        Some(&Value::String("alex".to_owned()))
    );
    assert_eq!(profile.get(FieldId::new(3)), None);

    let items = &fragments[&FragmentId::new(2)];
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].data().get(FieldId::new(2)), Some(&Value::U64(100)));
    assert_eq!(items[0].data().get(FieldId::new(1)), None);
}
