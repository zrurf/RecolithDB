use std::sync::Arc;

use recolith_backend::{StorageBackend, Transaction, TxnOptions};
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, CommitTs, FieldId, FragmentId, IndexId, KeyspaceId, PartitionId,
    SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, KeyValue, Keyspace,
};
use recolith_query::{DocumentScan, Filter, FilterOp, KeyFilter, QueryEngine};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{
    RecordData, RecordHeader, Value, encode_index_value, encode_record_key, put_index_entry,
    write_fields,
};

fn key_schema() -> DocumentKeySchema {
    DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
}

fn schema() -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "item",
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, false),
        ],
    )
}

fn catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");
    catalog.register_schema(schema()).expect("schema");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(1),
                "items",
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
            key_schema(),
            vec![Fragment::new(
                FragmentId::new(1),
                "items",
                BucketId::new(1),
                FragmentCardinality::Many,
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

fn record(name: &str, level: u32) -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String(name.to_owned()));
    data.insert(FieldId::new(2), Value::U32(level));
    data
}

fn backend() -> MemoryBackend {
    let backend = MemoryBackend::new();
    let schema = schema();
    let key_schema = key_schema();
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    let records = [
        (1_u64, "a", "alex", 10_u32),
        (1, "b", "alex-alt", 11),
        (2, "a", "sam", 20),
        (3, "a", "kim", 30),
    ];
    for (player, subkey, name, level) in records {
        let document_key = key_schema.encode(&[KeyValue::U64(player)]).expect("key");
        let key = encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            &document_key,
            FragmentId::new(1),
            subkey.as_bytes(),
        );
        write_fields(
            &mut transaction,
            &key,
            &RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(1)),
            &schema,
            &record(name, level),
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

fn scan() -> DocumentScan {
    DocumentScan::new(CollectionId::new(1), PartitionId::new(0), key_schema())
        .with_fragment(FragmentId::new(1))
}

#[test]
fn scans_bucket_and_returns_multiple_documents() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let plan = engine.plan_scan(&scan()).expect("plan");
    let documents = engine.execute_scan(&plan, 10).expect("execute");

    assert_eq!(documents.len(), 3);
    let keys: Vec<_> = documents
        .iter()
        .map(|(key, _)| key_schema().decode(key).expect("key"))
        .collect();
    assert_eq!(keys[0], vec![KeyValue::U64(1)]);
    assert_eq!(keys[2], vec![KeyValue::U64(3)]);
}

#[test]
fn applies_index_filters_key_ranges_and_limits() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let query = scan()
        .with_filter(Filter::new(
            FragmentId::new(1),
            FieldId::new(2),
            FilterOp::Ge,
            Value::U32(20),
        ))
        .with_key_filter(KeyFilter::new(0, FilterOp::Ge, KeyValue::U64(2)));
    let plan = engine.plan_scan(&query).expect("plan");
    let documents = engine.execute_scan(&plan, 10).expect("execute");

    assert_eq!(documents.len(), 2);
    assert_eq!(
        key_schema().decode(&documents[0].0).expect("key"),
        vec![KeyValue::U64(2)]
    );

    let documents = engine.execute_scan(&plan, 1).expect("limited");
    assert_eq!(documents.len(), 1);
}

#[test]
fn excludes_documents_without_matching_fragment_records() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let query = scan().with_filter(Filter::new(
        FragmentId::new(1),
        FieldId::new(1),
        FilterOp::Eq,
        Value::String("sam".to_owned()),
    ));
    let plan = engine.plan_scan(&query).expect("plan");
    let documents = engine.execute_scan(&plan, 10).expect("execute");

    assert_eq!(documents.len(), 1);
    assert_eq!(
        key_schema().decode(&documents[0].0).expect("key"),
        vec![KeyValue::U64(2)]
    );
}
