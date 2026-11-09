use std::sync::Arc;

use recolith_backend::TxnOptions;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, PartitionId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_query::{DocumentQuery, Filter, FilterOp, QueryEngine};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{RecordData, Value, encode_index_value, encode_record_key};
use recolith_transaction::{IndexUpdate, LocalWritePath, PersistentCommitClock};

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
                "player_items",
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
                "by_level",
                IndexKind::Equality,
                vec![FieldId::new(2)],
            ),
        )
        .expect("index");

    catalog
}

fn backend() -> MemoryBackend {
    let backend = MemoryBackend::new();
    let schema = schema();
    let mut path = LocalWritePath::begin(
        &backend,
        PersistentCommitClock::with_default_key(),
        TxnOptions::default(),
    )
    .expect("begin");

    for (subkey, level) in [(b"s1".as_slice(), 10), (b"s2".as_slice(), 20)] {
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("sword".to_owned()));
        data.insert(FieldId::new(2), Value::U32(level));
        let index = IndexUpdate::new(
            IndexId::new(1),
            None,
            encode_index_value(&Value::U32(level)).expect("index value"),
            false,
        );
        path.write_record_with_indexes(
            &encode_record_key(
                BucketId::new(1),
                PartitionId::new(0),
                b"alex",
                FragmentId::new(1),
                subkey,
            ),
            &schema,
            &data,
            &[index],
        )
        .expect("record");
    }
    path.commit().expect("commit");

    backend
}

#[test]
fn plans_index_for_many_fragment_equality() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let query = DocumentQuery::new(CollectionId::new(1), b"alex", PartitionId::new(0))
        .with_fragment(FragmentId::new(1))
        .with_filter(Filter::new(
            FragmentId::new(1),
            FieldId::new(2),
            FilterOp::Eq,
            Value::U32(20),
        ));

    let plan = engine.plan(&query).expect("plan");
    let reader = &plan.readers()[0];
    assert_eq!(
        reader.index_filter().expect("index filter").index(),
        IndexId::new(1)
    );

    let fragments = engine.execute(&plan).expect("execute");
    let records = &fragments[&FragmentId::new(1)];
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].subkey(), b"s2");
    assert_eq!(
        records[0].data().get(FieldId::new(2)),
        Some(&Value::U32(20))
    );
}

#[test]
fn keeps_full_scan_for_inequality_filters() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let query = DocumentQuery::new(CollectionId::new(1), b"alex", PartitionId::new(0))
        .with_fragment(FragmentId::new(1))
        .with_filter(Filter::new(
            FragmentId::new(1),
            FieldId::new(2),
            FilterOp::Ne,
            Value::U32(20),
        ));

    let plan = engine.plan(&query).expect("plan");

    assert!(plan.readers()[0].index_filter().is_none());
    let fragments = engine.execute(&plan).expect("execute");
    assert_eq!(fragments[&FragmentId::new(1)][0].subkey(), b"s1");
}
