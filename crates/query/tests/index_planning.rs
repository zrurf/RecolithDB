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
        .expect("level index");
    catalog
        .create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(2),
                "by_name",
                IndexKind::Range,
                vec![FieldId::new(1)],
            ),
        )
        .expect("name index");

    catalog
}

fn backend() -> MemoryBackend {
    backend_with(&[
        (b"s1".as_slice(), "sword", 10),
        (b"s2".as_slice(), "sword", 20),
    ])
}

fn backend_with(items: &[(&[u8], &str, u32)]) -> MemoryBackend {
    let backend = MemoryBackend::new();
    let schema = schema();
    let mut path = LocalWritePath::begin(
        &backend,
        PersistentCommitClock::with_default_key(),
        TxnOptions::default(),
    )
    .expect("begin");

    for (subkey, name, level) in items {
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String((*name).to_owned()));
        data.insert(FieldId::new(2), Value::U32(*level));
        let indexes = [
            IndexUpdate::new(
                IndexId::new(1),
                None,
                encode_index_value(&Value::U32(*level)).expect("level index value"),
                false,
            ),
            IndexUpdate::new(
                IndexId::new(2),
                None,
                encode_index_value(&Value::String((*name).to_owned())).expect("name index value"),
                false,
            ),
        ];
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
            &indexes,
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

#[test]
fn plans_range_index_for_many_fragment() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));

    for (op, value, expected) in [
        (FilterOp::Gt, 10, vec![b"s2".as_slice()]),
        (FilterOp::Gt, 20, Vec::new()),
        (FilterOp::Ge, 20, vec![b"s2".as_slice()]),
        (FilterOp::Lt, 20, vec![b"s1".as_slice()]),
        (FilterOp::Le, 20, vec![b"s1".as_slice(), b"s2".as_slice()]),
    ] {
        let query = DocumentQuery::new(CollectionId::new(1), b"alex", PartitionId::new(0))
            .with_fragment(FragmentId::new(1))
            .with_filter(Filter::new(
                FragmentId::new(1),
                FieldId::new(2),
                op,
                Value::U32(value),
            ));
        let plan = engine.plan(&query).expect("plan");
        let filter = plan.readers()[0].index_filter().expect("index filter");
        assert_eq!(filter.index(), IndexId::new(1));
        assert!(filter.bounds().is_some());

        let fragments = engine.execute(&plan).expect("execute");
        let subkeys = fragments
            .get(&FragmentId::new(1))
            .map(|records| {
                records
                    .iter()
                    .map(|record| record.subkey().to_vec())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        assert_eq!(subkeys, expected, "op {op:?} value {value}");
    }
}

#[test]
fn range_index_handles_duplicates_and_strings() {
    let engine = QueryEngine::new(
        Arc::new(catalog()),
        Arc::new(backend_with(&[
            (b"s1".as_slice(), "alice", 10),
            (b"s2".as_slice(), "bob", 20),
            (b"s3".as_slice(), "bob", 20),
            (b"s4".as_slice(), "carol", 30),
        ])),
    );

    for (field, op, value, expected) in [
        (
            FieldId::new(2),
            FilterOp::Gt,
            Value::U32(10),
            vec![b"s2".as_slice(), b"s3".as_slice(), b"s4".as_slice()],
        ),
        (
            FieldId::new(2),
            FilterOp::Gt,
            Value::U32(20),
            vec![b"s4".as_slice()],
        ),
        (
            FieldId::new(2),
            FilterOp::Lt,
            Value::U32(20),
            vec![b"s1".as_slice()],
        ),
        (
            FieldId::new(1),
            FilterOp::Ge,
            Value::String("bob".to_owned()),
            vec![b"s2".as_slice(), b"s3".as_slice(), b"s4".as_slice()],
        ),
        (
            FieldId::new(1),
            FilterOp::Le,
            Value::String("bob".to_owned()),
            vec![b"s1".as_slice(), b"s2".as_slice(), b"s3".as_slice()],
        ),
    ] {
        let query = DocumentQuery::new(CollectionId::new(1), b"alex", PartitionId::new(0))
            .with_fragment(FragmentId::new(1))
            .with_filter(Filter::new(FragmentId::new(1), field, op, value));
        let plan = engine.plan(&query).expect("plan");
        assert!(plan.readers()[0].index_filter().is_some());
        let fragments = engine.execute(&plan).expect("execute");
        let subkeys = fragments
            .get(&FragmentId::new(1))
            .map(|records| {
                records
                    .iter()
                    .map(|record| record.subkey().to_vec())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        assert_eq!(subkeys, expected, "field {field} op {op:?}");
    }
}

fn composite_catalog() -> Catalog {
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
                "by_level_name",
                IndexKind::Composite,
                vec![FieldId::new(2), FieldId::new(1)],
            ),
        )
        .expect("composite index");

    catalog
}

fn composite_backend(items: &[(&[u8], &str, u32)]) -> MemoryBackend {
    let backend = MemoryBackend::new();
    let schema = schema();
    let mut path = LocalWritePath::begin(
        &backend,
        PersistentCommitClock::with_default_key(),
        TxnOptions::default(),
    )
    .expect("begin");
    for (subkey, name, level) in items {
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String((*name).to_owned()));
        data.insert(FieldId::new(2), Value::U32(*level));
        let mut encoded = encode_index_value(&Value::U32(*level)).expect("level value");
        encoded.extend_from_slice(
            &encode_index_value(&Value::String((*name).to_owned())).expect("name value"),
        );
        let index = IndexUpdate::new(IndexId::new(1), None, encoded, false);
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
fn composite_index_pushes_leading_field_only() {
    let engine = QueryEngine::new(
        Arc::new(composite_catalog()),
        Arc::new(composite_backend(&[
            (b"s1".as_slice(), "alice", 10),
            (b"s2".as_slice(), "bob", 20),
            (b"s3".as_slice(), "carol", 20),
            (b"s4".as_slice(), "dave", 30),
        ])),
    );

    for (op, value, expected) in [
        (FilterOp::Eq, 20, vec![b"s2".as_slice(), b"s3".as_slice()]),
        (
            FilterOp::Gt,
            10,
            vec![b"s2".as_slice(), b"s3".as_slice(), b"s4".as_slice()],
        ),
        (
            FilterOp::Le,
            20,
            vec![b"s1".as_slice(), b"s2".as_slice(), b"s3".as_slice()],
        ),
    ] {
        let query = DocumentQuery::new(CollectionId::new(1), b"alex", PartitionId::new(0))
            .with_fragment(FragmentId::new(1))
            .with_filter(Filter::new(
                FragmentId::new(1),
                FieldId::new(2),
                op,
                Value::U32(value),
            ));
        let plan = engine.plan(&query).expect("plan");
        assert!(plan.readers()[0].index_filter().is_some(), "op {op:?}");
        let fragments = engine.execute(&plan).expect("execute");
        let subkeys: Vec<Vec<u8>> = fragments
            .get(&FragmentId::new(1))
            .map(|records| {
                records
                    .iter()
                    .map(|record| record.subkey().to_vec())
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(subkeys, expected, "op {op:?} value {value}");
    }

    let non_leading = DocumentQuery::new(CollectionId::new(1), b"alex", PartitionId::new(0))
        .with_fragment(FragmentId::new(1))
        .with_filter(Filter::new(
            FragmentId::new(1),
            FieldId::new(1),
            FilterOp::Eq,
            Value::String("bob".to_owned()),
        ));
    let plan = engine.plan(&non_leading).expect("plan");
    assert!(plan.readers()[0].index_filter().is_none());
    let fragments = engine.execute(&plan).expect("execute");
    assert_eq!(fragments[&FragmentId::new(1)].len(), 1);
    assert_eq!(fragments[&FragmentId::new(1)][0].subkey(), b"s2");
}

#[test]
fn intersects_multiple_index_filters() {
    let engine = QueryEngine::new(
        Arc::new(catalog()),
        Arc::new(backend_with(&[
            (b"s1".as_slice(), "alice", 10),
            (b"s2".as_slice(), "bob", 20),
            (b"s3".as_slice(), "bob", 20),
            (b"s4".as_slice(), "carol", 30),
        ])),
    );
    let query = DocumentQuery::new(CollectionId::new(1), b"alex", PartitionId::new(0))
        .with_fragment(FragmentId::new(1))
        .with_filter(Filter::new(
            FragmentId::new(1),
            FieldId::new(2),
            FilterOp::Ge,
            Value::U32(20),
        ))
        .with_filter(Filter::new(
            FragmentId::new(1),
            FieldId::new(1),
            FilterOp::Eq,
            Value::String("bob".to_owned()),
        ));

    let plan = engine.plan(&query).expect("plan");
    assert_eq!(plan.readers()[0].index_filters().len(), 2);
    let fragments = engine.execute(&plan).expect("execute");
    let records = &fragments[&FragmentId::new(1)];
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].subkey(), b"s2");
    assert_eq!(records[1].subkey(), b"s3");
}
