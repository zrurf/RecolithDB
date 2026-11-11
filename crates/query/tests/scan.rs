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
use recolith_query::{
    AggregateExpression, AggregateFunction, AggregateGroup, AggregateQuery, DocumentScan, Filter,
    FilterOp, KeyFilter, QueryEngine, ScanOrder, ScanPosition,
};
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
            Field::new(FieldId::new(2), "level", Type::U32, true),
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

fn ordered_backend() -> MemoryBackend {
    backend_with(&[
        (1_u64, Some(5_u32)),
        (1, Some(50)),
        (2, Some(10)),
        (3, None),
    ])
}

fn tie_backend() -> MemoryBackend {
    backend_with(&[(1_u64, Some(10_u32)), (2, Some(10)), (3, Some(20))])
}

fn backend_with(records: &[(u64, Option<u32>)]) -> MemoryBackend {
    let backend = MemoryBackend::new();
    let schema = schema();
    let key_schema = key_schema();
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    for (index, (player, level)) in records.iter().copied().enumerate() {
        let document_key = key_schema.encode(&[KeyValue::U64(player)]).expect("key");
        let key = encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            &document_key,
            FragmentId::new(1),
            format!("r{index}").as_bytes(),
        );
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String(format!("player-{player}")));
        if let Some(level) = level {
            data.insert(FieldId::new(2), Value::U32(level));
        }
        write_fields(
            &mut transaction,
            &key,
            &RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(1)),
            &schema,
            &data,
        )
        .expect("write");
        if let Some(level) = level {
            put_index_entry(
                &mut transaction,
                BucketId::new(1),
                PartitionId::new(0),
                IndexId::new(1),
                &encode_index_value(&Value::U32(level)).expect("index value"),
                &key,
            );
        }
    }
    transaction.commit().expect("commit");

    backend
}

fn single_catalog() -> Catalog {
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
                FragmentCardinality::One,
            )],
        ))
        .expect("collection");
    catalog
        .create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(2),
                "by-name",
                IndexKind::Range,
                vec![FieldId::new(1)],
            ),
        )
        .expect("index");

    catalog
}

fn single_backend() -> MemoryBackend {
    let backend = MemoryBackend::new();
    let schema = schema();
    let key_schema = key_schema();
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    let records = [(1_u64, "c", 30_u32), (2, "a", 10), (3, "b", 20)];
    for (player, name, level) in records {
        let document_key = key_schema.encode(&[KeyValue::U64(player)]).expect("key");
        let key = encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            &document_key,
            FragmentId::new(1),
            b"",
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
            IndexId::new(2),
            &encode_index_value(&Value::String(name.to_owned())).expect("index value"),
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

fn document_key(player: u64) -> Vec<u8> {
    key_schema()
        .encode(&[KeyValue::U64(player)])
        .expect("document key")
}

fn aggregate_query() -> AggregateQuery {
    AggregateQuery::new(CollectionId::new(1), PartitionId::new(0), key_schema())
        .with_fragment(FragmentId::new(1))
}

fn level_aggregate(function: AggregateFunction) -> AggregateExpression {
    AggregateExpression::new(
        function,
        Some(FragmentId::new(1)),
        Some(FieldId::new(2)),
        Some(Type::U32),
    )
}

fn count_aggregate() -> AggregateExpression {
    AggregateExpression::new(AggregateFunction::Count, None, None, None)
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

#[test]
fn orders_documents_by_minimum_value_and_places_missing_last() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(ordered_backend()));
    let ascending = scan().with_order(ScanOrder::new(FragmentId::new(1), FieldId::new(2), false));
    let plan = engine.plan_scan(&ascending).expect("plan");
    assert_eq!(plan.order_index(), None);
    let documents = engine.execute_scan(&plan, 10).expect("execute");
    let keys: Vec<_> = documents
        .iter()
        .map(|(key, _)| key_schema().decode(key).expect("key"))
        .collect();
    assert_eq!(
        keys,
        vec![
            vec![KeyValue::U64(1)],
            vec![KeyValue::U64(2)],
            vec![KeyValue::U64(3)],
        ]
    );

    let descending = scan().with_order(ScanOrder::new(FragmentId::new(1), FieldId::new(2), true));
    let plan = engine.plan_scan(&descending).expect("plan");
    let documents = engine.execute_scan(&plan, 10).expect("execute");
    let keys: Vec<_> = documents
        .iter()
        .map(|(key, _)| key_schema().decode(key).expect("key"))
        .collect();
    assert_eq!(
        keys,
        vec![
            vec![KeyValue::U64(2)],
            vec![KeyValue::U64(1)],
            vec![KeyValue::U64(3)],
        ]
    );
}

#[test]
fn breaks_order_ties_in_sort_direction() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(tie_backend()));
    let ascending = scan().with_order(ScanOrder::new(FragmentId::new(1), FieldId::new(2), false));
    let plan = engine.plan_scan(&ascending).expect("plan");
    let documents = engine.execute_scan(&plan, 10).expect("execute");
    let keys: Vec<_> = documents
        .iter()
        .map(|(key, _)| key_schema().decode(key).expect("key"))
        .collect();
    assert_eq!(
        keys,
        vec![
            vec![KeyValue::U64(1)],
            vec![KeyValue::U64(2)],
            vec![KeyValue::U64(3)],
        ]
    );

    let descending = scan().with_order(ScanOrder::new(FragmentId::new(1), FieldId::new(2), true));
    let plan = engine.plan_scan(&descending).expect("plan");
    let documents = engine.execute_scan(&plan, 10).expect("execute");
    let keys: Vec<_> = documents
        .iter()
        .map(|(key, _)| key_schema().decode(key).expect("key"))
        .collect();
    assert_eq!(
        keys,
        vec![
            vec![KeyValue::U64(3)],
            vec![KeyValue::U64(2)],
            vec![KeyValue::U64(1)],
        ]
    );
}

#[test]
fn uses_a_single_field_index_for_ordering() {
    let engine = QueryEngine::new(Arc::new(single_catalog()), Arc::new(single_backend()));
    let query = scan().with_order(ScanOrder::new(FragmentId::new(1), FieldId::new(1), false));
    let plan = engine.plan_scan(&query).expect("plan");
    assert_eq!(plan.order_index(), Some(IndexId::new(2)));
    let documents = engine.execute_scan(&plan, 10).expect("execute");
    let keys: Vec<_> = documents
        .iter()
        .map(|(key, _)| key_schema().decode(key).expect("key"))
        .collect();
    assert_eq!(
        keys,
        vec![
            vec![KeyValue::U64(2)],
            vec![KeyValue::U64(3)],
            vec![KeyValue::U64(1)],
        ]
    );

    let query = scan().with_order(ScanOrder::new(FragmentId::new(1), FieldId::new(1), true));
    let plan = engine.plan_scan(&query).expect("plan");
    let documents = engine.execute_scan(&plan, 10).expect("execute");
    let keys: Vec<_> = documents
        .iter()
        .map(|(key, _)| key_schema().decode(key).expect("key"))
        .collect();
    assert_eq!(
        keys,
        vec![
            vec![KeyValue::U64(1)],
            vec![KeyValue::U64(3)],
            vec![KeyValue::U64(2)],
        ]
    );
}

#[test]
fn scans_one_fragments_through_an_index() {
    let engine = QueryEngine::new(Arc::new(single_catalog()), Arc::new(single_backend()));
    let query = scan().with_filter(Filter::new(
        FragmentId::new(1),
        FieldId::new(1),
        FilterOp::Eq,
        Value::String("b".to_owned()),
    ));
    let plan = engine.plan_scan(&query).expect("plan");
    assert!(plan.uses_index_scan());
    let documents = engine.execute_scan(&plan, 10).expect("execute");

    assert_eq!(documents.len(), 1);
    assert_eq!(
        key_schema().decode(&documents[0].0).expect("key"),
        vec![KeyValue::U64(3)]
    );
}

#[test]
fn paginates_ordered_scans_with_order_values() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(ordered_backend()));
    let query = scan().with_order(ScanOrder::new(FragmentId::new(1), FieldId::new(2), false));
    let plan = engine.plan_scan(&query).expect("plan");

    let first = engine
        .execute_scan_page(&plan, 1, None)
        .expect("first page");
    assert_eq!(first.documents.len(), 1);
    assert!(first.has_more);
    assert_eq!(
        first.last_order_value,
        Some(encode_index_value(&Value::U32(5)).expect("order value"))
    );
    assert_eq!(
        key_schema().decode(&first.documents[0].0).expect("key"),
        vec![KeyValue::U64(1)]
    );

    let second = engine
        .execute_scan_page(
            &plan,
            1,
            Some(ScanPosition {
                document_key: &first.documents[0].0,
                order_value: first.last_order_value.as_deref(),
            }),
        )
        .expect("second page");
    assert_eq!(second.documents.len(), 1);
    assert!(second.has_more);
    assert_eq!(
        key_schema().decode(&second.documents[0].0).expect("key"),
        vec![KeyValue::U64(2)]
    );

    let third = engine
        .execute_scan_page(
            &plan,
            1,
            Some(ScanPosition {
                document_key: &second.documents[0].0,
                order_value: second.last_order_value.as_deref(),
            }),
        )
        .expect("third page");
    assert_eq!(third.documents.len(), 1);
    assert!(!third.has_more);
    assert_eq!(third.last_order_value, None);
    assert_eq!(
        key_schema().decode(&third.documents[0].0).expect("key"),
        vec![KeyValue::U64(3)]
    );
}

#[test]
fn aggregates_documents_globally() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let query = aggregate_query()
        .with_aggregate(count_aggregate())
        .with_aggregate(level_aggregate(AggregateFunction::Sum))
        .with_aggregate(level_aggregate(AggregateFunction::Min))
        .with_aggregate(level_aggregate(AggregateFunction::Max))
        .with_aggregate(level_aggregate(AggregateFunction::Avg));
    let page = engine.execute_aggregate(&query, 10).expect("aggregate");

    assert!(!page.truncated());
    assert_eq!(page.rows().len(), 1);
    let row = &page.rows()[0];
    assert_eq!(row.group(), None);
    assert_eq!(
        row.values(),
        &[
            Some(Value::U64(3)),
            Some(Value::U64(71)),
            Some(Value::U32(10)),
            Some(Value::U32(30)),
            Some(Value::F64(17.75)),
        ]
    );
}

#[test]
fn groups_documents_and_places_missing_keys_last() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(ordered_backend()));
    let query = aggregate_query()
        .with_group(AggregateGroup::new(FragmentId::new(1), FieldId::new(2)))
        .with_aggregate(count_aggregate())
        .with_aggregate(level_aggregate(AggregateFunction::Sum));
    let page = engine.execute_aggregate(&query, 10).expect("aggregate");

    assert!(!page.truncated());
    assert_eq!(page.rows().len(), 3);
    assert_eq!(page.rows()[0].group(), Some(&Value::U32(5)));
    assert_eq!(
        page.rows()[0].values(),
        &[Some(Value::U64(1)), Some(Value::U64(55))]
    );
    assert_eq!(page.rows()[1].group(), Some(&Value::U32(10)));
    assert_eq!(
        page.rows()[1].values(),
        &[Some(Value::U64(1)), Some(Value::U64(10))]
    );
    assert_eq!(page.rows()[2].group(), None);
    assert_eq!(page.rows()[2].values(), &[Some(Value::U64(1)), None]);
}

#[test]
fn limits_and_aggregates_a_single_document() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(ordered_backend()));
    let query = aggregate_query()
        .with_group(AggregateGroup::new(FragmentId::new(1), FieldId::new(2)))
        .with_aggregate(count_aggregate());
    let page = engine.execute_aggregate(&query, 2).expect("aggregate");
    assert_eq!(page.rows().len(), 2);
    assert!(page.truncated());

    let query = aggregate_query()
        .with_document_key(document_key(2))
        .with_aggregate(count_aggregate())
        .with_aggregate(level_aggregate(AggregateFunction::Sum));
    let page = engine
        .execute_aggregate(&query, 10)
        .expect("point aggregate");
    assert_eq!(page.rows().len(), 1);
    assert_eq!(
        page.rows()[0].values(),
        &[Some(Value::U64(1)), Some(Value::U64(10))]
    );
}

#[test]
fn empty_global_aggregate_returns_one_zero_row() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(MemoryBackend::new()));
    let query = aggregate_query()
        .with_aggregate(count_aggregate())
        .with_aggregate(level_aggregate(AggregateFunction::Sum))
        .with_aggregate(level_aggregate(AggregateFunction::Avg));
    let page = engine.execute_aggregate(&query, 10).expect("aggregate");

    assert_eq!(page.rows().len(), 1);
    assert_eq!(page.rows()[0].values(), &[Some(Value::U64(0)), None, None]);
}
