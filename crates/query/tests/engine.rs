use std::sync::Arc;

use recolith_backend::{StorageBackend, Transaction, TxnOptions};
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, CommitTs, FieldId, FragmentId, KeyspaceId, PartitionId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{RecordData, RecordHeader, Value, encode_record_key, write_fields};

use recolith_query::QueryEngine;
use recolith_query::ir::{DocumentQuery, FieldSelection, Filter, FilterOp};
use recolith_query::request::DocumentReadRequest;

fn schema() -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "profile",
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
}

fn backend() -> MemoryBackend {
    let backend = MemoryBackend::new();
    let schema = schema();
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("alex".to_owned()));
    data.insert(FieldId::new(2), Value::U32(10));
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    write_fields(
        &mut transaction,
        &encode_record_key(
            BucketId::new(1),
            PartitionId::new(0),
            b"alex",
            FragmentId::new(1),
            b"",
        ),
        &RecordHeader::new(SchemaId::new(1), SchemaVersion::new(1), CommitTs::new(1)),
        &schema,
        &data,
    )
    .expect("write");
    transaction.commit().expect("commit");

    backend
}

#[test]
fn binds_catalog_and_reads_document_fragment() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let request =
        DocumentReadRequest::new(CollectionId::new(1), b"alex".to_vec(), PartitionId::new(0))
            .with_fragment(FragmentId::new(1));
    let fragments = engine.read_document(&request).expect("read");

    assert_eq!(fragments.len(), 1);
    assert_eq!(
        fragments[&FragmentId::new(1)][0]
            .data()
            .get(FieldId::new(2)),
        Some(&Value::U32(10))
    );
}

#[test]
fn applies_filter_and_projection() {
    let engine = QueryEngine::new(Arc::new(catalog()), Arc::new(backend()));
    let query = DocumentQuery::new(CollectionId::new(1), b"alex".to_vec(), PartitionId::new(0))
        .with_fragment(FragmentId::new(1))
        .with_filter(Filter::new(
            FragmentId::new(1),
            FieldId::new(2),
            FilterOp::Eq,
            Value::U32(10),
        ))
        .with_projection(FieldSelection::new(FragmentId::new(1), FieldId::new(1)));

    let plan = engine.plan(&query).expect("plan");
    let fragments = engine.execute(&plan).expect("execute");
    let data = fragments[&FragmentId::new(1)][0].data();

    assert_eq!(
        data.get(FieldId::new(1)),
        Some(&Value::String("alex".to_owned()))
    );
    assert_eq!(data.get(FieldId::new(2)), None);
}
