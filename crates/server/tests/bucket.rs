use std::sync::Arc;

use recolith_backend::{ScanOptions, StorageBackend};
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, PartitionId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{RecordData, Value, decode_record_key, encode_index_value, scan_index};
use recolith_transaction::TransactionError;

use recolith_server::{DocumentError, DocumentStore};

fn record_schema() -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "record",
        vec![
            Field::new(FieldId::new(1), "id", Type::U64, false),
            Field::new(FieldId::new(2), "name", Type::String, false),
            Field::new(FieldId::new(3), "score", Type::U32, true),
        ],
    )
}

fn catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");
    catalog.register_schema(record_schema()).expect("schema");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(1),
                "raw_bucket",
                4,
                BucketPolicy::Raw,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("raw bucket");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(2),
                "managed_bucket",
                4,
                BucketPolicy::Managed,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("managed bucket");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(3),
                "strict_bucket",
                4,
                BucketPolicy::Strict,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("strict bucket");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(4),
                "managed_events",
                4,
                BucketPolicy::Managed,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("managed events bucket");
    catalog
        .create_collection(Collection::new(
            CollectionId::new(1),
            KeyspaceId::new(1),
            "players",
            "player",
            DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
            vec![
                Fragment::new(
                    FragmentId::new(1),
                    "core",
                    BucketId::new(2),
                    FragmentCardinality::One,
                ),
                Fragment::new(
                    FragmentId::new(2),
                    "events",
                    BucketId::new(4),
                    FragmentCardinality::Many,
                ),
            ],
        ))
        .expect("collection");
    catalog
        .create_index(
            BucketId::new(2),
            recolith_model::IndexDefinition::new(
                IndexId::new(1),
                "by_name",
                recolith_model::IndexKind::Equality,
                vec![FieldId::new(2)],
            ),
        )
        .expect("name index");

    catalog
}

fn store() -> (DocumentStore<MemoryBackend>, Arc<MemoryBackend>) {
    let backend = Arc::new(MemoryBackend::new());
    let store = DocumentStore::new(Arc::new(catalog()), backend.clone());

    (store, backend)
}

fn full_record(id: u64, name: &str) -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::U64(id));
    data.insert(FieldId::new(2), Value::String(name.to_owned()));
    data.insert(FieldId::new(3), Value::U32(10));
    data
}

fn partial_record(id: u64) -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::U64(id));
    data
}

#[test]
fn strict_bucket_rejects_direct_writes_but_allows_reads() {
    let (store, _backend) = store();
    let put = store.bucket_put(
        BucketId::new(3),
        PartitionId::new(0),
        FragmentId::new(9),
        b"k",
        b"",
        &full_record(1, "alex"),
        None,
    );
    assert!(matches!(
        put,
        Err(DocumentError::InvalidRequest(message)) if message.contains("strict")
    ));
    let delete = store.bucket_delete(
        BucketId::new(3),
        PartitionId::new(0),
        FragmentId::new(9),
        b"k",
        b"",
    );
    assert!(matches!(
        delete,
        Err(DocumentError::InvalidRequest(message)) if message.contains("strict")
    ));
    assert!(
        store
            .bucket_get(
                BucketId::new(3),
                PartitionId::new(0),
                FragmentId::new(9),
                b"k",
                b""
            )
            .expect("strict read")
            .is_none()
    );
    assert!(
        store
            .bucket_scan(BucketId::new(3), PartitionId::new(0), None, None, 10)
            .expect("strict scan")
            .is_empty()
    );
}

#[test]
fn raw_bucket_round_trip_skips_required_validation() {
    let (store, _backend) = store();
    let commit_ts = store
        .bucket_put(
            BucketId::new(1),
            PartitionId::new(0),
            FragmentId::new(9),
            b"k",
            b"",
            &partial_record(1),
            None,
        )
        .expect("raw put");
    assert!(commit_ts.get() > 0);

    let record = store
        .bucket_get(
            BucketId::new(1),
            PartitionId::new(0),
            FragmentId::new(9),
            b"k",
            b"",
        )
        .expect("raw get")
        .expect("record exists");
    assert_eq!(record.commit_ts(), commit_ts);
    assert_eq!(record.schema_id(), SchemaId::new(1));
    assert_eq!(record.schema_version(), SchemaVersion::new(1));
    assert_eq!(record.data().get(FieldId::new(1)), Some(&Value::U64(1)));

    assert!(
        store
            .bucket_delete(
                BucketId::new(1),
                PartitionId::new(0),
                FragmentId::new(9),
                b"k",
                b""
            )
            .expect("raw delete")
    );
    assert!(
        !store
            .bucket_delete(
                BucketId::new(1),
                PartitionId::new(0),
                FragmentId::new(9),
                b"k",
                b""
            )
            .expect("raw delete again")
    );
}

#[test]
fn raw_bucket_accepts_any_fragment_and_subkey() {
    let (store, _backend) = store();
    store
        .bucket_put(
            BucketId::new(1),
            PartitionId::new(0),
            FragmentId::new(7),
            b"k",
            b"sub",
            &partial_record(1),
            None,
        )
        .expect("raw put with subkey");
    assert!(
        store
            .bucket_get(
                BucketId::new(1),
                PartitionId::new(0),
                FragmentId::new(7),
                b"k",
                b"sub"
            )
            .expect("raw get")
            .is_some()
    );
}

#[test]
fn managed_bucket_requires_schema_fields() {
    let (store, _backend) = store();
    let put = store.bucket_put(
        BucketId::new(2),
        PartitionId::new(0),
        FragmentId::new(1),
        b"k",
        b"",
        &partial_record(1),
        None,
    );
    assert!(matches!(
        put,
        Err(DocumentError::Transaction(TransactionError::Storage(
            recolith_storage::StorageError::MissingField(_)
        )))
    ));
    store
        .bucket_put(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(1),
            b"k",
            b"",
            &full_record(1, "alex"),
            None,
        )
        .expect("managed put");
}

#[test]
fn managed_bucket_maintains_indexes() {
    let (store, backend) = store();
    let commit_ts = store
        .bucket_put(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(1),
            b"k",
            b"",
            &full_record(1, "alex"),
            None,
        )
        .expect("managed put");

    let snapshot = backend.snapshot().expect("snapshot");
    let keys = scan_index(
        &snapshot,
        BucketId::new(2),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::String("alex".to_owned())).expect("value"),
        ScanOptions::default(),
    )
    .expect("index scan");
    assert_eq!(keys.len(), 1);

    let mut renamed = full_record(1, "blake");
    renamed.insert(FieldId::new(2), Value::String("blake".to_owned()));
    store
        .bucket_put(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(1),
            b"k",
            b"",
            &renamed,
            Some(commit_ts),
        )
        .expect("managed rewrite");

    let snapshot = backend.snapshot().expect("snapshot");
    let old_keys = scan_index(
        &snapshot,
        BucketId::new(2),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::String("alex".to_owned())).expect("value"),
        ScanOptions::default(),
    )
    .expect("old index scan");
    let new_keys = scan_index(
        &snapshot,
        BucketId::new(2),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::String("blake".to_owned())).expect("value"),
        ScanOptions::default(),
    )
    .expect("new index scan");
    assert!(old_keys.is_empty());
    assert_eq!(new_keys.len(), 1);

    assert!(
        store
            .bucket_delete(
                BucketId::new(2),
                PartitionId::new(0),
                FragmentId::new(1),
                b"k",
                b""
            )
            .expect("managed delete")
    );
    let snapshot = backend.snapshot().expect("snapshot");
    let keys = scan_index(
        &snapshot,
        BucketId::new(2),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::String("blake".to_owned())).expect("value"),
        ScanOptions::default(),
    )
    .expect("post-delete index scan");
    assert!(keys.is_empty());
}

#[test]
fn managed_collection_owned_buckets_enforce_cardinality() {
    let (store, _backend) = store();
    let one_with_subkey = store.bucket_put(
        BucketId::new(2),
        PartitionId::new(0),
        FragmentId::new(1),
        b"k",
        b"sub",
        &full_record(1, "alex"),
        None,
    );
    assert!(matches!(
        one_with_subkey,
        Err(DocumentError::InvalidRequest(message)) if message.contains("one fragment")
    ));
    let many_without_subkey = store.bucket_put(
        BucketId::new(4),
        PartitionId::new(0),
        FragmentId::new(2),
        b"k",
        b"",
        &full_record(1, "alex"),
        None,
    );
    assert!(matches!(
        many_without_subkey,
        Err(DocumentError::InvalidRequest(message)) if message.contains("many fragment")
    ));
    let unknown_fragment = store.bucket_put(
        BucketId::new(2),
        PartitionId::new(0),
        FragmentId::new(5),
        b"k",
        b"",
        &full_record(1, "alex"),
        None,
    );
    assert!(matches!(
        unknown_fragment,
        Err(DocumentError::UnknownFragment(_))
    ));
}

#[test]
fn bucket_put_enforces_optimistic_if_match() {
    let (store, _backend) = store();
    let first = store
        .bucket_put(
            BucketId::new(1),
            PartitionId::new(0),
            FragmentId::new(9),
            b"k",
            b"",
            &partial_record(1),
            None,
        )
        .expect("first put");
    let second = store
        .bucket_put(
            BucketId::new(1),
            PartitionId::new(0),
            FragmentId::new(9),
            b"k",
            b"",
            &partial_record(2),
            Some(first),
        )
        .expect("matching put");
    assert!(second > first);
    let stale = store.bucket_put(
        BucketId::new(1),
        PartitionId::new(0),
        FragmentId::new(9),
        b"k",
        b"",
        &partial_record(3),
        Some(first),
    );
    assert!(matches!(
        stale,
        Err(DocumentError::Transaction(
            TransactionError::VersionConflict { .. }
        ))
    ));
}

#[test]
fn bucket_scan_paginates_and_filters_by_fragment() {
    let (store, _backend) = store();
    for index in 0..5_u64 {
        let key = format!("k{index}");
        let fragment = if index % 2 == 0 { 9 } else { 10 };
        store
            .bucket_put(
                BucketId::new(1),
                PartitionId::new(1),
                FragmentId::new(fragment),
                key.as_bytes(),
                b"",
                &partial_record(index),
                None,
            )
            .expect("put");
    }

    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = store
            .bucket_scan(
                BucketId::new(1),
                PartitionId::new(1),
                None,
                cursor.as_deref(),
                2,
            )
            .expect("scan page");
        assert!(page.len() <= 2);
        if page.is_empty() {
            break;
        }
        for (key, record) in &page {
            let decoded = decode_record_key(key).expect("record key");
            let document_key = String::from_utf8(decoded.document_key().to_vec()).expect("key");
            assert!(document_key.starts_with('k'));
            assert!(record.data().get(FieldId::new(1)).is_some());
            seen.push(document_key);
        }
        cursor = Some(page.last().expect("non-empty page").0.clone());
    }
    assert_eq!(seen.len(), 5);
    assert_eq!(seen.first().expect("first"), "k0");

    let odd_only = store
        .bucket_scan(
            BucketId::new(1),
            PartitionId::new(1),
            Some(FragmentId::new(10)),
            None,
            10,
        )
        .expect("fragment scan");
    assert_eq!(odd_only.len(), 2);

    let empty = store
        .bucket_scan(BucketId::new(1), PartitionId::new(2), None, None, 10)
        .expect("empty partition scan");
    assert!(empty.is_empty());
}

#[test]
fn bucket_reads_reject_out_of_range_partitions() {
    let (store, _backend) = store();
    let get = store.bucket_get(
        BucketId::new(1),
        PartitionId::new(4),
        FragmentId::new(9),
        b"k",
        b"",
    );
    assert!(matches!(
        get,
        Err(DocumentError::InvalidRequest(message)) if message.contains("outside bucket")
    ));
    let scan = store.bucket_scan(BucketId::new(1), PartitionId::new(4), None, None, 10);
    assert!(matches!(
        scan,
        Err(DocumentError::InvalidRequest(message)) if message.contains("outside bucket")
    ));
    let unknown = store.bucket_get(
        BucketId::new(99),
        PartitionId::new(0),
        FragmentId::new(9),
        b"k",
        b"",
    );
    assert!(matches!(
        unknown,
        Err(DocumentError::MissingMetadata("bucket"))
    ));
}

#[test]
fn bucket_scan_limit_zero_returns_empty() {
    let (store, _backend) = store();
    let page = store
        .bucket_scan(BucketId::new(1), PartitionId::new(0), None, None, 0)
        .expect("scan");
    assert!(page.is_empty());
}

#[test]
fn bucket_put_rejects_invalid_document_keys() {
    let (store, _backend) = store();
    let empty = store.bucket_put(
        BucketId::new(1),
        PartitionId::new(0),
        FragmentId::new(9),
        b"",
        b"",
        &partial_record(1),
        None,
    );
    assert!(matches!(
        empty,
        Err(DocumentError::InvalidRequest(message)) if message.contains("must not be empty")
    ));

    let oversized = vec![b'x'; recolith_storage::MAX_DOCUMENT_KEY_LEN + 1];
    let too_long = store.bucket_put(
        BucketId::new(1),
        PartitionId::new(0),
        FragmentId::new(9),
        &oversized,
        b"",
        &partial_record(1),
        None,
    );
    assert!(matches!(
        too_long,
        Err(DocumentError::InvalidRequest(message)) if message.contains("limit")
    ));
}
