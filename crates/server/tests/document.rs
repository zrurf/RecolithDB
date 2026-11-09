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
use recolith_query::DocumentReadRequest;
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{RecordData, Value, encode_document_key, encode_index_value, scan_index};
use recolith_transaction::TransactionError;

use recolith_server::{DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest};

fn profile_schema() -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "profile",
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, false),
            Field::new(FieldId::new(3), "title", Type::String, true),
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
        .create_index(
            BucketId::new(2),
            recolith_model::IndexDefinition::new(
                recolith_common::IndexId::new(1),
                "by_item",
                recolith_model::IndexKind::Equality,
                vec![FieldId::new(2)],
            ),
        )
        .expect("item index");
    catalog
        .create_index(
            BucketId::new(1),
            recolith_model::IndexDefinition::new(
                recolith_common::IndexId::new(2),
                "by_name",
                recolith_model::IndexKind::Unique,
                vec![FieldId::new(1)],
            ),
        )
        .expect("name index");
    catalog
        .create_index(
            BucketId::new(1),
            recolith_model::IndexDefinition::new(
                recolith_common::IndexId::new(3),
                "by_title",
                recolith_model::IndexKind::Equality,
                vec![FieldId::new(3)],
            ),
        )
        .expect("title index");

    catalog
}

fn store_with_backend() -> (DocumentStore<MemoryBackend>, Arc<MemoryBackend>, Vec<u8>) {
    let key =
        encode_document_key(&key_schema(), &[recolith_model::KeyValue::U64(42)]).expect("key");
    let backend = Arc::new(MemoryBackend::new());

    (
        DocumentStore::new(Arc::new(catalog()), backend.clone()),
        backend,
        key,
    )
}

fn store() -> (DocumentStore<MemoryBackend>, Vec<u8>) {
    let (store, _, key) = store_with_backend();

    (store, key)
}

fn profile() -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("alex".to_owned()));
    data.insert(FieldId::new(2), Value::U32(10));
    data
}

fn item(slot: u32, item_id: u64) -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::U32(slot));
    data.insert(FieldId::new(2), Value::U64(item_id));
    data
}

fn document(key: &[u8]) -> DocumentWrite {
    DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0))
        .with_fragment(FragmentWriteRequest::new(
            FragmentId::new(1),
            Vec::new(),
            profile(),
        ))
        .with_fragment(FragmentWriteRequest::new(
            FragmentId::new(2),
            b"1".to_vec(),
            item(1, 100),
        ))
}

#[test]
fn writes_and_reads_document_fragments() {
    let (store, key) = store();
    let commit_ts = store.write(&document(&key)).expect("write");

    assert_eq!(commit_ts, recolith_common::CommitTs::new(1));
    let read = DocumentReadRequest::new(CollectionId::new(1), key, PartitionId::new(0));
    let fragments = store.read(&read).expect("read");

    assert_eq!(fragments.len(), 2);
    assert_eq!(
        fragments[&FragmentId::new(1)][0]
            .data()
            .get(FieldId::new(1)),
        Some(&Value::String("alex".to_owned()))
    );
    assert_eq!(
        fragments[&FragmentId::new(2)][0]
            .data()
            .get(FieldId::new(2)),
        Some(&Value::U64(100))
    );
    assert_eq!(
        fragments[&FragmentId::new(1)][0].header().commit_ts(),
        commit_ts
    );
    assert_eq!(
        fragments[&FragmentId::new(2)][0].header().commit_ts(),
        commit_ts
    );
}

#[test]
fn rejects_non_empty_subkey_for_one_fragment() {
    let (store, key) = store();
    let write =
        DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0)).with_fragment(
            FragmentWriteRequest::new(FragmentId::new(1), b"x".to_vec(), profile()),
        );

    assert!(matches!(
        store.write(&write),
        Err(DocumentError::InvalidRequest(_))
    ));
}

#[test]
fn rejects_empty_subkey_for_many_fragment() {
    let (store, key) = store();
    let write =
        DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0)).with_fragment(
            FragmentWriteRequest::new(FragmentId::new(2), Vec::new(), item(1, 100)),
        );

    assert!(matches!(
        store.write(&write),
        Err(DocumentError::InvalidRequest(_))
    ));
}

#[test]
fn rejects_duplicate_fragment_targets() {
    let (store, key) = store();
    let write = DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
        .with_fragment(FragmentWriteRequest::new(
            FragmentId::new(2),
            b"1".to_vec(),
            item(1, 100),
        ))
        .with_fragment(FragmentWriteRequest::new(
            FragmentId::new(2),
            b"1".to_vec(),
            item(2, 200),
        ));

    assert!(matches!(
        store.write(&write),
        Err(DocumentError::InvalidRequest(_))
    ));
}

#[test]
fn rejects_partition_out_of_range() {
    let (store, key) = store();
    let write =
        DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(16)).with_fragment(
            FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile()),
        );

    assert!(matches!(
        store.write(&write),
        Err(DocumentError::InvalidRequest(_))
    ));
}

#[test]
fn enforces_optimistic_fragment_version() {
    let (store, key) = store();
    store.write(&document(&key)).expect("first write");
    let write = DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
        .with_fragment(
            FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile())
                .with_if_match(recolith_common::CommitTs::new(0)),
        );

    assert!(matches!(
        store.write(&write),
        Err(DocumentError::Transaction(
            TransactionError::VersionConflict { .. }
        ))
    ));
}

#[test]
fn maintains_indexes_on_document_write() {
    let (store, backend, key) = store_with_backend();
    store.write(&document(&key)).expect("write");

    let snapshot = backend.snapshot().expect("snapshot");
    let item_keys = scan_index(
        &snapshot,
        BucketId::new(2),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::U64(100)).expect("value"),
        ScanOptions::default(),
    )
    .expect("item index");
    let name_keys = scan_index(
        &snapshot,
        BucketId::new(1),
        PartitionId::new(0),
        IndexId::new(2),
        &encode_index_value(&Value::String("alex".to_owned())).expect("value"),
        ScanOptions::default(),
    )
    .expect("name index");

    assert_eq!(item_keys.len(), 1);
    assert_eq!(name_keys.len(), 1);

    let write =
        DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0)).with_fragment(
            FragmentWriteRequest::new(FragmentId::new(2), b"1".to_vec(), item(1, 200)),
        );
    store.write(&write).expect("rewrite");

    let snapshot = backend.snapshot().expect("snapshot");
    let old_keys = scan_index(
        &snapshot,
        BucketId::new(2),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::U64(100)).expect("value"),
        ScanOptions::default(),
    )
    .expect("old index");
    let new_keys = scan_index(
        &snapshot,
        BucketId::new(2),
        PartitionId::new(0),
        IndexId::new(1),
        &encode_index_value(&Value::U64(200)).expect("value"),
        ScanOptions::default(),
    )
    .expect("new index");

    assert!(old_keys.is_empty());
    assert_eq!(new_keys.len(), 1);
}

#[test]
fn removes_sparse_index_entry_when_optional_field_disappears() {
    let (store, backend, key) = store_with_backend();
    let mut data = profile();
    data.insert(FieldId::new(3), Value::String("veteran".to_owned()));
    let write =
        DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0)).with_fragment(
            FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), data),
        );
    store.write(&write).expect("write");

    let snapshot = backend.snapshot().expect("snapshot");
    let entries = scan_index(
        &snapshot,
        BucketId::new(1),
        PartitionId::new(0),
        IndexId::new(3),
        &encode_index_value(&Value::String("veteran".to_owned())).expect("value"),
        ScanOptions::default(),
    )
    .expect("index");
    assert_eq!(entries.len(), 1);

    let write = DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0)).with_fragment(
        FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile()),
    );
    store.write(&write).expect("rewrite");

    let snapshot = backend.snapshot().expect("snapshot");
    let entries = scan_index(
        &snapshot,
        BucketId::new(1),
        PartitionId::new(0),
        IndexId::new(3),
        &encode_index_value(&Value::String("veteran".to_owned())).expect("value"),
        ScanOptions::default(),
    )
    .expect("index");
    assert!(entries.is_empty());
}

#[test]
fn rejects_duplicate_unique_index_value() {
    let (store, _, key) = store_with_backend();
    store.write(&document(&key)).expect("first write");
    let other_key =
        encode_document_key(&key_schema(), &[recolith_model::KeyValue::U64(43)]).expect("key");
    let write =
        DocumentWrite::new(CollectionId::new(1), other_key, PartitionId::new(0)).with_fragment(
            FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile()),
        );

    assert!(matches!(
        store.write(&write),
        Err(DocumentError::Transaction(TransactionError::Storage(
            recolith_storage::StorageError::UniqueViolation { .. }
        )))
    ));
}
