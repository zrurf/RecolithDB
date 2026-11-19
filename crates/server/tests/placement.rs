use std::sync::Arc;

use recolith_backend_memory::MemoryBackend;
use recolith_catalog::{BucketPlacement, Catalog};
use recolith_common::{
    BackendId, BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, PartitionId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, KeyValue, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{RecordData, Value, encode_document_key};

use recolith_server::{DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest};

fn catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");
    catalog
        .register_schema(RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "profile",
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
                Field::new(FieldId::new(3), "gold", Type::U64, false),
            ],
        ))
        .expect("schema");
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
                "player_events",
                16,
                BucketPolicy::Managed,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("events bucket");
    catalog
        .create_collection(Collection::new(
            CollectionId::new(1),
            KeyspaceId::new(1),
            "player",
            "player",
            DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
            vec![
                Fragment::new(
                    FragmentId::new(1),
                    "profile",
                    BucketId::new(1),
                    FragmentCardinality::One,
                ),
                Fragment::new(
                    FragmentId::new(2),
                    "events",
                    BucketId::new(2),
                    FragmentCardinality::Many,
                ),
            ],
        ))
        .expect("collection");

    catalog
}

fn place_bucket(catalog: &Catalog, bucket_id: u64, backend_id: u64) {
    let partition_count = catalog
        .snapshot()
        .expect("snapshot")
        .buckets
        .get(&BucketId::new(bucket_id))
        .expect("bucket")
        .partition_count();
    catalog
        .place_bucket(BucketPlacement::new(
            BucketId::new(bucket_id),
            BackendId::new(backend_id),
            "us-east",
            partition_count,
        ))
        .expect("place bucket");
}

fn key(player: u64) -> Vec<u8> {
    encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[KeyValue::U64(player)],
    )
    .expect("key")
}

fn profile_fragment(name: &str) -> FragmentWriteRequest {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String(name.to_owned()));
    data.insert(FieldId::new(2), Value::U32(10));
    data.insert(FieldId::new(3), Value::U64(0));
    FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), data)
}

fn events_fragment() -> FragmentWriteRequest {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("event".to_owned()));
    data.insert(FieldId::new(2), Value::U32(1));
    data.insert(FieldId::new(3), Value::U64(0));
    FragmentWriteRequest::new(FragmentId::new(2), b"e1".to_vec(), data)
}

#[test]
fn placed_bucket_rejects_foreign_document_writes() {
    let catalog = catalog();
    place_bucket(&catalog, 1, 7);
    let store = DocumentStore::new(Arc::new(catalog), Arc::new(MemoryBackend::new()));

    let write = DocumentWrite::new(CollectionId::new(1), key(1), PartitionId::new(0))
        .with_fragment(profile_fragment("alex"));
    assert!(matches!(
        store.write(&write),
        Err(DocumentError::PlacementRejected { bucket, backend, local })
            if bucket == BucketId::new(1)
                && backend == BackendId::new(7)
                && local == BackendId::new(0)
    ));

    // Unplaced buckets stay writable.
    let write = DocumentWrite::new(CollectionId::new(1), key(2), PartitionId::new(0))
        .with_fragment(events_fragment());
    store.write(&write).expect("unplaced write");

    // A write spanning placed and unplaced buckets is rejected as a whole.
    let write = DocumentWrite::new(CollectionId::new(1), key(3), PartitionId::new(0))
        .with_fragment(profile_fragment("bob"))
        .with_fragment(events_fragment());
    assert!(matches!(
        store.write(&write),
        Err(DocumentError::PlacementRejected { .. })
    ));

    assert!(matches!(
        store.delete_document(CollectionId::new(1), PartitionId::new(0), &key(1)),
        Err(DocumentError::PlacementRejected { .. })
    ));
}

#[test]
fn placed_bucket_rejects_foreign_counters_and_bucket_writes() {
    let seed_catalog = catalog();
    place_bucket(&seed_catalog, 1, 7);
    place_bucket(&seed_catalog, 2, 7);
    let backend = Arc::new(MemoryBackend::new());
    let writer = DocumentStore::new(Arc::new(seed_catalog), backend.clone())
        .with_local_backend_id(BackendId::new(7));

    let enforce_catalog = catalog();
    place_bucket(&enforce_catalog, 1, 7);
    place_bucket(&enforce_catalog, 2, 7);
    let store = DocumentStore::new(Arc::new(enforce_catalog), backend.clone());

    let stored = DocumentWrite::new(CollectionId::new(1), key(1), PartitionId::new(0))
        .with_fragment(profile_fragment("alex"));
    writer.write(&stored).expect("seed");

    assert!(matches!(
        store.increment_field(
            CollectionId::new(1),
            PartitionId::new(0),
            &key(1),
            FragmentId::new(1),
            FieldId::new(3),
            5,
            None,
        ),
        Err(DocumentError::PlacementRejected { .. })
    ));
    assert!(matches!(
        store.bucket_put(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(2),
            b"k",
            b"s",
            &RecordData::new(),
            None,
        ),
        Err(DocumentError::PlacementRejected { .. })
    ));
    assert!(matches!(
        store.bucket_delete(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(2),
            b"k",
            b"s"
        ),
        Err(DocumentError::PlacementRejected { .. })
    ));
}

#[test]
fn matching_local_backend_allows_writes() {
    let catalog = catalog();
    place_bucket(&catalog, 1, 7);
    place_bucket(&catalog, 2, 7);
    let store = DocumentStore::new(Arc::new(catalog), Arc::new(MemoryBackend::new()))
        .with_local_backend_id(BackendId::new(7));

    let write = DocumentWrite::new(CollectionId::new(1), key(1), PartitionId::new(0))
        .with_fragment(profile_fragment("alex"))
        .with_fragment(events_fragment());
    store.write(&write).expect("local write");

    store
        .increment_field(
            CollectionId::new(1),
            PartitionId::new(0),
            &key(1),
            FragmentId::new(1),
            FieldId::new(3),
            5,
            None,
        )
        .expect("local increment");

    let mut record = RecordData::new();
    record.insert(FieldId::new(1), Value::String("event".to_owned()));
    record.insert(FieldId::new(2), Value::U32(1));
    record.insert(FieldId::new(3), Value::U64(0));
    store
        .bucket_put(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(2),
            b"k",
            b"s",
            &record,
            None,
        )
        .expect("local bucket put");
    store
        .bucket_delete(
            BucketId::new(2),
            PartitionId::new(0),
            FragmentId::new(2),
            b"k",
            b"s",
        )
        .expect("local bucket delete");
}

#[test]
fn unplaced_buckets_need_no_backend_match() {
    let catalog = catalog();
    let store = DocumentStore::new(Arc::new(catalog), Arc::new(MemoryBackend::new()))
        .with_local_backend_id(BackendId::new(42));

    let write = DocumentWrite::new(CollectionId::new(1), key(1), PartitionId::new(0))
        .with_fragment(profile_fragment("alex"));
    store.write(&write).expect("unplaced write");
    store
        .delete_document(CollectionId::new(1), PartitionId::new(0), &key(1))
        .expect("unplaced delete");
}

#[test]
fn expiry_reaper_deletes_placed_documents() {
    let placed = catalog();
    let partition_count = placed
        .snapshot()
        .expect("snapshot")
        .buckets
        .get(&BucketId::new(1))
        .expect("bucket")
        .partition_count();
    placed
        .place_bucket(recolith_catalog::BucketPlacement::new(
            BucketId::new(1),
            BackendId::new(7),
            "us-east",
            partition_count,
        ))
        .expect("place bucket");
    let backend = Arc::new(MemoryBackend::new());
    let store = DocumentStore::new(Arc::new(placed), backend.clone());

    // Document writes are placement-checked even with an expiry.
    let expired_write = DocumentWrite::new(CollectionId::new(1), key(1), PartitionId::new(0))
        .with_fragment(profile_fragment("temp"))
        .with_ttl_seconds(Some(1));
    assert!(matches!(
        store.write(&expired_write),
        Err(DocumentError::PlacementRejected { .. })
    ));

    // A document written by the leader before the placement still gets
    // reaped: internal maintenance is exempt from the placement check.
    let pre_write = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros() as u64;
    let leader =
        DocumentStore::new(Arc::new(catalog()), backend).with_local_backend_id(BackendId::new(7));
    let write = DocumentWrite::new(CollectionId::new(1), key(1), PartitionId::new(0))
        .with_fragment(profile_fragment("temp"))
        .with_ttl_seconds(Some(1));
    leader.write(&write).expect("leader write");

    let sweep = store
        .sweep_expired(pre_write + 2_000_000, 100)
        .expect("sweep");
    assert_eq!(sweep.deleted, 1);
}
