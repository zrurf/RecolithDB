use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use recolith_backend::{Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_backend_memory::MemoryBackend;
use recolith_common::{CollectionId, FieldId, FragmentId, PartitionId};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_query::{AggregateExpression, AggregateFunction, AggregateQuery, DocumentScan};
use recolith_server::{DocumentStore, DocumentWrite, FragmentWriteRequest};
use recolith_storage::{
    RecordData, Value, decode_ttl_value, encode_document_key, encode_ttl_index_key, encode_ttl_key,
    encode_ttl_value,
};

fn key_schema() -> DocumentKeySchema {
    DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
}

fn profile() -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("alex".to_owned()));
    data.insert(FieldId::new(2), Value::U32(10));
    data
}

fn store() -> (DocumentStore<MemoryBackend>, Arc<MemoryBackend>, Vec<u8>) {
    let backend = Arc::new(MemoryBackend::new());
    let key = encode_document_key(&key_schema(), &[KeyValue::U64(42)]).expect("key");

    (
        DocumentStore::new(Arc::new(recolith_testkit::game_catalog()), backend.clone()),
        backend,
        key,
    )
}

fn document(key: &[u8], ttl_seconds: Option<u64>) -> DocumentWrite {
    DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0))
        .with_fragment(FragmentWriteRequest::new(
            FragmentId::new(1),
            Vec::new(),
            profile(),
        ))
        .with_ttl_seconds(ttl_seconds)
}

fn metadata_key(key: &[u8]) -> Vec<u8> {
    encode_ttl_key(CollectionId::new(1), PartitionId::new(0), key)
}

fn index_key(expires_at: u64, key: &[u8]) -> Vec<u8> {
    encode_ttl_index_key(expires_at, CollectionId::new(1), PartitionId::new(0), key)
}

fn expiry(backend: &MemoryBackend, key: &[u8]) -> Option<u64> {
    backend
        .snapshot()
        .expect("snapshot")
        .get(&metadata_key(key))
        .expect("ttl")
        .and_then(|value| decode_ttl_value(&value))
}

fn set_expiry(backend: &MemoryBackend, key: &[u8], expires_at: u64) {
    let mut transaction = backend.begin(TxnOptions::default()).expect("begin");
    transaction.put(&metadata_key(key), encode_ttl_value(expires_at).to_vec());
    transaction.put(&index_key(expires_at, key), Vec::new());
    transaction.commit().expect("commit");
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
        })
}

#[test]
fn writes_preserves_and_clears_document_expiry() {
    let (store, backend, key) = store();
    store
        .write(&document(&key, Some(3_600)))
        .expect("write with ttl");
    let first = expiry(&backend, &key).expect("expiry");
    assert!(first > now_micros());

    store
        .write(&document(&key, None))
        .expect("partial write without ttl");
    assert_eq!(expiry(&backend, &key), Some(first));

    store.write(&document(&key, Some(0))).expect("clear ttl");
    assert_eq!(expiry(&backend, &key), None);
    assert!(
        backend
            .snapshot()
            .expect("snapshot")
            .get(&index_key(first, &key))
            .expect("index")
            .is_none()
    );
}

#[test]
fn filters_expired_documents_from_reads_queries_and_aggregates() {
    let (store, backend, key) = store();
    store.write(&document(&key, None)).expect("write");
    let expired_at = now_micros().saturating_sub(1_000);
    set_expiry(&backend, &key, expired_at);

    let read = recolith_query::DocumentReadRequest::new(
        CollectionId::new(1),
        key.clone(),
        PartitionId::new(0),
    );
    assert!(store.read(&read).expect("read").is_empty());

    let engine = store.query_engine();
    let scan = DocumentScan::new(CollectionId::new(1), PartitionId::new(0), key_schema())
        .with_fragment(FragmentId::new(1));
    let plan = engine.plan_scan(&scan).expect("plan");
    assert!(engine.execute_scan(&plan, 10).expect("scan").is_empty());

    let aggregate = AggregateQuery::new(CollectionId::new(1), PartitionId::new(0), key_schema())
        .with_fragment(FragmentId::new(1))
        .with_aggregate(AggregateExpression::new(
            AggregateFunction::Count,
            None,
            None,
            None,
        ));
    let page = engine.execute_aggregate(&aggregate, 10).expect("aggregate");
    assert_eq!(page.rows()[0].values(), &[Some(Value::U64(0))]);
}

#[test]
fn reaps_expired_documents_and_cleans_stale_index_entries() {
    let (store, backend, key) = store();
    store.write(&document(&key, None)).expect("write");
    let expired_at = now_micros().saturating_sub(1_000);
    set_expiry(&backend, &key, expired_at);

    let sweep = store.sweep_expired(now_micros(), 100).expect("sweep");
    assert_eq!(sweep.scanned, 1);
    assert_eq!(sweep.deleted, 1);
    assert_eq!(expiry(&backend, &key), None);
    assert!(
        backend
            .snapshot()
            .expect("snapshot")
            .get(&index_key(expired_at, &key))
            .expect("index")
            .is_none()
    );
}

#[test]
fn conditional_reaping_ignores_changed_or_future_expiry() {
    let (store, backend, key) = store();
    store
        .write(&document(&key, Some(3_600)))
        .expect("write with ttl");
    let expires_at = expiry(&backend, &key).expect("expiry");

    assert!(
        !store
            .delete_expired_document(
                CollectionId::new(1),
                PartitionId::new(0),
                &key,
                expires_at.saturating_sub(1),
                expires_at.saturating_add(1),
            )
            .expect("stale expiry")
    );
    assert!(expiry(&backend, &key).is_some());

    assert!(
        store
            .delete_expired_document(
                CollectionId::new(1),
                PartitionId::new(0),
                &key,
                expires_at,
                expires_at.saturating_add(1),
            )
            .expect("expired")
    );
    assert_eq!(expiry(&backend, &key), None);
}

#[test]
fn explicit_deletion_removes_expiry_metadata() {
    let (store, backend, key) = store();
    store
        .write(&document(&key, Some(3_600)))
        .expect("write with ttl");
    let expires_at = expiry(&backend, &key).expect("expiry");

    assert!(
        store
            .delete_document(CollectionId::new(1), PartitionId::new(0), &key)
            .expect("delete")
    );
    assert_eq!(expiry(&backend, &key), None);
    assert!(
        backend
            .snapshot()
            .expect("snapshot")
            .get(&index_key(expires_at, &key))
            .expect("index")
            .is_none()
    );
}
