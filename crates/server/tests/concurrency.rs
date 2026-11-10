use std::collections::HashSet;
use std::sync::Arc;

use recolith_backend_memory::MemoryBackend;
use recolith_common::{CollectionId, CommitTs, FieldId, FragmentId, PartitionId};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_query::DocumentReadRequest;
use recolith_server::{DocumentStore, DocumentWrite, FragmentWriteRequest, Idempotency};
use recolith_storage::{RecordData, Value};

#[test]
fn commits_distinct_documents_from_multiple_threads() {
    let store = Arc::new(DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    ));
    let key_schema = DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]);

    let timestamps = std::thread::scope(|scope| {
        let handles = (0..8_u64)
            .map(|worker| {
                let store = Arc::clone(&store);
                let key_schema = key_schema.clone();
                scope.spawn(move || {
                    let mut timestamps = Vec::with_capacity(100);
                    for offset in 0..100_u64 {
                        let id = worker * 100 + offset;
                        let key = key_schema.encode(&[KeyValue::U64(id)]).expect("key");
                        let timestamp = store
                            .write(&document(key, "alex").with_idempotency(Idempotency::new(
                                format!("idem-{id}").into_bytes(),
                                [0; 32],
                            )))
                            .unwrap_or_else(|error| panic!("write {id} failed: {error}"));
                        timestamps.push(timestamp.get());
                    }
                    timestamps
                })
            })
            .collect::<Vec<_>>();

        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("thread"))
            .collect::<Vec<_>>()
    });

    assert_eq!(
        timestamps.iter().copied().collect::<HashSet<_>>().len(),
        800
    );
    assert_eq!(timestamps.iter().copied().min(), Some(1));
    assert_eq!(timestamps.iter().copied().max(), Some(800));

    for id in [0, 399, 799] {
        let key = key_schema.encode(&[KeyValue::U64(id)]).expect("key");
        let fragments = store
            .read(
                &DocumentReadRequest::new(CollectionId::new(1), key, PartitionId::new(0))
                    .with_fragment(FragmentId::new(1)),
            )
            .expect("read");
        assert_eq!(
            fragments[&FragmentId::new(1)][0]
                .data()
                .get(FieldId::new(1)),
            Some(&Value::String("alex".to_owned()))
        );
    }
}

#[test]
fn failed_optimistic_write_wastes_only_its_timestamp() {
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    );
    let key = key_schema().encode(&[KeyValue::U64(1)]).expect("key");

    let first = store.write(&document(key.clone(), "alex")).expect("first");

    let mut profile = RecordData::new();
    profile.insert(FieldId::new(1), Value::String("sam".to_owned()));
    profile.insert(FieldId::new(2), Value::U32(10));
    let conflict = store.write(
        &DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0)).with_fragment(
            FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile)
                .with_if_match(CommitTs::new(0)),
        ),
    );
    assert!(conflict.is_err());

    let second = store.write(&document(key.clone(), "sam")).expect("second");

    assert_eq!(second.get(), first.get() + 2);
}

#[test]
fn concurrent_if_match_writers_have_single_winner() {
    let store = Arc::new(DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    ));
    let key = key_schema().encode(&[KeyValue::U64(1)]).expect("key");
    let first = store.write(&document(key.clone(), "alex")).expect("first");

    let results = std::thread::scope(|scope| {
        let handles = ["sam", "kim"]
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                let store = Arc::clone(&store);
                let key = key.clone();
                scope.spawn(move || {
                    let mut profile = RecordData::new();
                    profile.insert(FieldId::new(1), Value::String(name.to_owned()));
                    profile.insert(FieldId::new(2), Value::U32(10));
                    store.write(
                        &DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0))
                            .with_fragment(
                                FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile)
                                    .with_if_match(first),
                            )
                            .with_idempotency(Idempotency::new(
                                format!("if-match-{index}").into_bytes(),
                                [0; 32],
                            )),
                    )
                })
            })
            .collect::<Vec<_>>();

        handles
            .into_iter()
            .map(|handle| handle.join().expect("thread"))
            .collect::<Vec<_>>()
    });

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
}

fn document(key: Vec<u8>, name: &str) -> DocumentWrite {
    let mut profile = RecordData::new();
    profile.insert(FieldId::new(1), Value::String(name.to_owned()));
    profile.insert(FieldId::new(2), Value::U32(10));

    DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0)).with_fragment(
        FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile),
    )
}

fn key_schema() -> DocumentKeySchema {
    DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
}
