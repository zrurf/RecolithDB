use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use recolith_backend_fjall::FjallBackend;
use recolith_catalog::{Catalog, load_catalog, save_catalog};
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, PartitionId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, KeyValue, Keyspace,
};
use recolith_query::DocumentReadRequest;
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{DocumentStore, DocumentWrite, FragmentWriteRequest};
use recolith_storage::{RecordData, Value, encode_document_key};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "recolith-server-{label}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn key_schema() -> DocumentKeySchema {
    DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
}

fn profile_schema() -> RecordSchema {
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
    catalog.register_schema(profile_schema()).expect("schema");
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
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(2),
                "scratch",
                8,
                BucketPolicy::Raw,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("raw bucket");
    catalog
        .create_collection(Collection::new(
            CollectionId::new(1),
            KeyspaceId::new(1),
            "player",
            "player",
            key_schema(),
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

fn document(key: Vec<u8>, name: &str) -> DocumentWrite {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String(name.to_owned()));
    data.insert(FieldId::new(2), Value::U32(10));

    DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0)).with_fragment(
        FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), data),
    )
}

fn document_key(player: u64) -> Vec<u8> {
    encode_document_key(&key_schema(), &[KeyValue::U64(player)]).expect("key")
}

#[test]
fn survives_restart_with_catalog_clock_and_document() {
    let dir = TestDir::new("restart");
    let first_key = document_key(42);
    let second_key = document_key(43);
    let generation;
    let first_ts;

    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("open"));
        let catalog = catalog();
        save_catalog(backend.as_ref(), &catalog).expect("save catalog");
        generation = catalog.generation();
        let store = DocumentStore::new(Arc::new(catalog), backend.clone());
        first_ts = store
            .write(&document(first_key.clone(), "alex"))
            .expect("write");
        backend.persist().expect("persist");
    }

    let loaded_generation;
    let second_ts;
    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("reopen"));
        let catalog = load_catalog(backend.as_ref()).expect("load catalog");
        loaded_generation = catalog.generation();
        let store = DocumentStore::new(Arc::new(catalog), backend.clone());

        let request =
            DocumentReadRequest::new(CollectionId::new(1), first_key.clone(), PartitionId::new(0));
        let fragments = store.read(&request).expect("read");
        let record = &fragments[&FragmentId::new(1)][0];
        assert_eq!(
            record.data().get(FieldId::new(1)),
            Some(&Value::String("alex".to_owned()))
        );
        assert_eq!(record.header().commit_ts(), first_ts);

        second_ts = store
            .write(&document(second_key.clone(), "sam"))
            .expect("second write");
        backend.persist().expect("persist");
    }

    assert_eq!(loaded_generation, generation);
    assert!(second_ts > first_ts);

    let backend = Arc::new(FjallBackend::open(dir.path()).expect("reopen again"));
    let catalog = load_catalog(backend.as_ref()).expect("load catalog again");
    let store = DocumentStore::new(Arc::new(catalog), Arc::clone(&backend));
    let request = DocumentReadRequest::new(CollectionId::new(1), second_key, PartitionId::new(0));
    let fragments = store.read(&request).expect("read second");
    let record = &fragments[&FragmentId::new(1)][0];

    assert_eq!(
        record.data().get(FieldId::new(1)),
        Some(&Value::String("sam".to_owned()))
    );
    assert_eq!(record.header().commit_ts(), second_ts);
}

#[test]
fn async_workflow_resumes_across_restart() {
    use recolith_protocol::messages::{
        FragmentPayload, WorkflowDefinition, WorkflowPutDocument, WorkflowState, WorkflowStep,
        WorkflowStepStatus, value, workflow_step,
    };

    let dir = TestDir::new("workflow-restart");
    let key = document_key(42);

    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("open"));
        let catalog = catalog();
        save_catalog(backend.as_ref(), &catalog).expect("save catalog");
        let store = DocumentStore::new(Arc::new(catalog), backend.clone());
        let record = store
            .submit_workflow(
                "async-fjall",
                &WorkflowDefinition {
                    steps: vec![WorkflowStep {
                        kind: Some(workflow_step::Kind::PutDocument(WorkflowPutDocument {
                            collection_id: 1,
                            partition: 0,
                            document_key: key.clone(),
                            fragments: vec![FragmentPayload {
                                fragment_id: 1,
                                subkey: Vec::new(),
                                fields: vec![
                                    recolith_protocol::messages::FieldValue {
                                        field_id: 1,
                                        value: Some(recolith_protocol::messages::Value {
                                            kind: Some(value::Kind::StringValue("alex".to_owned())),
                                        }),
                                    },
                                    recolith_protocol::messages::FieldValue {
                                        field_id: 2,
                                        value: Some(recolith_protocol::messages::Value {
                                            kind: Some(value::Kind::U32Value(10)),
                                        }),
                                    },
                                ],
                                if_match: None,
                            }],
                            ttl_seconds: None,
                        })),
                    }],
                },
            )
            .expect("submit");
        assert_eq!(record.state(), WorkflowState::Pending);
        backend.persist().expect("persist");
    }

    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("reopen"));
        let catalog = load_catalog(backend.as_ref()).expect("load catalog");
        let store = DocumentStore::new(Arc::new(catalog), backend.clone());

        let record = store.workflow_record("async-fjall").expect("record");
        assert_eq!(
            record.expect("record exists").state(),
            WorkflowState::Pending
        );
        assert_eq!(store.resume_workflows(100).expect("resume"), 1);
        let record = store
            .workflow_record("async-fjall")
            .expect("record")
            .expect("record exists");
        assert_eq!(record.state(), WorkflowState::Completed);
        assert_eq!(record.steps[0].status(), WorkflowStepStatus::Succeeded);

        let request = DocumentReadRequest::new(CollectionId::new(1), key, PartitionId::new(0));
        let fragments = store.read(&request).expect("read");
        assert_eq!(
            fragments[&FragmentId::new(1)][0]
                .data()
                .get(FieldId::new(1)),
            Some(&Value::String("alex".to_owned()))
        );
        backend.persist().expect("persist");
    }
}

#[test]
fn placement_enforcement_persists_across_restart() {
    use recolith_catalog::BucketPlacement;
    use recolith_common::BackendId;
    use recolith_server::DocumentError;

    let dir = TestDir::new("placement-restart");
    let key = document_key(42);

    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("open"));
        let catalog = catalog();
        let partition_count = catalog
            .snapshot()
            .expect("snapshot")
            .buckets
            .get(&BucketId::new(1))
            .expect("bucket")
            .partition_count();
        catalog
            .place_bucket(BucketPlacement::new(
                BucketId::new(1),
                BackendId::new(7),
                "us-east",
                partition_count,
            ))
            .expect("place bucket");
        save_catalog(backend.as_ref(), &catalog).expect("save catalog");
        let store = DocumentStore::new(Arc::new(catalog), backend.clone())
            .with_local_backend_id(BackendId::new(7));
        store
            .write(&document(key.clone(), "alex"))
            .expect("leader write");
        backend.persist().expect("persist");
    }

    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("reopen"));
        let catalog = load_catalog(backend.as_ref()).expect("load catalog");
        let snapshot = catalog.snapshot().expect("snapshot");
        let store = DocumentStore::new(
            Arc::new(Catalog::restore(snapshot).expect("restore")),
            Arc::clone(&backend),
        );
        assert!(matches!(
            store.write(&document(key.clone(), "bob")),
            Err(DocumentError::PlacementRejected { .. })
        ));

        let local = DocumentStore::new(Arc::new(catalog), backend.clone())
            .with_local_backend_id(BackendId::new(7));
        local.write(&document(key, "carl")).expect("leader write");
        backend.persist().expect("persist");
    }
}

#[test]
fn bucket_records_survive_restart() {
    use recolith_server::DocumentError;

    let dir = TestDir::new("bucket-restart");
    let commit_ts;

    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("open"));
        let catalog = catalog();
        save_catalog(backend.as_ref(), &catalog).expect("save catalog");
        let store = DocumentStore::new(Arc::new(catalog), backend.clone());
        let mut data = RecordData::new();
        data.insert(FieldId::new(1), Value::String("event".to_owned()));
        data.insert(FieldId::new(2), Value::U32(1));
        commit_ts = store
            .bucket_put(
                BucketId::new(2),
                PartitionId::new(1),
                FragmentId::new(9),
                b"k1",
                b"",
                &data,
                None,
            )
            .expect("bucket put");
        backend.persist().expect("persist");
    }

    {
        let backend = Arc::new(FjallBackend::open(dir.path()).expect("reopen"));
        let catalog = load_catalog(backend.as_ref()).expect("load catalog");
        let store = DocumentStore::new(Arc::new(catalog), Arc::clone(&backend));

        let record = store
            .bucket_get(
                BucketId::new(2),
                PartitionId::new(1),
                FragmentId::new(9),
                b"k1",
                b"",
            )
            .expect("bucket get")
            .expect("record exists");
        assert_eq!(record.commit_ts(), commit_ts);
        assert_eq!(record.schema_version(), SchemaVersion::new(1));

        let scan = store
            .bucket_scan(BucketId::new(2), PartitionId::new(1), None, None, 10)
            .expect("bucket scan");
        assert_eq!(scan.len(), 1);

        assert!(
            store
                .bucket_delete(
                    BucketId::new(2),
                    PartitionId::new(1),
                    FragmentId::new(9),
                    b"k1",
                    b""
                )
                .expect("bucket delete")
        );

        let missing = store.bucket_get(
            BucketId::new(99),
            PartitionId::new(0),
            FragmentId::new(9),
            b"k",
            b"",
        );
        assert!(matches!(
            missing,
            Err(DocumentError::MissingMetadata("bucket"))
        ));
        backend.persist().expect("persist");
    }
}
