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
    assert_eq!(second_ts.get(), first_ts.get() + 1);

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
