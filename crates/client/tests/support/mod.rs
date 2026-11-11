#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, KeyValue, Keyspace,
};
use recolith_protocol::messages::{
    DocumentPutRequest, FieldValue, FragmentPayload, RequestMeta, Value, value,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{DocumentStore, ServerConfig, serve};
use recolith_storage::encode_document_key;
use tokio::net::TcpListener;

pub fn key_schema() -> DocumentKeySchema {
    DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
}

pub fn game_store() -> Arc<DocumentStore<MemoryBackend>> {
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

    Arc::new(DocumentStore::new(
        Arc::new(catalog),
        Arc::new(MemoryBackend::new()),
    ))
}

pub async fn start_server(
    config: ServerConfig,
    store: Arc<DocumentStore<MemoryBackend>>,
) -> SocketAddr {
    let listener = TcpListener::bind(config.listen).await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        let _ = serve(listener, config, store).await;
    });

    address
}

pub fn document_key(player_id: u64) -> Vec<u8> {
    encode_document_key(&key_schema(), &[KeyValue::U64(player_id)]).expect("key")
}

pub fn profile_put(key: &[u8], name: &str, idempotency: &[u8]) -> DocumentPutRequest {
    DocumentPutRequest {
        meta: Some(RequestMeta {
            idempotency_key: idempotency.to_vec(),
            ..Default::default()
        }),
        collection_id: 1,
        partition: 0,
        document_key: key.to_vec(),
        fragments: vec![FragmentPayload {
            fragment_id: 1,
            subkey: Vec::new(),
            fields: vec![
                FieldValue {
                    field_id: 1,
                    value: Some(Value {
                        kind: Some(value::Kind::StringValue(name.to_owned())),
                    }),
                },
                FieldValue {
                    field_id: 2,
                    value: Some(Value {
                        kind: Some(value::Kind::U32Value(10)),
                    }),
                },
            ],
            if_match: None,
        }],
        ttl_seconds: None,
    }
}
