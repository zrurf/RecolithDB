use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use recolith_backend_fjall::FjallBackend;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::{Catalog, CatalogChanges, load_catalog};
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy as ModelBucketPolicy, Collection, DocumentKeySchema, Fragment,
    FragmentCardinality, IndexDefinition as ModelIndexDefinition, IndexKind as ModelIndexKind,
    KeyField, KeyType, KeyValue, Keyspace,
};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN};
use recolith_protocol::messages::{
    AuthMethod, AuthResult, BucketDefinition, BucketPolicy, CatalogApplyRequest,
    CatalogApplyResponse, CatalogDropRequest, CatalogDropResponse, CatalogIndexTarget,
    CollectionDefinition, DescribeCollectionRequest, DescribeCollectionResponse,
    DocumentGetRequest, DocumentGetResponse, DocumentPutRequest, ErrorResponse, FieldValue,
    FragmentDefinition, FragmentPayload, HelloRequest, IndexDefinition, IndexKind,
    KeyFieldDefinition, KeyFieldType, KeyspaceDefinition, QueryRequest, QueryResponse, RequestMeta,
    SchemaDefinition, Value, WelcomeResponse, catalog_drop_request, describe_collection_request,
    opcode, status, value,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{DocumentStore, ServerConfig, serve};
use recolith_storage::encode_document_key;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const IDL: &str = r#"
record Profile {
    name: string @1;
    level: u32 @2;
}
"#;

fn store() -> Arc<DocumentStore<MemoryBackend>> {
    Arc::new(DocumentStore::new(
        Arc::new(Catalog::new()),
        Arc::new(MemoryBackend::new()),
    ))
}

async fn start_server(store: Arc<DocumentStore<MemoryBackend>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let mut config = ServerConfig::new(address);
    config.listen = address;
    tokio::spawn(serve(listener, config, store));

    address
}

fn hello() -> HelloRequest {
    HelloRequest {
        protocol_min_major: 1,
        protocol_min_minor: 0,
        protocol_max_major: 1,
        protocol_max_minor: 0,
        client_id: "catalog-test".to_owned(),
        client_version: "0.1.0".to_owned(),
        auth_method: AuthMethod::None as i32,
        auth_token: Vec::new(),
        features: 0,
        max_frame_size: 16 * 1024 * 1024,
        max_batch_subframes: 64,
        max_batch_bytes: 1024 * 1024,
        compression_algorithms: 0,
        compression_threshold: 0,
        encodings: 1,
    }
}

async fn connect(address: SocketAddr) -> (TcpStream, FrameCodec) {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let plain = FrameCodec::plain().expect("plain");
    write_frame(
        &mut stream,
        &plain,
        &FrameHeader::new(opcode::HELLO, 1),
        &hello().encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &plain).await;
    assert_eq!(header.opcode(), opcode::WELCOME);
    let welcome = WelcomeResponse::decode(payload.as_slice()).expect("welcome");
    assert_eq!(welcome.auth_result, AuthResult::Ok as i32);
    let codec = FrameCodec::new(
        CompressionAlgorithm::None,
        0,
        welcome.max_frame_size as usize,
        welcome.max_decompressed_size as usize,
    )
    .expect("codec");

    (stream, codec)
}

async fn write_frame(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    header: &FrameHeader,
    payload: &[u8],
) {
    let frame = codec.encode(header, payload).expect("encode");
    stream.write_all(&frame).await.expect("write");
}

async fn read_frame(stream: &mut TcpStream, codec: &FrameCodec) -> (FrameHeader, Vec<u8>) {
    let mut header = [0_u8; HEADER_LEN];
    stream.read_exact(&mut header).await.expect("header");
    let header = FrameHeader::decode(&header).expect("header");
    let mut payload = vec![0_u8; header.payload_len() as usize];
    stream.read_exact(&mut payload).await.expect("payload");
    let payload = codec.decode(&header, &payload).expect("payload");

    (header, payload)
}

fn base_request() -> CatalogApplyRequest {
    CatalogApplyRequest {
        meta: Some(RequestMeta::default()),
        keyspaces: vec![KeyspaceDefinition {
            id: 1,
            name: "game".to_owned(),
        }],
        schemas: vec![SchemaDefinition {
            id: 1,
            version: 1,
            idl: IDL.to_owned(),
        }],
        buckets: vec![BucketDefinition {
            id: 1,
            keyspace_id: 1,
            name: "player_profile".to_owned(),
            partitions: 16,
            policy: BucketPolicy::Strict as i32,
            schema_id: 1,
            schema_version: 1,
        }],
        collections: vec![CollectionDefinition {
            id: 1,
            keyspace_id: 1,
            name: "player".to_owned(),
            affinity_group: "player".to_owned(),
            key_fields: vec![KeyFieldDefinition {
                name: "player_id".to_owned(),
                r#type: KeyFieldType::U64 as i32,
            }],
            fragments: vec![FragmentDefinition {
                id: 1,
                name: "profile".to_owned(),
                bucket_id: 1,
                cardinality: recolith_protocol::messages::FragmentCardinality::One as i32,
            }],
        }],
        indexes: Vec::new(),
    }
}

fn index_request() -> CatalogApplyRequest {
    CatalogApplyRequest {
        meta: Some(RequestMeta::default()),
        indexes: vec![IndexDefinition {
            id: 1,
            bucket_id: 1,
            name: "by-level".to_owned(),
            kind: IndexKind::Range as i32,
            fields: vec![2],
        }],
        ..Default::default()
    }
}

async fn apply(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    request_id: u64,
    request: &CatalogApplyRequest,
) -> (FrameHeader, CatalogApplyResponse) {
    write_frame(
        stream,
        codec,
        &FrameHeader::new(opcode::CATALOG_APPLY, request_id),
        &request.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(stream, codec).await;
    let response = if header.status() == status::OK {
        CatalogApplyResponse::decode(payload.as_slice()).expect("apply response")
    } else {
        CatalogApplyResponse::default()
    };

    (header, response)
}

fn drop_target(target: catalog_drop_request::Target, purge: bool) -> CatalogDropRequest {
    CatalogDropRequest {
        meta: Some(RequestMeta::default()),
        purge,
        target: Some(target),
    }
}

async fn drop_catalog(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    request_id: u64,
    request: &CatalogDropRequest,
) -> (FrameHeader, CatalogDropResponse) {
    write_frame(
        stream,
        codec,
        &FrameHeader::new(opcode::CATALOG_DROP, request_id),
        &request.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(stream, codec).await;
    let response = if header.status() == status::OK {
        CatalogDropResponse::decode(payload.as_slice()).expect("drop response")
    } else {
        CatalogDropResponse::default()
    };

    (header, response)
}

async fn put_document(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    request_id: u64,
    key: Vec<u8>,
    idempotency: &[u8],
    name: &str,
    level: u32,
) -> FrameHeader {
    write_frame(
        stream,
        codec,
        &FrameHeader::new(opcode::DOC_PUT, request_id),
        &DocumentPutRequest {
            meta: Some(RequestMeta {
                idempotency_key: idempotency.to_vec(),
                ..Default::default()
            }),
            collection_id: 1,
            partition: 0,
            document_key: key,
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
                            kind: Some(value::Kind::U32Value(level)),
                        }),
                    },
                ],
                if_match: None,
            }],
        }
        .encode_to_vec(),
    )
    .await;
    read_frame(stream, codec).await.0
}

fn document_key(player: u64) -> Vec<u8> {
    encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[KeyValue::U64(player)],
    )
    .expect("key")
}

fn catalog_changes() -> CatalogChanges {
    CatalogChanges {
        schemas: vec![RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "profile",
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
            ],
        )],
        keyspaces: vec![Keyspace::new(KeyspaceId::new(1), "game")],
        buckets: vec![(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(1),
                "player_profile",
                16,
                ModelBucketPolicy::Strict,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )],
        collections: vec![Collection::new(
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
        )],
        indexes: vec![(
            BucketId::new(1),
            ModelIndexDefinition::new(
                IndexId::new(1),
                "by-level",
                ModelIndexKind::Range,
                vec![FieldId::new(2)],
            ),
        )],
    }
}

#[test]
fn persists_applied_catalog_across_reopen() {
    let dir = std::env::temp_dir().join(format!(
        "recolith-catalog-apply-persist-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");

    {
        let backend = Arc::new(FjallBackend::open(&dir).expect("open"));
        let store = DocumentStore::new(Arc::new(Catalog::new()), backend.clone());
        let generation = store
            .apply_catalog(&catalog_changes(), None)
            .expect("apply");
        assert_eq!(generation.get(), 5);
        backend.persist().expect("persist");
    }

    let backend = Arc::new(FjallBackend::open(&dir).expect("reopen"));
    let catalog = load_catalog(backend.as_ref()).expect("load catalog");
    assert_eq!(catalog.generation().get(), 5);
    let snapshot = catalog.snapshot().expect("snapshot");
    assert!(snapshot.collections.contains_key(&CollectionId::new(1)));
    assert!(
        snapshot
            .indexes
            .get(&BucketId::new(1))
            .is_some_and(|indexes| indexes.contains_key(&IndexId::new(1)))
    );
    assert!(
        catalog
            .apply_changes(&catalog_changes())
            .expect("reapply")
            .is_none()
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn applies_catalog_atomically_and_serves_new_collection() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let (mut stream, codec) = connect(address).await;

    let (header, response) = apply(&mut stream, &codec, 2, &base_request()).await;
    assert_eq!(header.opcode(), opcode::CATALOG_APPLY);
    assert_eq!(header.status(), status::OK);
    assert_eq!(response.catalog_generation, 4);
    assert_eq!(store.catalog_generation().get(), 4);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DESCRIBE_COLLECTION, 3),
        &DescribeCollectionRequest {
            meta: None,
            keyspace: "game".to_owned(),
            target: Some(describe_collection_request::Target::CollectionName(
                "player".to_owned(),
            )),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let described = DescribeCollectionResponse::decode(payload.as_slice()).expect("describe");
    let collection = described.collection.expect("collection");
    assert_eq!(collection.name, "player");
    assert_eq!(collection.fragments.len(), 1);
    assert_eq!(described.catalog_generation, 4);

    let key = document_key(42);
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 4),
        &DocumentPutRequest {
            meta: Some(RequestMeta {
                idempotency_key: b"ddl-write".to_vec(),
                ..Default::default()
            }),
            collection_id: 1,
            partition: 0,
            document_key: key.clone(),
            fragments: vec![FragmentPayload {
                fragment_id: 1,
                subkey: Vec::new(),
                fields: vec![
                    FieldValue {
                        field_id: 1,
                        value: Some(Value {
                            kind: Some(value::Kind::StringValue("alex".to_owned())),
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
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::DOC_PUT);
    assert_eq!(header.status(), status::OK);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_GET, 5),
        &DocumentGetRequest {
            meta: None,
            collection_id: 1,
            partition: 0,
            document_key: key,
            fragments: vec![1],
            projection: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let read = DocumentGetResponse::decode(payload.as_slice()).expect("read");
    assert_eq!(read.fragments.len(), 1);

    let (header, response) = apply(&mut stream, &codec, 6, &base_request()).await;
    assert_eq!(header.status(), status::OK);
    assert_eq!(response.catalog_generation, 4, "identical apply is a no-op");

    let mut conflicting = base_request();
    conflicting.keyspaces[0].name = "other".to_owned();
    let (header, _) = apply(&mut stream, &codec, 7, &conflicting).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::CONFLICT);

    let mut stale = base_request();
    stale.meta = Some(RequestMeta {
        catalog_generation: 1,
        ..Default::default()
    });
    let (header, _) = apply(&mut stream, &codec, 8, &stale).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::STALE_CATALOG);
}

#[tokio::test]
async fn backfills_indexes_for_existing_records() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let (mut stream, codec) = connect(address).await;

    let (header, _) = apply(&mut stream, &codec, 2, &base_request()).await;
    assert_eq!(header.status(), status::OK);

    let header = put_document(
        &mut stream,
        &codec,
        3,
        document_key(7),
        b"backfill",
        "alex",
        5,
    )
    .await;
    assert_eq!(header.status(), status::OK);

    let (header, response) = apply(&mut stream, &codec, 4, &index_request()).await;
    assert_eq!(header.status(), status::OK);
    assert_eq!(response.catalog_generation, 5);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 5),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql:
                "SELECT profile.name FROM player WHERE player_id = 7 AND profile.level = 5 INCLUDE FRAGMENTS profile"
                    .to_owned(),
            parameters: Vec::new(),
            limit: 10,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    if header.status() != status::OK {
        let error = ErrorResponse::decode(payload.as_slice()).expect("error");
        panic!("query failed with {}: {}", header.status(), error.message);
    }
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    assert_eq!(response.documents.len(), 1);
    assert_eq!(response.documents[0].fragments.len(), 1);
}

#[tokio::test]
async fn rejects_unique_index_backfill_with_duplicate_values() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let (mut stream, codec) = connect(address).await;

    let (header, _) = apply(&mut stream, &codec, 2, &base_request()).await;
    assert_eq!(header.status(), status::OK);
    assert_eq!(
        put_document(&mut stream, &codec, 3, document_key(1), b"dup-1", "alex", 1)
            .await
            .status(),
        status::OK
    );
    assert_eq!(
        put_document(&mut stream, &codec, 4, document_key(2), b"dup-2", "alex", 2)
            .await
            .status(),
        status::OK
    );

    let unique = CatalogApplyRequest {
        meta: Some(RequestMeta::default()),
        indexes: vec![IndexDefinition {
            id: 1,
            bucket_id: 1,
            name: "by-name".to_owned(),
            kind: IndexKind::Unique as i32,
            fields: vec![1],
        }],
        ..Default::default()
    };
    let (header, _) = apply(&mut stream, &codec, 5, &unique).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::CONFLICT);
    assert!(
        !store
            .catalog_snapshot()
            .expect("snapshot")
            .indexes
            .contains_key(&BucketId::new(1))
    );
}

#[tokio::test]
async fn drops_collections_only_with_purge() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let (mut stream, codec) = connect(address).await;

    let (header, _) = apply(&mut stream, &codec, 2, &base_request()).await;
    assert_eq!(header.status(), status::OK);
    let key = document_key(9);
    assert_eq!(
        put_document(
            &mut stream,
            &codec,
            3,
            key.clone(),
            b"drop-collection",
            "alex",
            9
        )
        .await
        .status(),
        status::OK
    );

    let request = drop_target(catalog_drop_request::Target::CollectionId(1), false);
    let (header, _) = drop_catalog(&mut stream, &codec, 4, &request).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::FAILED_PRECONDITION);

    let (header, response) = drop_catalog(
        &mut stream,
        &codec,
        5,
        &drop_target(catalog_drop_request::Target::CollectionId(1), true),
    )
    .await;
    assert_eq!(header.status(), status::OK);
    assert_eq!(response.catalog_generation, 5);
    assert!(
        store
            .catalog_snapshot()
            .expect("snapshot")
            .collections
            .is_empty()
    );

    let (header, _) = apply(&mut stream, &codec, 6, &base_request()).await;
    assert_eq!(header.status(), status::OK);
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_GET, 7),
        &DocumentGetRequest {
            meta: None,
            collection_id: 1,
            partition: 0,
            document_key: key,
            fragments: vec![1],
            projection: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let read = DocumentGetResponse::decode(payload.as_slice()).expect("read");
    assert!(read.fragments.is_empty(), "purged records must not return");
}

#[tokio::test]
async fn drops_indexes_only_with_purge_and_queries_still_work() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let (mut stream, codec) = connect(address).await;

    let (header, _) = apply(&mut stream, &codec, 2, &base_request()).await;
    assert_eq!(header.status(), status::OK);
    assert_eq!(
        put_document(
            &mut stream,
            &codec,
            3,
            document_key(3),
            b"drop-index",
            "sam",
            3
        )
        .await
        .status(),
        status::OK
    );
    let (header, _) = apply(&mut stream, &codec, 4, &index_request()).await;
    assert_eq!(header.status(), status::OK);

    let target = catalog_drop_request::Target::Index(CatalogIndexTarget {
        bucket_id: 1,
        index_id: 1,
    });
    let (header, _) = drop_catalog(&mut stream, &codec, 5, &drop_target(target, false)).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::FAILED_PRECONDITION);

    let target = catalog_drop_request::Target::Index(CatalogIndexTarget {
        bucket_id: 1,
        index_id: 1,
    });
    let (header, response) = drop_catalog(&mut stream, &codec, 6, &drop_target(target, true)).await;
    assert_eq!(header.status(), status::OK);
    assert_eq!(response.catalog_generation, 6);
    assert!(
        !store
            .catalog_snapshot()
            .expect("snapshot")
            .indexes
            .contains_key(&BucketId::new(1))
    );

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 7),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql:
                "SELECT profile.name FROM player WHERE player_id = 3 AND profile.level = 3 INCLUDE FRAGMENTS profile"
                    .to_owned(),
            parameters: Vec::new(),
            limit: 10,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    if header.status() != status::OK {
        let error = ErrorResponse::decode(payload.as_slice()).expect("error");
        panic!("query failed with {}: {}", header.status(), error.message);
    }
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    assert_eq!(response.documents.len(), 1);
}

#[tokio::test]
async fn rejects_invalid_schemas_and_missing_references() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let (mut stream, codec) = connect(address).await;

    let mut invalid = base_request();
    invalid.schemas[0].idl = "record Broken {".to_owned();
    let (header, _) = apply(&mut stream, &codec, 2, &invalid).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::INVALID_REQUEST);

    let mut missing = base_request();
    missing.schemas.clear();
    let (header, _) = apply(&mut stream, &codec, 3, &missing).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::NOT_FOUND);
    assert_eq!(store.catalog_generation().get(), 0);
}
