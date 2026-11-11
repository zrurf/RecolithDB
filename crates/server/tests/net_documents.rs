use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, KeyValue, Keyspace,
};
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN};
use recolith_protocol::messages::{
    AggregateRequest, AggregateResponse, AuthMethod, CatalogApplyRequest,
    DescribeCollectionRequest, DescribeCollectionResponse, DocumentDeleteRequest,
    DocumentDeleteResponse, DocumentGetRequest, DocumentGetResponse, DocumentPutRequest,
    DocumentPutResponse, ExplainRequest, FieldSelection, FieldValue, FragmentPayload, HelloRequest,
    KeyFieldType, KeyspaceDefinition, NamedParameter, QueryRequest, QueryResponse, RequestMeta,
    SchemaScalarType, Value, WelcomeResponse, describe_collection_request, explain_response,
    opcode, schema_type, status, value,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{DocumentStore, ServerConfig, serve};
use recolith_storage::encode_document_key;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

fn store() -> Arc<DocumentStore<MemoryBackend>> {
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
        .create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(1),
                "by-level",
                IndexKind::Range,
                vec![FieldId::new(2)],
            ),
        )
        .expect("index");

    Arc::new(DocumentStore::new(
        Arc::new(catalog),
        Arc::new(MemoryBackend::new()),
    ))
}

async fn start_server(store: Arc<DocumentStore<MemoryBackend>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let config = ServerConfig::new(address);
    tokio::spawn(async move {
        let _ = serve(listener, config, store).await;
    });

    address
}

fn hello() -> HelloRequest {
    HelloRequest {
        protocol_min_major: 1,
        protocol_min_minor: 0,
        protocol_max_major: 1,
        protocol_max_minor: 0,
        client_id: "test".to_owned(),
        client_version: "0.1.0".to_owned(),
        auth_method: AuthMethod::None as i32,
        auth_token: Vec::new(),
        features: 0,
        max_frame_size: 16 * 1024 * 1024,
        max_batch_subframes: 64,
        max_batch_bytes: 1024 * 1024,
        compression_algorithms: 0,
        compression_threshold: 256,
        encodings: 1,
    }
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
    let header = FrameHeader::decode(&header).expect("decode header");
    let mut payload = vec![0_u8; header.payload_len() as usize];
    stream.read_exact(&mut payload).await.expect("payload");
    let payload = codec.decode(&header, &payload).expect("decode payload");

    (header, payload)
}

async fn handshake(stream: &mut TcpStream) -> FrameCodec {
    let plain = FrameCodec::plain().expect("plain");
    write_frame(
        stream,
        &plain,
        &FrameHeader::new(opcode::HELLO, 1),
        &hello().encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(stream, &plain).await;
    assert_eq!(header.opcode(), opcode::WELCOME);
    let welcome = WelcomeResponse::decode(payload.as_slice()).expect("welcome");
    assert_eq!(
        welcome.auth_result,
        recolith_protocol::messages::AuthResult::Ok as i32
    );
    FrameCodec::new(
        recolith_protocol::compression::CompressionAlgorithm::None,
        welcome.compression_threshold as usize,
        welcome.max_frame_size as usize,
        welcome.max_decompressed_size as usize,
    )
    .expect("codec")
}

fn document_key(player: u64) -> Vec<u8> {
    encode_document_key(&key_schema(), &[KeyValue::U64(player)]).expect("key")
}

fn profile_put(key: &[u8], name: &str, idempotency: &[u8]) -> DocumentPutRequest {
    profile_put_level(key, name, 10, idempotency)
}

fn profile_put_level(key: &[u8], name: &str, level: u32, idempotency: &[u8]) -> DocumentPutRequest {
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
                        kind: Some(value::Kind::U32Value(level)),
                    }),
                },
            ],
            if_match: None,
        }],
        ttl_seconds: None,
    }
}

#[tokio::test]
async fn writes_reads_and_replays_document() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;
    let key = document_key(42);

    let put = profile_put(&key, "alex", b"idem-1");
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 10),
        &put.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::DOC_PUT);
    assert!(header.is_response());
    assert_eq!(header.status(), status::OK);
    let response = DocumentPutResponse::decode(payload.as_slice()).expect("put response");
    assert_eq!(response.commit_ts, 1);
    assert_eq!(response.versions.len(), 1);

    let get = DocumentGetRequest {
        meta: None,
        collection_id: 1,
        partition: 0,
        document_key: key.clone(),
        fragments: vec![1],
        projection: vec![FieldSelection {
            fragment_id: 1,
            field_id: 1,
        }],
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_GET, 11),
        &get.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::DOC_GET);
    let response = DocumentGetResponse::decode(payload.as_slice()).expect("get response");
    assert_eq!(response.fragments.len(), 1);
    assert_eq!(
        response.fragments[0].fields,
        vec![FieldValue {
            field_id: 1,
            value: Some(Value {
                kind: Some(value::Kind::StringValue("alex".to_owned())),
            }),
        }]
    );

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 12),
        &put.encode_to_vec(),
    )
    .await;
    let (_, payload) = read_frame(&mut stream, &codec).await;
    let replay = DocumentPutResponse::decode(payload.as_slice()).expect("replay");
    assert_eq!(replay.commit_ts, 1);
}

#[tokio::test]
async fn rejects_idempotency_key_reuse_with_different_payload() {
    let store = store();
    let address = start_server(store).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;
    let key = document_key(42);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 20),
        &profile_put(&key, "alex", b"idem-2").encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 21),
        &profile_put(&key, "sam", b"idem-2").encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;

    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::CONFLICT);
}

#[tokio::test]
async fn requires_idempotency_key_for_writes() {
    let store = store();
    let address = start_server(store).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;
    let key = document_key(42);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 30),
        &profile_put(&key, "alex", b"").encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;

    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::INVALID_REQUEST);
}

#[tokio::test]
async fn executes_rql_query_over_protocol() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;
    let key = document_key(42);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 40),
        &profile_put(&key, "alex", b"idem-3").encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);

    let query = QueryRequest {
        meta: None,
        partition: 0,
        rql: "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile"
            .to_owned(),
        parameters: vec![NamedParameter {
            name: "id".to_owned(),
            value: Some(Value {
                kind: Some(value::Kind::U64Value(42)),
            }),
        }],
        limit: 10,
        cursor: Vec::new(),
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 41),
        &query.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::QUERY);
    assert_eq!(header.status(), status::OK);
    let response = QueryResponse::decode(payload.as_slice()).expect("query response");

    assert_eq!(response.documents.len(), 1);
    assert_eq!(response.documents[0].document_key, key);
    assert_eq!(
        response.documents[0].fragments[0].fields[0].value,
        Some(Value {
            kind: Some(value::Kind::StringValue("alex".to_owned())),
        })
    );
}

#[tokio::test]
async fn describes_collection_metadata() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    let request = DescribeCollectionRequest {
        meta: None,
        keyspace: "game".to_owned(),
        target: Some(describe_collection_request::Target::CollectionName(
            "player".to_owned(),
        )),
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DESCRIBE_COLLECTION, 50),
        &request.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::DESCRIBE_COLLECTION);
    assert_eq!(header.status(), status::OK);
    let response = DescribeCollectionResponse::decode(payload.as_slice()).expect("describe");
    assert_eq!(
        response.catalog_generation,
        store.catalog_generation().get()
    );
    let collection = response.collection.expect("collection");
    assert_eq!(collection.id, 1);
    assert_eq!(collection.keyspace, "game");
    assert_eq!(collection.name, "player");
    assert_eq!(collection.key_fields[0].name, "player_id");
    assert_eq!(collection.key_fields[0].r#type, KeyFieldType::U64 as i32);
    assert_eq!(collection.fragments[0].name, "profile");
    assert_eq!(collection.fragments[0].fields[0].name, "name");
    assert!(matches!(
        collection.fragments[0].fields[0]
            .r#type
            .as_ref()
            .and_then(|schema_type| schema_type.kind.as_ref()),
        Some(schema_type::Kind::Scalar(value)) if *value == SchemaScalarType::String as i32
    ));
}

#[tokio::test]
async fn rejects_unknown_or_stale_collection_description() {
    let store = store();
    let address = start_server(store).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    let unknown = DescribeCollectionRequest {
        meta: None,
        keyspace: String::new(),
        target: Some(describe_collection_request::Target::CollectionName(
            "missing".to_owned(),
        )),
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DESCRIBE_COLLECTION, 51),
        &unknown.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::NOT_FOUND);

    let stale = DescribeCollectionRequest {
        meta: Some(RequestMeta {
            catalog_generation: 999,
            ..Default::default()
        }),
        keyspace: String::new(),
        target: Some(describe_collection_request::Target::CollectionId(1)),
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DESCRIBE_COLLECTION, 52),
        &stale.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::STALE_CATALOG);
}

#[tokio::test]
async fn scans_multiple_documents_without_a_key_predicate() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    for (index, player) in [1_u64, 2, 3].into_iter().enumerate() {
        let put = profile_put(
            &document_key(player),
            &format!("player-{player}"),
            format!("scan-{index}").as_bytes(),
        );
        write_frame(
            &mut stream,
            &codec,
            &FrameHeader::new(opcode::DOC_PUT, 10 + index as u64),
            &put.encode_to_vec(),
        )
        .await;
        let (header, _) = read_frame(&mut stream, &codec).await;
        assert_eq!(header.status(), status::OK);
    }

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 20),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 2,
            cursor: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    assert_eq!(response.documents.len(), 2);
    assert!(!response.next_cursor.is_empty());
    let cursor = response.next_cursor;
    let stale_cursor = cursor.clone();

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 22),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
            cursor: cursor.clone(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    assert_eq!(response.documents.len(), 1);
    assert!(response.next_cursor.is_empty());

    let mut tampered = cursor.clone();
    tampered[0] ^= 1;
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 23),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
            cursor: tampered,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::INVALID_REQUEST);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 24),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 20 \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
            cursor,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::INVALID_REQUEST);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 21),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE player_id >= 2 \
                  AND profile.level >= 10 INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
            cursor: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    assert_eq!(response.documents.len(), 2);
    let keys: Vec<_> = response
        .documents
        .iter()
        .map(|document| document.document_key.clone())
        .collect();
    assert_eq!(keys, vec![document_key(2), document_key(3)]);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::CATALOG_APPLY, 25),
        &CatalogApplyRequest {
            meta: None,
            keyspaces: vec![KeyspaceDefinition {
                id: 2,
                name: "events".to_owned(),
            }],
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 26),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
            cursor: stale_cursor,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::STALE_CATALOG);
}

#[tokio::test]
async fn orders_and_paginates_scans() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    for (index, (player, level)) in [(1_u64, 30_u32), (2, 10), (3, 30)].into_iter().enumerate() {
        let put = profile_put_level(
            &document_key(player),
            &format!("player-{player}"),
            level,
            format!("order-{index}").as_bytes(),
        );
        write_frame(
            &mut stream,
            &codec,
            &FrameHeader::new(opcode::DOC_PUT, 10 + index as u64),
            &put.encode_to_vec(),
        )
        .await;
        let (header, _) = read_frame(&mut stream, &codec).await;
        assert_eq!(header.status(), status::OK);
    }

    const RQL: &str = "SELECT profile.name FROM player WHERE player_id >= 0 \
                       INCLUDE FRAGMENTS profile ORDER BY profile.level DESC";
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 20),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: RQL.to_owned(),
            parameters: Vec::new(),
            limit: 2,
            cursor: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    let keys: Vec<_> = response
        .documents
        .iter()
        .map(|document| document.document_key.clone())
        .collect();
    assert_eq!(keys, vec![document_key(3), document_key(1)]);
    assert!(!response.next_cursor.is_empty());

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 21),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: RQL.to_owned(),
            parameters: Vec::new(),
            limit: 2,
            cursor: response.next_cursor,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    let keys: Vec<_> = response
        .documents
        .iter()
        .map(|document| document.document_key.clone())
        .collect();
    assert_eq!(keys, vec![document_key(2)]);
    assert!(response.next_cursor.is_empty());

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 22),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE player_id >= 0 \
                  INCLUDE FRAGMENTS profile ORDER BY player_id"
                .to_owned(),
            parameters: Vec::new(),
            limit: 2,
            cursor: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::INVALID_REQUEST);
}

#[tokio::test]
async fn explains_point_and_scan_queries() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::EXPLAIN, 10),
        &ExplainRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE player_id = :id \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: vec![NamedParameter {
                name: "id".to_owned(),
                value: Some(Value {
                    kind: Some(value::Kind::U64Value(42)),
                }),
            }],
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::EXPLAIN);
    assert_eq!(header.status(), status::OK);
    let response = recolith_protocol::messages::ExplainResponse::decode(payload.as_slice())
        .expect("explain response");
    assert!(response.catalog_generation > 0);
    let Some(explain_response::Plan::Document(plan)) = response.plan else {
        panic!("expected document plan");
    };
    assert_eq!(plan.collection_id, 1);
    assert_eq!(plan.document_key, document_key(42));
    assert_eq!(plan.fragment_ids, vec![1]);
    assert_eq!(plan.projections[0].field_id, 1);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::EXPLAIN, 11),
        &ExplainRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
                  INCLUDE FRAGMENTS profile ORDER BY profile.level DESC"
                .to_owned(),
            parameters: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = recolith_protocol::messages::ExplainResponse::decode(payload.as_slice())
        .expect("explain response");
    let Some(explain_response::Plan::Scan(plan)) = response.plan else {
        panic!("expected scan plan");
    };
    assert_eq!(plan.collection_id, 1);
    assert_eq!(plan.partition, 0);
    assert_eq!(plan.fragment_ids, vec![1]);
    assert_eq!(plan.candidate_limit, 10_000);
    assert_eq!(plan.drives.len(), 1);
    assert!(!plan.drives[0].bucket_scan);
    assert_eq!(plan.drives[0].indexes[0].index_id, 1);
    assert_eq!(plan.drives[0].indexes[0].field_ids, vec![2]);
    assert_eq!(plan.filters[0].field_id, 2);
    assert_eq!(plan.order.as_ref().expect("order").index_id, None);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::EXPLAIN, 12),
        &ExplainRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE player_id >= 0 \
                  INCLUDE FRAGMENTS profile ORDER BY profile.level DESC"
                .to_owned(),
            parameters: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = recolith_protocol::messages::ExplainResponse::decode(payload.as_slice())
        .expect("explain response");
    let Some(explain_response::Plan::Scan(plan)) = response.plan else {
        panic!("expected scan plan");
    };
    assert!(plan.drives[0].bucket_scan);
    let order = plan.order.expect("order");
    assert_eq!(order.fragment_id, 1);
    assert_eq!(order.field_id, 2);
    assert!(order.descending);
    assert_eq!(order.index_id, Some(1));
    assert_eq!(plan.key_filters[0].field, 0);
}

#[tokio::test]
async fn aggregates_global_and_grouped_queries() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    for (index, (player, level)) in [(1_u64, 10_u32), (2, 20), (3, 30)].into_iter().enumerate() {
        let put = profile_put_level(
            &document_key(player),
            &format!("player-{player}"),
            level,
            format!("aggregate-{index}").as_bytes(),
        );
        write_frame(
            &mut stream,
            &codec,
            &FrameHeader::new(opcode::DOC_PUT, 10 + index as u64),
            &put.encode_to_vec(),
        )
        .await;
        let (header, _) = read_frame(&mut stream, &codec).await;
        assert_eq!(header.status(), status::OK);
    }

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::AGGREGATE, 20),
        &AggregateRequest {
            meta: None,
            partition: 0,
            rql: "SELECT COUNT(*), SUM(profile.level), AVG(profile.level) \
                  FROM player WHERE profile.level >= 0 INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::AGGREGATE);
    assert_eq!(header.status(), status::OK);
    let response = AggregateResponse::decode(payload.as_slice()).expect("aggregate response");
    assert_eq!(response.columns.len(), 3);
    assert_eq!(response.group_fragment_id, None);
    assert_eq!(response.rows.len(), 1);
    assert_eq!(
        response.rows[0].values,
        vec![
            Value {
                kind: Some(value::Kind::U64Value(3)),
            },
            Value {
                kind: Some(value::Kind::U64Value(60)),
            },
            Value {
                kind: Some(value::Kind::F64Value(20.0)),
            },
        ]
    );

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::AGGREGATE, 21),
        &AggregateRequest {
            meta: None,
            partition: 0,
            rql: "SELECT COUNT(*) FROM player WHERE profile.level >= 0 \
                  INCLUDE FRAGMENTS profile GROUP BY profile.level LIMIT 2"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = AggregateResponse::decode(payload.as_slice()).expect("aggregate response");
    assert_eq!(response.group_fragment_id, Some(1));
    assert_eq!(response.group_field_id, Some(2));
    assert_eq!(response.rows.len(), 2);
    assert!(response.truncated);
    assert_eq!(
        response.rows[0].group,
        Some(Value {
            kind: Some(value::Kind::U32Value(10)),
        })
    );
    assert_eq!(
        response.rows[1].group,
        Some(Value {
            kind: Some(value::Kind::U32Value(20)),
        })
    );
}

#[tokio::test]
async fn deletes_documents_and_reports_missing_documents() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;
    let key = document_key(1);

    let put = profile_put(&key, "alex", b"delete-1");
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 10),
        &put.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);

    for (request_id, expected) in [(20_u64, true), (21, false)] {
        write_frame(
            &mut stream,
            &codec,
            &FrameHeader::new(opcode::DOC_DELETE, request_id),
            &DocumentDeleteRequest {
                meta: None,
                collection_id: 1,
                partition: 0,
                document_key: key.clone(),
            }
            .encode_to_vec(),
        )
        .await;
        let (header, payload) = read_frame(&mut stream, &codec).await;
        assert_eq!(header.opcode(), opcode::DOC_DELETE);
        assert_eq!(header.status(), status::OK);
        let response = DocumentDeleteResponse::decode(payload.as_slice()).expect("delete response");
        assert_eq!(response.deleted, expected);
    }

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::QUERY, 22),
        &QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE player_id = :id \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: vec![NamedParameter {
                name: "id".to_owned(),
                value: Some(Value {
                    kind: Some(value::Kind::U64Value(1)),
                }),
            }],
            limit: 10,
            cursor: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    let response = QueryResponse::decode(payload.as_slice()).expect("query");
    assert!(response.documents.is_empty());
}

#[tokio::test]
async fn hides_expired_documents_after_ttl() {
    let store = store();
    let address = start_server(Arc::clone(&store)).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;
    let key = document_key(1);
    let mut put = profile_put(&key, "alex", b"ttl-1");
    put.ttl_seconds = Some(1);

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_PUT, 10),
        &put.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);

    let get = DocumentGetRequest {
        meta: None,
        collection_id: 1,
        partition: 0,
        document_key: key.clone(),
        fragments: vec![1],
        projection: Vec::new(),
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_GET, 11),
        &get.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    assert!(
        !DocumentGetResponse::decode(payload.as_slice())
            .expect("get")
            .fragments
            .is_empty()
    );

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_GET, 12),
        &get.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.status(), status::OK);
    assert!(
        DocumentGetResponse::decode(payload.as_slice())
            .expect("get")
            .fragments
            .is_empty()
    );
}
