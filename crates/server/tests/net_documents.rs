use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, KeyValue, Keyspace,
};
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN};
use recolith_protocol::messages::{
    AuthMethod, DocumentGetRequest, DocumentGetResponse, DocumentPutRequest, DocumentPutResponse,
    FieldSelection, FieldValue, FragmentPayload, HelloRequest, NamedParameter, QueryRequest,
    QueryResponse, RequestMeta, Value, WelcomeResponse, opcode, status, value,
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
