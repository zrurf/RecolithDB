use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use prost::Message;
use recolith_backend_memory::MemoryBackend;
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_protocol::batch::{BatchLimits, SubFrame, encode_batch};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN};
use recolith_protocol::messages::{
    AggregateRequest, AuthMethod, AuthResult, BackupRequest, CatalogAlterRequest,
    CatalogApplyRequest, CatalogDropRequest, DescribeCollectionRequest, DocumentDeleteRequest,
    DocumentGetRequest, DocumentPutRequest, ErrorResponse, ExplainRequest, FieldValue,
    FragmentPayload, HelloRequest, KeyspaceDefinition, PingRequest, RequestMeta, SetBucketSchema,
    Value, WelcomeResponse, catalog_alter_request, catalog_drop_request,
    describe_collection_request, opcode, status, value,
};
use recolith_security::{
    Authenticator, TokenClaims, TokenIssuer, fingerprint_hex, token_fingerprint, unix_seconds,
};
use recolith_server::{DocumentStore, ServerConfig, serve};
use recolith_storage::encode_document_key;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ROLES: &str = r#"
[[roles]]
name = "reader"
permissions = [{ keyspace = "game", collection = "player", actions = ["read"] }]

[[roles]]
name = "writer"
permissions = [{ keyspace = "game", collection = "player", actions = ["read", "write"] }]

[[roles]]
name = "admin"
permissions = [{ keyspace = "*", collection = "*", actions = ["admin"] }]
"#;

struct Fixture {
    dir: PathBuf,
    secret: PathBuf,
    roles: PathBuf,
    revocations: PathBuf,
    issuer: TokenIssuer,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("recolith-auth-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp dir");
        let secret = dir.join("secret.hex");
        let roles = dir.join("roles.toml");
        let revocations = dir.join("revoked.toml");
        fs::write(&secret, "11".repeat(32)).expect("secret");
        fs::write(&roles, ROLES).expect("roles");
        fs::write(&revocations, "hashes = []\n").expect("revocations");
        let issuer = TokenIssuer::from_secret_file(&secret).expect("issuer");

        Self {
            dir,
            secret,
            roles,
            revocations,
            issuer,
        }
    }

    fn token(&self, subject: &str, role: &str) -> String {
        let now = unix_seconds();
        self.issue(subject, role, now, now + 3_600)
    }

    fn issue(&self, subject: &str, role: &str, issued_at: u64, expires_at: u64) -> String {
        self.issuer
            .issue(&TokenClaims {
                subject: subject.to_owned(),
                roles: vec![role.to_owned()],
                issued_at,
                expires_at,
            })
            .expect("token")
    }

    fn server_config(&self, address: SocketAddr) -> ServerConfig {
        let authenticator = Authenticator::from_files_with_revocations(
            &self.secret,
            None::<&PathBuf>,
            &self.roles,
            Some(&self.revocations),
        )
        .expect("authenticator");
        ServerConfig::new(address).with_auth(authenticator)
    }

    fn revoke(&self, token: &str) {
        let fingerprint = fingerprint_hex(&token_fingerprint(token));
        fs::write(&self.revocations, format!("hashes = [\"{fingerprint}\"]\n"))
            .expect("revocations");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn store() -> Arc<DocumentStore<MemoryBackend>> {
    Arc::new(DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    ))
}

async fn start_server(
    config: ServerConfig,
    store: Arc<DocumentStore<MemoryBackend>>,
) -> SocketAddr {
    let listener = TcpListener::bind(config.listen).await.expect("bind");
    let address = listener.local_addr().expect("address");
    let mut config = config;
    config.listen = address;
    tokio::spawn(serve(listener, config, store));

    address
}

fn hello(token: &str) -> HelloRequest {
    HelloRequest {
        protocol_min_major: 1,
        protocol_min_minor: 0,
        protocol_max_major: 1,
        protocol_max_minor: 0,
        client_id: "auth-test".to_owned(),
        client_version: "0.1.0".to_owned(),
        auth_method: AuthMethod::Bearer as i32,
        auth_token: token.as_bytes().to_vec(),
        features: 0,
        max_frame_size: 16 * 1024 * 1024,
        max_batch_subframes: 64,
        max_batch_bytes: 1024 * 1024,
        compression_algorithms: 0,
        compression_threshold: 0,
        encodings: 1,
    }
}

async fn connect(address: SocketAddr, token: &str) -> (TcpStream, FrameCodec) {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let plain = FrameCodec::plain().expect("plain");
    write_frame(
        &mut stream,
        &plain,
        &FrameHeader::new(opcode::HELLO, 1),
        &hello(token).encode_to_vec(),
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

async fn rejected(address: SocketAddr, token: &str) {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let plain = FrameCodec::plain().expect("plain");
    write_frame(
        &mut stream,
        &plain,
        &FrameHeader::new(opcode::HELLO, 1),
        &hello(token).encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &plain).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::AUTH_FAILED);
    ErrorResponse::decode(payload.as_slice()).expect("error");
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

fn key() -> Vec<u8> {
    encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[KeyValue::U64(42)],
    )
    .expect("key")
}

fn put(idempotency: &str, name: &str) -> Vec<u8> {
    DocumentPutRequest {
        meta: Some(RequestMeta {
            idempotency_key: idempotency.as_bytes().to_vec(),
            ..Default::default()
        }),
        collection_id: 1,
        partition: 0,
        document_key: key(),
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
    .encode_to_vec()
}

fn get() -> Vec<u8> {
    DocumentGetRequest {
        meta: None,
        collection_id: 1,
        partition: 0,
        document_key: key(),
        fragments: vec![1],
        projection: Vec::new(),
    }
    .encode_to_vec()
}

#[tokio::test]
async fn roles_authorize_operations_and_return_permission_denied() {
    let fixture = Fixture::new("roles");
    let address = start_server(
        fixture.server_config("127.0.0.1:0".parse().expect("addr")),
        store(),
    )
    .await;

    let (mut reader, reader_codec) = connect(address, &fixture.token("reader", "reader")).await;
    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::DOC_GET, 2),
        &get(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::DOC_GET);
    assert_eq!(header.status(), status::OK);

    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::EXPLAIN, 6),
        &ExplainRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 0 \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::EXPLAIN);
    assert_eq!(header.status(), status::OK);

    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::AGGREGATE, 7),
        &AggregateRequest {
            meta: None,
            partition: 0,
            rql: "SELECT COUNT(*) FROM player WHERE profile.level >= 0 \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::AGGREGATE);
    assert_eq!(header.status(), status::OK);

    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::BACKUP, 8),
        &BackupRequest {
            meta: None,
            name: "reader.backup".to_owned(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::PERMISSION_DENIED);

    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::DOC_DELETE, 9),
        &DocumentDeleteRequest {
            meta: None,
            collection_id: 1,
            partition: 0,
            document_key: key(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::PERMISSION_DENIED);

    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::DOC_PUT, 3),
        &put("r-1", "alex"),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::PERMISSION_DENIED);

    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::DESCRIBE_COLLECTION, 4),
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
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.status(), status::PERMISSION_DENIED);

    let (mut writer, writer_codec) = connect(address, &fixture.token("writer", "writer")).await;
    write_frame(
        &mut writer,
        &writer_codec,
        &FrameHeader::new(opcode::DOC_PUT, 5),
        &put("w-1", "alex"),
    )
    .await;
    let (header, _) = read_frame(&mut writer, &writer_codec).await;
    assert_eq!(header.opcode(), opcode::DOC_PUT);
    assert_eq!(header.status(), status::OK);
}

#[tokio::test]
async fn admin_role_can_describe_and_batch_is_checked_per_subframe() {
    let fixture = Fixture::new("admin");
    let address = start_server(
        fixture.server_config("127.0.0.1:0".parse().expect("addr")),
        store(),
    )
    .await;

    let (mut admin, admin_codec) = connect(address, &fixture.token("admin", "admin")).await;
    write_frame(
        &mut admin,
        &admin_codec,
        &FrameHeader::new(opcode::DESCRIBE_COLLECTION, 2),
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
    let (header, _) = read_frame(&mut admin, &admin_codec).await;
    assert_eq!(header.opcode(), opcode::DESCRIBE_COLLECTION);
    assert_eq!(header.status(), status::OK);

    let (mut reader, reader_codec) = connect(address, &fixture.token("reader", "reader")).await;
    let ping = PingRequest {
        payload: b"batch".to_vec(),
    }
    .encode_to_vec();
    let body = encode_batch(
        &[
            SubFrame::new(opcode::PING, 10, &ping),
            SubFrame::new(opcode::DOC_PUT, 11, &put("r-batch", "alex")),
        ],
        BatchLimits::default(),
    )
    .expect("batch");
    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::BATCH, 9),
        &body,
    )
    .await;

    let mut statuses = Vec::new();
    for _ in 0..2 {
        let (header, _) = read_frame(&mut reader, &reader_codec).await;
        statuses.push((header.opcode(), header.status()));
    }
    assert!(statuses.contains(&(opcode::PONG, status::OK)));
    assert!(statuses.contains(&(opcode::ERROR, status::PERMISSION_DENIED)));
}

#[tokio::test]
async fn rejects_invalid_and_expired_tokens_and_honors_expiry_per_request() {
    let fixture = Fixture::new("expiry");
    let address = start_server(
        fixture.server_config("127.0.0.1:0".parse().expect("addr")),
        store(),
    )
    .await;

    rejected(address, "not-a-token").await;

    let now = unix_seconds();
    rejected(
        address,
        &fixture.issue("expired", "reader", now - 20, now - 10),
    )
    .await;

    let now = unix_seconds();
    let expiring = fixture.issue("short", "reader", now, now + 3);
    let (mut stream, codec) = connect(address, &expiring).await;
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::PING, 20),
        &PingRequest {
            payload: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::AUTH_FAILED);
}

#[tokio::test]
async fn catalog_changes_require_admin_on_affected_scopes() {
    let fixture = Fixture::new("catalog");
    let address = start_server(
        fixture.server_config("127.0.0.1:0".parse().expect("addr")),
        store(),
    )
    .await;

    let request = CatalogApplyRequest {
        meta: None,
        keyspaces: vec![KeyspaceDefinition {
            id: 2,
            name: "events".to_owned(),
        }],
        ..Default::default()
    };
    let (mut reader, reader_codec) = connect(address, &fixture.token("reader", "reader")).await;
    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::CATALOG_APPLY, 2),
        &request.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::PERMISSION_DENIED);

    let (mut admin, admin_codec) = connect(address, &fixture.token("admin", "admin")).await;
    write_frame(
        &mut admin,
        &admin_codec,
        &FrameHeader::new(opcode::CATALOG_APPLY, 3),
        &request.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut admin, &admin_codec).await;
    assert_eq!(header.opcode(), opcode::CATALOG_APPLY);
    assert_eq!(header.status(), status::OK);

    let drop = CatalogDropRequest {
        meta: None,
        purge: false,
        target: Some(catalog_drop_request::Target::CollectionId(1)),
    };
    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::CATALOG_DROP, 4),
        &drop.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::PERMISSION_DENIED);

    write_frame(
        &mut admin,
        &admin_codec,
        &FrameHeader::new(opcode::CATALOG_DROP, 5),
        &drop.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut admin, &admin_codec).await;
    assert_eq!(header.opcode(), opcode::CATALOG_DROP);
    assert_eq!(header.status(), status::OK);

    let alter = CatalogAlterRequest {
        meta: None,
        change: Some(catalog_alter_request::Change::BucketSchema(
            SetBucketSchema {
                bucket_id: 1,
                schema_version: 1,
                migration: None,
            },
        )),
    };
    write_frame(
        &mut reader,
        &reader_codec,
        &FrameHeader::new(opcode::CATALOG_ALTER, 6),
        &alter.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut reader, &reader_codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::PERMISSION_DENIED);

    write_frame(
        &mut admin,
        &admin_codec,
        &FrameHeader::new(opcode::CATALOG_ALTER, 7),
        &alter.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut admin, &admin_codec).await;
    assert_eq!(header.opcode(), opcode::CATALOG_ALTER);
    assert_eq!(header.status(), status::OK);
}

#[tokio::test]
async fn idempotency_is_scoped_by_principal() {
    let fixture = Fixture::new("idempotency");
    let address = start_server(
        fixture.server_config("127.0.0.1:0".parse().expect("addr")),
        store(),
    )
    .await;

    let (mut first, first_codec) = connect(address, &fixture.token("writer-a", "writer")).await;
    write_frame(
        &mut first,
        &first_codec,
        &FrameHeader::new(opcode::DOC_PUT, 2),
        &put("shared", "alex"),
    )
    .await;
    let (header, _) = read_frame(&mut first, &first_codec).await;
    assert_eq!(header.status(), status::OK);

    let (mut second, second_codec) = connect(address, &fixture.token("writer-b", "writer")).await;
    write_frame(
        &mut second,
        &second_codec,
        &FrameHeader::new(opcode::DOC_PUT, 2),
        &put("shared", "sam"),
    )
    .await;
    let (header, _) = read_frame(&mut second, &second_codec).await;
    assert_eq!(header.opcode(), opcode::DOC_PUT);
    assert_eq!(header.status(), status::OK);
}

#[tokio::test]
async fn revoked_tokens_are_rejected_at_handshake_and_mid_session() {
    let fixture = Fixture::new("revoked");
    let address = start_server(
        fixture.server_config("127.0.0.1:0".parse().expect("addr")),
        store(),
    )
    .await;

    let token = fixture.token("reader", "reader");
    let (mut stream, codec) = connect(address, &token).await;
    fixture.revoke(&token);
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::PING, 20),
        &PingRequest {
            payload: Vec::new(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::AUTH_FAILED);

    let other = fixture.token("other", "reader");
    fixture.revoke(&other);
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    rejected(address, &other).await;
}
