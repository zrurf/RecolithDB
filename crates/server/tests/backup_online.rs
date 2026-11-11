use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use prost::Message;
use recolith_backend_memory::MemoryBackend;
use recolith_common::{CollectionId, FieldId, FragmentId, PartitionId};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN};
use recolith_protocol::messages::{
    AuthMethod, BackupRequest, BackupResponse, ErrorResponse, HelloRequest, WelcomeResponse,
    feature, opcode, status,
};
use recolith_server::{
    DocumentStore, DocumentWrite, FragmentWriteRequest, ServerConfig, serve, verify,
};
use recolith_storage::{RecordData, Value, encode_document_key};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "recolith-online-backup-{label}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create temp dir");

    path
}

fn store(directory: Option<PathBuf>) -> Arc<DocumentStore<MemoryBackend>> {
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    );
    let store = match directory {
        Some(directory) => store.with_backup_directory(directory),
        None => store,
    };
    let key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[KeyValue::U64(1)],
    )
    .expect("document key");
    let mut profile = RecordData::new();
    profile.insert(FieldId::new(1), Value::String("alex".to_owned()));
    profile.insert(FieldId::new(2), Value::U32(10));
    store
        .write(
            &DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0)).with_fragment(
                FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile),
            ),
        )
        .expect("write document");

    Arc::new(store)
}

async fn start_server(store: Arc<DocumentStore<MemoryBackend>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        let _ = serve(listener, ServerConfig::new(address), store).await;
    });

    address
}

fn hello() -> HelloRequest {
    HelloRequest {
        protocol_min_major: 1,
        protocol_min_minor: 0,
        protocol_max_major: 1,
        protocol_max_minor: 0,
        client_id: "backup-test".to_owned(),
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
    assert_eq!(welcome.features & feature::BACKUP, feature::BACKUP);
    FrameCodec::new(
        CompressionAlgorithm::None,
        welcome.compression_threshold as usize,
        welcome.max_frame_size as usize,
        welcome.max_decompressed_size as usize,
    )
    .expect("codec")
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

async fn backup(
    stream: &mut TcpStream,
    codec: &FrameCodec,
    request_id: u64,
    name: &str,
) -> (FrameHeader, Vec<u8>) {
    write_frame(
        stream,
        codec,
        &FrameHeader::new(opcode::BACKUP, request_id),
        &BackupRequest {
            meta: None,
            name: name.to_owned(),
        }
        .encode_to_vec(),
    )
    .await;
    read_frame(stream, codec).await
}

#[tokio::test]
async fn writes_online_backup_and_rejects_invalid_names() {
    let directory = temp_dir("valid");
    let store = store(Some(directory.clone()));
    let address = start_server(store).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    let (header, payload) = backup(&mut stream, &codec, 2, "game.backup").await;
    assert_eq!(header.opcode(), opcode::BACKUP);
    assert_eq!(header.status(), status::OK);
    let response = BackupResponse::decode(payload.as_slice()).expect("backup response");
    assert_eq!(response.file_name, "game.backup");
    assert!(response.entries > 0);
    assert!(response.bytes > 0);
    let path = directory.join("game.backup");
    assert!(path.exists());
    let stats = verify(&path).expect("verify");
    assert_eq!(stats.entries, response.entries);
    assert_eq!(stats.bytes, response.bytes);

    let (header, _) = backup(&mut stream, &codec, 3, "game.backup").await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::CONFLICT);

    let (header, payload) = backup(&mut stream, &codec, 4, "../escape.backup").await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::INVALID_REQUEST);
    ErrorResponse::decode(payload.as_slice()).expect("error response");

    let _ = std::fs::remove_dir_all(directory);
}

#[tokio::test]
async fn requires_a_configured_backup_directory() {
    let store = store(None);
    let address = start_server(store).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let codec = handshake(&mut stream).await;

    let (header, _) = backup(&mut stream, &codec, 2, "game.backup").await;
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::FAILED_PRECONDITION);
}
