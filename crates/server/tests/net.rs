use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_protocol::batch::{BatchLimits, SubFrame, encode_batch};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN};
use recolith_protocol::messages::{
    AuthMethod, AuthResult, DocumentGetRequest, ErrorResponse, HelloRequest, PingRequest,
    PingResponse, WelcomeResponse, compression_bit, opcode, status,
};
use recolith_server::{DocumentStore, ServerConfig, serve};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn empty_store() -> Arc<DocumentStore<MemoryBackend>> {
    Arc::new(DocumentStore::new(
        Arc::new(Catalog::new()),
        Arc::new(MemoryBackend::new()),
    ))
}

async fn start_server(config: ServerConfig) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        let _ = serve(listener, config, empty_store()).await;
    });

    address
}

fn hello(auth_method: AuthMethod, token: &[u8], compression: u32) -> HelloRequest {
    HelloRequest {
        protocol_min_major: 1,
        protocol_min_minor: 0,
        protocol_max_major: 1,
        protocol_max_minor: 0,
        client_id: "test".to_owned(),
        client_version: "0.1.0".to_owned(),
        auth_method: auth_method as i32,
        auth_token: token.to_vec(),
        features: 0,
        max_frame_size: 16 * 1024 * 1024,
        max_batch_subframes: 64,
        max_batch_bytes: 1024 * 1024,
        compression_algorithms: compression,
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

async fn handshake(stream: &mut TcpStream, hello: HelloRequest) -> (FrameCodec, WelcomeResponse) {
    let plain = FrameCodec::plain().expect("plain codec");
    let header = FrameHeader::new(opcode::HELLO, 1);
    write_frame(stream, &plain, &header, &hello.encode_to_vec()).await;
    let (header, payload) = read_frame(stream, &plain).await;
    assert_eq!(header.opcode(), opcode::WELCOME);
    let welcome = WelcomeResponse::decode(payload.as_slice()).expect("welcome");
    let algorithm = CompressionAlgorithm::from_wire(welcome.compression_algorithm as u8)
        .expect("known compression");
    let codec = FrameCodec::new(
        algorithm,
        welcome.compression_threshold as usize,
        welcome.max_frame_size as usize,
        welcome.max_decompressed_size as usize,
    )
    .expect("codec");

    (codec, welcome)
}

#[tokio::test]
async fn handshake_and_ping() {
    let address = start_server(ServerConfig::new("127.0.0.1:0".parse().expect("addr"))).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let (codec, welcome) = handshake(
        &mut stream,
        hello(
            AuthMethod::None,
            b"",
            compression_bit::ZSTD | compression_bit::LZ4,
        ),
    )
    .await;

    assert_eq!(welcome.protocol_major, 1);
    assert_eq!(welcome.auth_result, AuthResult::Ok as i32);
    assert_eq!(welcome.compression_algorithm, 1);

    let ping = PingRequest {
        payload: b"ping".to_vec(),
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::PING, 2),
        &ping.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &codec).await;
    let pong = PingResponse::decode(payload.as_slice()).expect("pong");

    assert_eq!(header.opcode(), opcode::PONG);
    assert_eq!(header.request_id(), 2);
    assert_eq!(pong.payload, b"ping");
}

#[tokio::test]
async fn rejects_wrong_token() {
    let address = start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("addr")).with_auth_token(b"secret"),
    )
    .await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let plain = FrameCodec::plain().expect("plain codec");
    let request = hello(AuthMethod::Bearer, b"wrong", 0);
    write_frame(
        &mut stream,
        &plain,
        &FrameHeader::new(opcode::HELLO, 1),
        &request.encode_to_vec(),
    )
    .await;
    let (header, payload) = read_frame(&mut stream, &plain).await;

    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::AUTH_FAILED);
    let error = ErrorResponse::decode(payload.as_slice()).expect("error");
    assert!(!error.message.is_empty());
}

#[tokio::test]
async fn rejects_unsupported_version() {
    let address = start_server(ServerConfig::new("127.0.0.1:0".parse().expect("addr"))).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let plain = FrameCodec::plain().expect("plain codec");
    let mut request = hello(AuthMethod::None, b"", 0);
    request.protocol_min_major = 2;
    request.protocol_max_major = 2;
    write_frame(
        &mut stream,
        &plain,
        &FrameHeader::new(opcode::HELLO, 1),
        &request.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &plain).await;

    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::PROTOCOL_VERSION);
}

#[tokio::test]
async fn handles_batched_pings() {
    let address = start_server(ServerConfig::new("127.0.0.1:0".parse().expect("addr"))).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let (codec, _) = handshake(&mut stream, hello(AuthMethod::None, b"", 0)).await;

    let first = PingRequest {
        payload: b"a".to_vec(),
    }
    .encode_to_vec();
    let second = PingRequest {
        payload: b"b".to_vec(),
    }
    .encode_to_vec();
    let batch = encode_batch(
        &[
            SubFrame::new(opcode::PING, 10, &first),
            SubFrame::new(opcode::PING, 11, &second),
        ],
        BatchLimits::default(),
    )
    .expect("batch");
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::BATCH, 9),
        &batch,
    )
    .await;

    let mut request_ids = Vec::new();
    for _ in 0..2 {
        let (header, payload) = read_frame(&mut stream, &codec).await;
        assert_eq!(header.opcode(), opcode::PONG);
        request_ids.push(header.request_id());
        assert!(
            !PingResponse::decode(payload.as_slice())
                .expect("pong")
                .payload
                .is_empty()
        );
    }
    request_ids.sort_unstable();

    assert_eq!(request_ids, vec![10, 11]);
}

#[tokio::test]
async fn compresses_large_ping_response() {
    let address = start_server(ServerConfig::new("127.0.0.1:0".parse().expect("addr"))).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let (codec, welcome) = handshake(
        &mut stream,
        hello(AuthMethod::None, b"", compression_bit::ZSTD),
    )
    .await;
    assert_eq!(welcome.compression_algorithm, 1);

    let payload = vec![b'x'; 4096];
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::PING, 3),
        &PingRequest {
            payload: payload.clone(),
        }
        .encode_to_vec(),
    )
    .await;
    let (header, response) = read_frame(&mut stream, &codec).await;
    let pong = PingResponse::decode(response.as_slice()).expect("pong");

    assert!(header.is_compressed());
    assert_eq!(pong.payload, payload);
}

#[tokio::test]
async fn rejects_unknown_collection_document_get() {
    let address = start_server(ServerConfig::new("127.0.0.1:0".parse().expect("addr"))).await;
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let (codec, _) = handshake(&mut stream, hello(AuthMethod::None, b"", 0)).await;

    let request = DocumentGetRequest {
        meta: None,
        collection_id: 99,
        partition: 0,
        document_key: b"player".to_vec(),
        fragments: Vec::new(),
        projection: Vec::new(),
    };
    write_frame(
        &mut stream,
        &codec,
        &FrameHeader::new(opcode::DOC_GET, 4),
        &request.encode_to_vec(),
    )
    .await;
    let (header, _) = read_frame(&mut stream, &codec).await;

    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::NOT_FOUND);
}
