use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_protocol::batch::{BatchLimits, SubFrame, encode_batch};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN};
use recolith_protocol::messages::{
    AuthMethod, AuthResult, DocumentGetRequest, ErrorResponse, HelloRequest, PingRequest,
    PingResponse, WelcomeResponse, opcode, status,
};
use recolith_server::{DocumentStore, ServerConfig, serve};
use recolith_storage::crc32c;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn empty_store() -> Arc<DocumentStore<MemoryBackend>> {
    Arc::new(DocumentStore::new(
        Arc::new(Catalog::new()),
        Arc::new(MemoryBackend::new()),
    ))
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(serve(listener, ServerConfig::new(address), empty_store()));

    address
}

fn hello() -> HelloRequest {
    HelloRequest {
        protocol_min_major: 1,
        protocol_min_minor: 0,
        protocol_max_major: 1,
        protocol_max_minor: 0,
        client_id: "robustness-test".to_owned(),
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

async fn connect_and_handshake(address: SocketAddr) -> (TcpStream, FrameCodec) {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let plain = FrameCodec::plain().expect("plain codec");
    let frame = plain
        .encode(
            &FrameHeader::new(opcode::HELLO, 1),
            &hello().encode_to_vec(),
        )
        .expect("hello frame");
    stream.write_all(&frame).await.expect("write hello");
    let mut header_bytes = [0_u8; HEADER_LEN];
    stream
        .read_exact(&mut header_bytes)
        .await
        .expect("welcome header");
    let header = FrameHeader::decode(&header_bytes).expect("welcome header");
    assert_eq!(header.opcode(), opcode::WELCOME);
    let mut payload = vec![0_u8; header.payload_len() as usize];
    stream
        .read_exact(&mut payload)
        .await
        .expect("welcome payload");
    let payload = plain.decode(&header, &payload).expect("welcome payload");
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

async fn ping(stream: &mut TcpStream, codec: &FrameCodec, request_id: u64) {
    let request = PingRequest {
        payload: b"ping".to_vec(),
    };
    let frame = codec
        .encode(
            &FrameHeader::new(opcode::PING, request_id),
            &request.encode_to_vec(),
        )
        .expect("ping frame");
    stream.write_all(&frame).await.expect("write ping");
    let mut header_bytes = [0_u8; HEADER_LEN];
    stream
        .read_exact(&mut header_bytes)
        .await
        .expect("pong header");
    let header = FrameHeader::decode(&header_bytes).expect("pong header");
    let mut payload = vec![0_u8; header.payload_len() as usize];
    stream.read_exact(&mut payload).await.expect("pong payload");
    let payload = codec.decode(&header, &payload).expect("pong payload");
    let response = PingResponse::decode(payload.as_slice()).expect("pong");
    assert_eq!(header.opcode(), opcode::PONG);
    assert_eq!(response.payload, b"ping");
}

async fn expect_closed(stream: &mut TcpStream) {
    let mut buffer = [0_u8; 1];
    let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buffer))
        .await
        .expect("connection did not close");
    match result {
        Ok(0) => {}
        Ok(length) => panic!("server sent {length} unexpected bytes"),
        Err(error) => assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ),
            "unexpected error: {error}"
        ),
    }
}

#[tokio::test]
async fn oversized_frame_header_closes_connection() {
    let address = start_server().await;
    let (mut stream, _) = connect_and_handshake(address).await;
    let mut header = [0_u8; HEADER_LEN];
    header[0..4].copy_from_slice(b"RCLT");
    header[4] = 1;
    header[8..10].copy_from_slice(&opcode::PING.to_le_bytes());
    header[12..14].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    header[14..22].copy_from_slice(&2_u64.to_le_bytes());
    header[26..30].copy_from_slice(&(32_u32 * 1024 * 1024).to_le_bytes());
    let checksum = crc32c(&header);
    header[30..34].copy_from_slice(&checksum.to_le_bytes());
    stream.write_all(&header).await.expect("write header");
    expect_closed(&mut stream).await;

    let (mut stream, codec) = connect_and_handshake(address).await;
    ping(&mut stream, &codec, 3).await;
}

#[tokio::test]
async fn corrupt_payload_closes_connection() {
    let address = start_server().await;
    let (mut stream, codec) = connect_and_handshake(address).await;
    let request = PingRequest {
        payload: b"ping".to_vec(),
    };
    let mut frame = codec
        .encode(&FrameHeader::new(opcode::PING, 2), &request.encode_to_vec())
        .expect("frame");
    *frame.last_mut().expect("payload") ^= 0xFF;
    stream.write_all(&frame).await.expect("write frame");
    expect_closed(&mut stream).await;

    let (mut stream, codec) = connect_and_handshake(address).await;
    ping(&mut stream, &codec, 3).await;
}

#[tokio::test]
async fn truncated_frame_does_not_break_server() {
    let address = start_server().await;
    let (mut stream, _) = connect_and_handshake(address).await;
    stream.write_all(b"RCLT").await.expect("partial");
    drop(stream);

    let (mut stream, codec) = connect_and_handshake(address).await;
    ping(&mut stream, &codec, 3).await;
}

#[tokio::test]
async fn malformed_batch_returns_error_and_keeps_session() {
    let address = start_server().await;
    let (mut stream, codec) = connect_and_handshake(address).await;
    let frame = codec
        .encode(&FrameHeader::new(opcode::BATCH, 4), &[2, 0])
        .expect("batch frame");
    stream.write_all(&frame).await.expect("write batch");
    let mut header_bytes = [0_u8; HEADER_LEN];
    stream
        .read_exact(&mut header_bytes)
        .await
        .expect("error header");
    let header = FrameHeader::decode(&header_bytes).expect("error header");
    assert_eq!(header.opcode(), opcode::ERROR);
    assert_eq!(header.status(), status::INVALID_REQUEST);
    let mut payload = vec![0_u8; header.payload_len() as usize];
    stream
        .read_exact(&mut payload)
        .await
        .expect("error payload");
    let payload = codec.decode(&header, &payload).expect("error payload");
    ErrorResponse::decode(payload.as_slice()).expect("error response");

    ping(&mut stream, &codec, 5).await;
}

#[tokio::test]
async fn batch_reports_partial_failures_without_closing() {
    let address = start_server().await;
    let (mut stream, codec) = connect_and_handshake(address).await;
    let get = DocumentGetRequest {
        meta: None,
        collection_id: 99,
        partition: 0,
        document_key: vec![0],
        fragments: Vec::new(),
        projection: Vec::new(),
    }
    .encode_to_vec();
    let ping_payload = PingRequest {
        payload: b"batch".to_vec(),
    }
    .encode_to_vec();
    let body = encode_batch(
        &[
            SubFrame::new(opcode::PING, 10, &ping_payload),
            SubFrame::new(opcode::DOC_GET, 11, &get),
        ],
        BatchLimits::default(),
    )
    .expect("batch");
    let frame = codec
        .encode(&FrameHeader::new(opcode::BATCH, 9), &body)
        .expect("batch frame");
    stream.write_all(&frame).await.expect("write batch");

    let mut statuses = Vec::new();
    for _ in 0..2 {
        let mut header_bytes = [0_u8; HEADER_LEN];
        stream
            .read_exact(&mut header_bytes)
            .await
            .expect("response header");
        let header = FrameHeader::decode(&header_bytes).expect("response header");
        let mut payload = vec![0_u8; header.payload_len() as usize];
        stream
            .read_exact(&mut payload)
            .await
            .expect("response payload");
        codec.decode(&header, &payload).expect("response payload");
        statuses.push((header.opcode(), header.status()));
    }

    assert!(statuses.contains(&(opcode::PONG, status::OK)));
    assert!(statuses.contains(&(opcode::ERROR, status::NOT_FOUND)));

    ping(&mut stream, &codec, 12).await;
}
