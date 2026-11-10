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
use recolith_server::{DocumentStore, ServerConfig, ServerMetrics, serve};
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
    start_server_with_metrics().await.0
}

async fn start_server_with_metrics() -> (SocketAddr, Arc<ServerMetrics>) {
    start_server_with_config(ServerConfig::new("127.0.0.1:0".parse().expect("address"))).await
}

async fn start_server_with_config(mut config: ServerConfig) -> (SocketAddr, Arc<ServerMetrics>) {
    let listener = TcpListener::bind(config.listen).await.expect("bind");
    let address = listener.local_addr().expect("address");
    config.listen = address;
    let metrics = config.metrics();
    tokio::spawn(serve(listener, config, empty_store()));

    (address, metrics)
}

async fn wait_for_idle(metrics: &ServerMetrics) {
    for _ in 0..100 {
        if metrics.snapshot().active_sessions == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("sessions did not drain: {:?}", metrics.snapshot());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sessions_are_counted() {
    let (address, metrics) = start_server_with_metrics().await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        tasks.spawn(async move {
            let (mut stream, codec) = connect_and_handshake(address).await;
            for request_id in 2..52 {
                ping(&mut stream, &codec, request_id).await;
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("client task");
    }
    wait_for_idle(&metrics).await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.accepted_sessions, 16);
    assert_eq!(snapshot.active_sessions, 0);
    assert_eq!(snapshot.pings, 800);
    assert_eq!(snapshot.requests, 800);
    assert_eq!(snapshot.ping_latency.count, 800);
    assert!(snapshot.ping_latency.p99_micros > 0);
    assert_eq!(snapshot.errors, 0);
    assert_eq!(snapshot.session_errors, 0);
}

#[tokio::test]
async fn connection_churn_is_counted() {
    let (address, metrics) = start_server_with_metrics().await;
    for _ in 0..100 {
        let (stream, _) = connect_and_handshake(address).await;
        drop(stream);
    }
    wait_for_idle(&metrics).await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.accepted_sessions, 100);
    assert_eq!(snapshot.active_sessions, 0);
    assert_eq!(snapshot.session_errors, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_client_does_not_block_other_sessions() {
    let (address, metrics) = start_server_with_metrics().await;
    let mut slow = TcpStream::connect(address).await.expect("slow connect");
    slow.write_all(b"R").await.expect("partial header");

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        tasks.spawn(async move {
            let (mut stream, codec) = connect_and_handshake(address).await;
            for request_id in 2..22 {
                ping(&mut stream, &codec, request_id).await;
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("client task");
    }

    let snapshot = metrics.snapshot();
    assert!(snapshot.active_sessions >= 1);
    assert_eq!(snapshot.pings, 160);
    assert_eq!(snapshot.errors, 0);

    drop(slow);
    wait_for_idle(&metrics).await;
    assert_eq!(metrics.snapshot().active_sessions, 0);
}

#[tokio::test]
async fn malformed_batch_counts_an_error_and_keeps_session() {
    let (address, metrics) = start_server_with_metrics().await;
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
    let mut error_payload = vec![0_u8; header.payload_len() as usize];
    stream
        .read_exact(&mut error_payload)
        .await
        .expect("error payload");

    ping(&mut stream, &codec, 5).await;
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.errors, 1);
    assert_eq!(snapshot.requests, 1);
    assert_eq!(snapshot.pings, 1);

    drop(stream);
    wait_for_idle(&metrics).await;
}

#[tokio::test]
async fn corrupt_payload_counts_a_session_error() {
    let (address, metrics) = start_server_with_metrics().await;
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
    wait_for_idle(&metrics).await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.active_sessions, 0);
    assert_eq!(snapshot.session_errors, 1);
    assert_eq!(snapshot.errors, 0);
}

#[tokio::test]
async fn connection_limit_rejects_extra_sessions() {
    let config = ServerConfig::new("127.0.0.1:0".parse().expect("address")).with_max_connections(2);
    let (address, metrics) = start_server_with_config(config).await;
    let (mut first, first_codec) = connect_and_handshake(address).await;
    let (mut second, second_codec) = connect_and_handshake(address).await;

    let mut rejected = TcpStream::connect(address).await.expect("tcp connect");
    expect_closed(&mut rejected).await;

    ping(&mut first, &first_codec, 2).await;
    ping(&mut second, &second_codec, 2).await;

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.accepted_sessions, 2);
    assert_eq!(snapshot.rejected_connections, 1);
    assert_eq!(snapshot.active_sessions, 2);

    drop(first);
    drop(second);
    wait_for_idle(&metrics).await;
    assert_eq!(metrics.snapshot().active_sessions, 0);
}
