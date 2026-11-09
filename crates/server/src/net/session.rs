use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;
use recolith_backend::StorageBackend;
use recolith_protocol::batch::{BatchLimits, decode_batch};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::error::ProtocolError;
use recolith_protocol::frame::{
    FrameCodec, FrameHeader, HEADER_LEN, PayloadEncoding, VERSION_MAJOR, VERSION_MINOR,
};
use recolith_protocol::messages::{
    AuthMethod, AuthResult, DocumentGetRequest, DocumentPutRequest, ErrorResponse, GoAway,
    HelloRequest, PingRequest, PingResponse, QueryRequest, SecurityMode, WelcomeResponse,
    compression_bit, feature, opcode, status,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::document::DocumentStore;
use crate::net::{SERVER_VERSION, ServerConfig};

use super::operations::{self, OperationError, OperationResponse};

pub(crate) async fn run<B: StorageBackend, S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut stream: S,
    peer: SocketAddr,
    config: Arc<ServerConfig>,
    store: Arc<DocumentStore<B>>,
    security: SecurityMode,
) -> Result<(), SessionError> {
    let plain = FrameCodec::new(
        CompressionAlgorithm::None,
        0,
        config.max_frame_size,
        config.max_decompressed_size,
    )?;

    let Some((hello_header, hello_payload)) =
        read_frame(&mut stream, config.max_frame_size).await?
    else {
        return Ok(());
    };
    if hello_header.opcode() != opcode::HELLO || hello_header.request_id() == 0 {
        send_error(
            &mut stream,
            &plain,
            hello_header.request_id(),
            status::INVALID_REQUEST,
            "first frame must be a HELLO request with a non-zero request ID",
        )
        .await?;
        return Ok(());
    }

    let hello = match HelloRequest::decode(hello_payload.as_slice()) {
        Ok(hello) => hello,
        Err(_) => {
            send_error(
                &mut stream,
                &plain,
                hello_header.request_id(),
                status::INVALID_REQUEST,
                "malformed HELLO payload",
            )
            .await?;
            return Ok(());
        }
    };
    if !version_supported(&hello) {
        send_error(
            &mut stream,
            &plain,
            hello_header.request_id(),
            status::PROTOCOL_VERSION,
            "no mutually supported protocol version",
        )
        .await?;
        return Ok(());
    }
    if let Err((code, message)) = authenticate(&hello, &config, peer, security) {
        send_error(
            &mut stream,
            &plain,
            hello_header.request_id(),
            code,
            &message,
        )
        .await?;
        return Ok(());
    }

    let algorithm = select_compression(&hello);
    let threshold = select_threshold(&hello, &config);
    let max_frame_size = negotiated_max_frame(&hello, &config);
    let max_batch_subframes =
        negotiated_limit(hello.max_batch_subframes, config.max_batch_subframes);
    let max_batch_bytes = negotiated_limit(hello.max_batch_bytes, config.max_batch_bytes);
    let codec = FrameCodec::new(
        algorithm,
        threshold,
        max_frame_size,
        config.max_decompressed_size,
    )?;

    let welcome = WelcomeResponse {
        protocol_major: u32::from(VERSION_MAJOR),
        protocol_minor: u32::from(VERSION_MINOR),
        server_version: SERVER_VERSION.to_owned(),
        session_id: session_id(),
        auth_result: AuthResult::Ok as i32,
        features: feature::BATCH
            | feature::DOCUMENT_READ
            | feature::DOCUMENT_WRITE
            | feature::QUERY
            | feature::IDEMPOTENCY,
        max_frame_size: u32::try_from(max_frame_size).unwrap_or(u32::MAX),
        max_batch_subframes: u32::try_from(max_batch_subframes).unwrap_or(u32::MAX),
        max_batch_bytes: u32::try_from(max_batch_bytes).unwrap_or(u32::MAX),
        compression_algorithm: u32::from(algorithm.as_wire()),
        compression_threshold: u32::try_from(threshold).unwrap_or(u32::MAX),
        max_decompressed_size: u32::try_from(config.max_decompressed_size).unwrap_or(u32::MAX),
        selected_encoding: recolith_protocol::messages::encoding_bit::PROTOBUF,
        catalog_generation: store.catalog_generation().get(),
        server_time_unix_micros: operations::now_micros(),
        security: security as i32,
    };
    let welcome_header =
        FrameHeader::new(opcode::WELCOME, hello_header.request_id()).as_response(status::OK);
    write_frame(
        &mut stream,
        &plain,
        &welcome_header,
        &welcome.encode_to_vec(),
    )
    .await?;

    let limits = BatchLimits {
        max_subframes: max_batch_subframes,
        max_bytes: max_batch_bytes,
    };
    loop {
        let Some((header, payload)) = read_frame(&mut stream, max_frame_size).await? else {
            break;
        };
        if header.is_response() || header.status() != 0 || header.stream_id() != 0 {
            send_error(
                &mut stream,
                &codec,
                header.request_id(),
                status::INVALID_REQUEST,
                "invalid request frame",
            )
            .await?;
            continue;
        }
        if header.encoding() != Some(PayloadEncoding::Protobuf) {
            send_error(
                &mut stream,
                &codec,
                header.request_id(),
                status::UNSUPPORTED_ENCODING,
                "unsupported payload encoding",
            )
            .await?;
            continue;
        }

        let body = codec.decode(&header, &payload)?;
        match header.opcode() {
            opcode::PING | opcode::DOC_GET | opcode::DOC_PUT | opcode::QUERY => {
                match execute(
                    header.opcode(),
                    body,
                    Arc::clone(&store),
                    hello.client_id.clone(),
                )
                .await
                {
                    Ok(response) => {
                        write_operation(&mut stream, &codec, header.request_id(), response).await?;
                    }
                    Err(error) => {
                        send_error(
                            &mut stream,
                            &codec,
                            header.request_id(),
                            error.status(),
                            error.message(),
                        )
                        .await?;
                    }
                }
            }
            opcode::BATCH => {
                handle_batch(
                    &mut stream,
                    &codec,
                    &header,
                    &body,
                    limits,
                    Arc::clone(&store),
                    hello.client_id.clone(),
                )
                .await?;
            }
            opcode::GOAWAY => {
                let response = GoAway {
                    message: "server closing session".to_owned(),
                    retry_after_millis: 0,
                };
                let response_header =
                    FrameHeader::new(opcode::GOAWAY, header.request_id()).as_response(status::OK);
                write_frame(
                    &mut stream,
                    &codec,
                    &response_header,
                    &response.encode_to_vec(),
                )
                .await?;
                break;
            }
            opcode::HELLO | opcode::WELCOME | opcode::PONG | opcode::ERROR => {
                send_error(
                    &mut stream,
                    &codec,
                    header.request_id(),
                    status::INVALID_REQUEST,
                    "opcode is not valid for a client request",
                )
                .await?;
            }
            _ => {
                send_error(
                    &mut stream,
                    &codec,
                    header.request_id(),
                    status::UNSUPPORTED_OPCODE,
                    "unknown opcode",
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn execute<B: StorageBackend>(
    opcode: u16,
    body: Vec<u8>,
    store: Arc<DocumentStore<B>>,
    client_id: String,
) -> Result<OperationResponse, OperationError> {
    if opcode == opcode::PING {
        let request = PingRequest::decode(body.as_slice())
            .map_err(|_| OperationError::invalid("malformed PING payload"))?;
        let response = PingResponse {
            payload: request.payload,
        };

        return Ok(OperationResponse {
            opcode: opcode::PONG,
            payload: response.encode_to_vec(),
        });
    }

    tokio::task::spawn_blocking(move || match opcode {
        opcode::DOC_GET => {
            let request = DocumentGetRequest::decode(body.as_slice())
                .map_err(|_| OperationError::invalid("malformed DOC_GET payload"))?;
            operations::document_get(&store, request)
        }
        opcode::DOC_PUT => {
            let request = DocumentPutRequest::decode(body.as_slice())
                .map_err(|_| OperationError::invalid("malformed DOC_PUT payload"))?;
            operations::document_put(&store, &client_id, request)
        }
        opcode::QUERY => {
            let request = QueryRequest::decode(body.as_slice())
                .map_err(|_| OperationError::invalid("malformed QUERY payload"))?;
            operations::query(&store, request)
        }
        _ => Err(OperationError::internal("unsupported operation")),
    })
    .await
    .map_err(|_| OperationError::internal("worker task failed"))?
}

async fn write_operation<W: AsyncWrite + Unpin>(
    writer: &mut W,
    codec: &FrameCodec,
    request_id: u64,
    response: OperationResponse,
) -> Result<(), SessionError> {
    let header = FrameHeader::new(response.opcode, request_id).as_response(status::OK);
    write_frame(writer, codec, &header, &response.payload).await
}

async fn handle_batch<B: StorageBackend, W: AsyncWrite + Unpin>(
    writer: &mut W,
    codec: &FrameCodec,
    header: &FrameHeader,
    body: &[u8],
    limits: BatchLimits,
    store: Arc<DocumentStore<B>>,
    client_id: String,
) -> Result<(), SessionError> {
    let subframes = match decode_batch(body, limits) {
        Ok(subframes) => subframes,
        Err(error) => {
            send_error(
                writer,
                codec,
                header.request_id(),
                status::INVALID_REQUEST,
                &error.to_string(),
            )
            .await?;
            return Ok(());
        }
    };

    for subframe in subframes {
        match execute(
            subframe.opcode(),
            subframe.payload().to_vec(),
            Arc::clone(&store),
            client_id.clone(),
        )
        .await
        {
            Ok(response) => {
                write_operation(writer, codec, subframe.request_id(), response).await?;
            }
            Err(error) => {
                send_error(
                    writer,
                    codec,
                    subframe.request_id(),
                    error.status(),
                    error.message(),
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_size: usize,
) -> Result<Option<(FrameHeader, Vec<u8>)>, SessionError> {
    let mut header_bytes = [0_u8; HEADER_LEN];
    if let Err(error) = reader.read_exact(&mut header_bytes).await {
        return match error.kind() {
            std::io::ErrorKind::UnexpectedEof => Ok(None),
            _ => Err(SessionError::Io(error)),
        };
    }
    let header = FrameHeader::decode(&header_bytes)?;
    let payload_len = usize::try_from(header.payload_len()).unwrap_or(usize::MAX);
    if payload_len > max_frame_size {
        return Err(ProtocolError::PayloadTooLarge {
            length: payload_len,
            limit: max_frame_size,
        }
        .into());
    }
    let mut payload = vec![0_u8; payload_len];
    if let Err(error) = reader.read_exact(&mut payload).await {
        return match error.kind() {
            std::io::ErrorKind::UnexpectedEof => Err(SessionError::Closed),
            _ => Err(SessionError::Io(error)),
        };
    }

    Ok(Some((header, payload)))
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    codec: &FrameCodec,
    header: &FrameHeader,
    payload: &[u8],
) -> Result<(), SessionError> {
    let frame = codec.encode(header, payload)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;

    Ok(())
}

async fn send_error<W: AsyncWrite + Unpin>(
    writer: &mut W,
    codec: &FrameCodec,
    request_id: u64,
    code: u16,
    message: &str,
) -> Result<(), SessionError> {
    let payload = ErrorResponse {
        message: message.to_owned(),
        retryable: false,
        retry_after_millis: 0,
        catalog_generation: 0,
        details: Default::default(),
    };
    let header = FrameHeader::new(opcode::ERROR, request_id).as_response(code);
    write_frame(writer, codec, &header, &payload.encode_to_vec()).await
}

fn version_supported(hello: &HelloRequest) -> bool {
    let server = (u32::from(VERSION_MAJOR), u32::from(VERSION_MINOR));
    let min = (hello.protocol_min_major, hello.protocol_min_minor);
    let max = (hello.protocol_max_major, hello.protocol_max_minor);

    (hello.protocol_max_major == 0 && hello.protocol_max_minor == 0)
        || (min <= server && server <= max)
}

fn authenticate(
    hello: &HelloRequest,
    config: &ServerConfig,
    peer: SocketAddr,
    security: SecurityMode,
) -> Result<(), (u16, String)> {
    if security == SecurityMode::None && !peer.ip().is_loopback() && !config.allow_plaintext_remote
    {
        return Err((
            status::AUTH_FAILED,
            "plaintext remote sessions are disabled".to_owned(),
        ));
    }

    match &config.auth_token {
        Some(expected) => {
            if hello.auth_method != AuthMethod::Bearer as i32
                || !constant_time_eq(&hello.auth_token, expected)
            {
                return Err((status::AUTH_FAILED, "authentication failed".to_owned()));
            }
        }
        None => {
            if hello.auth_method != AuthMethod::None as i32 {
                return Err((
                    status::AUTH_FAILED,
                    "server does not accept bearer tokens".to_owned(),
                ));
            }
        }
    }

    Ok(())
}

fn select_compression(hello: &HelloRequest) -> CompressionAlgorithm {
    if hello.compression_algorithms & compression_bit::ZSTD != 0 {
        CompressionAlgorithm::Zstd
    } else if hello.compression_algorithms & compression_bit::LZ4 != 0 {
        CompressionAlgorithm::Lz4
    } else {
        CompressionAlgorithm::None
    }
}

fn select_threshold(hello: &HelloRequest, config: &ServerConfig) -> usize {
    let requested = if hello.compression_threshold == 0 {
        config.compression_threshold
    } else {
        hello.compression_threshold as usize
    };

    requested.clamp(256, 8192)
}

fn negotiated_max_frame(hello: &HelloRequest, config: &ServerConfig) -> usize {
    let requested = if hello.max_frame_size == 0 {
        config.max_frame_size
    } else {
        hello.max_frame_size as usize
    };

    requested.min(config.max_frame_size).max(HEADER_LEN)
}

fn negotiated_limit(requested: u32, limit: usize) -> usize {
    if requested == 0 {
        limit
    } else {
        (requested as usize).min(limit).max(1)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn session_id() -> Vec<u8> {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let mut state = operations::now_micros()
        ^ (u64::from(std::process::id()) << 32)
        ^ COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut output = vec![0_u8; 16];
    for chunk in output.chunks_mut(8) {
        state = splitmix64(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }

    output
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut result = value;
    result = (result ^ (result >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    result = (result ^ (result >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    result ^ (result >> 31)
}

#[derive(Debug)]
pub(crate) enum SessionError {
    Closed,
    Io(std::io::Error),
    Protocol(ProtocolError),
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("session closed"),
            Self::Io(error) => write!(formatter, "session I/O error: {error}"),
            Self::Protocol(error) => write!(formatter, "session protocol error: {error}"),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Closed => None,
        }
    }
}

impl From<std::io::Error> for SessionError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for SessionError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}
