use std::fmt;
use std::future::Future;
use std::time::Duration;

use prost::Message;
use recolith_protocol::batch::{BatchLimits, SubFrame, encode_batch};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::error::ProtocolError;
use recolith_protocol::frame::{FrameCodec, FrameHeader, HEADER_LEN, VERSION_MAJOR, VERSION_MINOR};
use recolith_protocol::messages::{
    AggregateRequest, AggregateResponse, AuthMethod, AuthResult, BackupRequest, BackupResponse,
    CatalogAlterRequest, CatalogAlterResponse, CatalogApplyRequest, CatalogApplyResponse,
    CatalogDropRequest, CatalogDropResponse, DescribeCollectionRequest, DescribeCollectionResponse,
    DocumentGetRequest, DocumentGetResponse, DocumentPutRequest, DocumentPutResponse,
    ErrorResponse, ExplainRequest, ExplainResponse, GoAway, HelloRequest, PingRequest,
    PingResponse, QueryChunk, QueryRequest, QueryResponse, QueryStreamRequest, SecurityMode,
    WelcomeResponse, compression_bit, encoding_bit, feature, opcode, status,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::batch::{BatchOperation, BatchResult};
use crate::config::ClientConfig;
use crate::error::ClientError;
use crate::tls;

trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Negotiated client session.
pub struct Client {
    stream: Box<dyn AsyncStream>,
    codec: FrameCodec,
    welcome: WelcomeResponse,
    max_frame_size: usize,
    batch_limits: BatchLimits,
    request_timeout: Option<Duration>,
    next_request_id: u64,
    closed: bool,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("session_id", &self.welcome.session_id)
            .field("compression", &self.codec.compression())
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Connects, negotiates the session, and authenticates.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] for transport, TLS, protocol, or authentication
    /// failures.
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        let tls_setup = config.tls.as_ref().map(tls::setup).transpose()?;
        let has_tls = tls_setup.is_some();

        let tcp = with_timeout(config.connect_timeout, "connect", async {
            TcpStream::connect(config.endpoint.as_str())
                .await
                .map_err(ClientError::from)
        })
        .await?;
        tcp.set_nodelay(true)?;

        let mut stream: Box<dyn AsyncStream> = match tls_setup {
            Some(setup) => Box::new(
                with_timeout(config.connect_timeout, "TLS handshake", async {
                    setup
                        .connector
                        .connect(setup.server_name, tcp)
                        .await
                        .map_err(|error| ClientError::Tls(error.to_string()))
                })
                .await?,
            ),
            None => Box::new(tcp),
        };

        let plain = FrameCodec::new(
            CompressionAlgorithm::None,
            0,
            config.max_frame_size,
            config.max_decompressed_size,
        )?;
        let hello = build_hello(&config);
        let hello_frame =
            plain.encode(&FrameHeader::new(opcode::HELLO, 1), &hello.encode_to_vec())?;

        let (header, payload) = with_timeout(config.connect_timeout, "handshake", async {
            stream.write_all(&hello_frame).await?;
            stream.flush().await?;
            read_frame(&mut stream, &plain, config.max_frame_size).await
        })
        .await?;

        if header.request_id() != 1 {
            return Err(ClientError::UnexpectedResponse(format!(
                "handshake response used request ID {}",
                header.request_id()
            )));
        }
        if header.opcode() == opcode::ERROR {
            return Err(decode_server_error(header.status(), &payload));
        }
        if header.opcode() != opcode::WELCOME {
            return Err(ClientError::UnexpectedOpcode {
                expected: opcode::WELCOME,
                actual: header.opcode(),
            });
        }
        if !header.is_response() {
            return Err(ClientError::UnexpectedResponse(
                "handshake response flag is not set".to_owned(),
            ));
        }
        if header.status() != status::OK {
            return Err(ClientError::Server {
                status: header.status(),
                message: "handshake failed".to_owned(),
                retryable: false,
            });
        }

        let welcome = WelcomeResponse::decode(payload.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed WELCOME payload".to_owned()))?;
        if welcome.auth_result != AuthResult::Ok as i32 {
            return Err(ClientError::Server {
                status: status::AUTH_FAILED,
                message: "authentication failed".to_owned(),
                retryable: false,
            });
        }
        if welcome.protocol_major != u32::from(VERSION_MAJOR)
            || welcome.protocol_minor > u32::from(VERSION_MINOR)
        {
            return Err(ClientError::UnexpectedResponse(format!(
                "unsupported protocol version {}.{}",
                welcome.protocol_major, welcome.protocol_minor
            )));
        }
        if welcome.selected_encoding != encoding_bit::PROTOBUF {
            return Err(ClientError::UnexpectedResponse(
                "server selected an unsupported payload encoding".to_owned(),
            ));
        }

        let expected_security = if has_tls {
            SecurityMode::Tls13
        } else {
            SecurityMode::None
        };
        if welcome.security != expected_security as i32 {
            return Err(ClientError::UnexpectedResponse(format!(
                "server reported security mode {}, expected {}",
                welcome.security, expected_security as i32
            )));
        }

        let compression = u8::try_from(welcome.compression_algorithm)
            .ok()
            .and_then(CompressionAlgorithm::from_wire)
            .ok_or_else(|| {
                ClientError::UnexpectedResponse(format!(
                    "server selected unknown compression algorithm {}",
                    welcome.compression_algorithm
                ))
            })?;
        if compression != CompressionAlgorithm::None && !config.compression.contains(&compression) {
            return Err(ClientError::UnexpectedResponse(format!(
                "server selected compression algorithm {}",
                welcome.compression_algorithm
            )));
        }

        let max_frame_size = negotiated_size(welcome.max_frame_size, config.max_frame_size);
        let max_decompressed_size =
            negotiated_limit(welcome.max_decompressed_size, config.max_decompressed_size);
        let max_batch_subframes =
            negotiated_limit(welcome.max_batch_subframes, config.max_batch_subframes);
        let max_batch_bytes = negotiated_limit(welcome.max_batch_bytes, config.max_batch_bytes);
        let threshold = usize::try_from(welcome.compression_threshold).unwrap_or(usize::MAX);
        let codec = FrameCodec::new(
            compression,
            threshold,
            max_frame_size,
            max_decompressed_size,
        )?;

        Ok(Self {
            stream,
            codec,
            welcome,
            max_frame_size,
            batch_limits: BatchLimits {
                max_subframes: max_batch_subframes,
                max_bytes: max_batch_bytes,
            },
            request_timeout: config.request_timeout,
            next_request_id: 2,
            closed: false,
        })
    }

    /// Returns the negotiated server greeting.
    #[must_use]
    pub fn welcome(&self) -> &WelcomeResponse {
        &self.welcome
    }

    /// Returns the negotiated compression algorithm.
    #[must_use]
    pub const fn compression(&self) -> CompressionAlgorithm {
        self.codec.compression()
    }

    /// Returns whether the client session was closed explicitly.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Sends a ping and returns the echoed payload.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn ping(&mut self, payload: impl Into<Vec<u8>>) -> Result<Vec<u8>, ClientError> {
        let request = PingRequest {
            payload: payload.into(),
        };
        let body = self
            .request(opcode::PING, opcode::PONG, &request.encode_to_vec())
            .await?;
        let response = PingResponse::decode(body.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed PONG payload".to_owned()))?;

        Ok(response.payload)
    }

    /// Reads document fragments.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn document_get(
        &mut self,
        request: &DocumentGetRequest,
    ) -> Result<DocumentGetResponse, ClientError> {
        self.require_feature(feature::DOCUMENT_READ, "document reads")?;
        let body = self
            .request(opcode::DOC_GET, opcode::DOC_GET, &request.encode_to_vec())
            .await?;
        DocumentGetResponse::decode(body.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed DOC_GET response".to_owned()))
    }

    /// Writes document fragments.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn document_put(
        &mut self,
        request: &DocumentPutRequest,
    ) -> Result<DocumentPutResponse, ClientError> {
        self.require_feature(feature::DOCUMENT_WRITE, "document writes")?;
        let body = self
            .request(opcode::DOC_PUT, opcode::DOC_PUT, &request.encode_to_vec())
            .await?;
        DocumentPutResponse::decode(body.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed DOC_PUT response".to_owned()))
    }

    /// Executes an RQL query.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn query(&mut self, request: &QueryRequest) -> Result<QueryResponse, ClientError> {
        self.require_feature(feature::QUERY, "queries")?;
        let body = self
            .request(opcode::QUERY, opcode::QUERY, &request.encode_to_vec())
            .await?;
        QueryResponse::decode(body.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed QUERY response".to_owned()))
    }

    /// Writes a consistent logical backup on the server.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn backup(&mut self, request: &BackupRequest) -> Result<BackupResponse, ClientError> {
        self.require_feature(feature::BACKUP, "online backups")?;
        let body = self
            .request(opcode::BACKUP, opcode::BACKUP, &request.encode_to_vec())
            .await?;
        BackupResponse::decode(body.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed BACKUP response".to_owned()))
    }

    /// Executes an aggregate query.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn aggregate(
        &mut self,
        request: &AggregateRequest,
    ) -> Result<AggregateResponse, ClientError> {
        self.require_feature(feature::AGGREGATE, "aggregate queries")?;
        let body = self
            .request(
                opcode::AGGREGATE,
                opcode::AGGREGATE,
                &request.encode_to_vec(),
            )
            .await?;
        AggregateResponse::decode(body.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed AGGREGATE response".to_owned()))
    }

    /// Explains how a query would be planned without executing it.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn explain(
        &mut self,
        request: &ExplainRequest,
    ) -> Result<ExplainResponse, ClientError> {
        self.require_feature(feature::EXPLAIN, "query explanations")?;
        let body = self
            .request(opcode::EXPLAIN, opcode::EXPLAIN, &request.encode_to_vec())
            .await?;
        ExplainResponse::decode(body.as_slice())
            .map_err(|_| ClientError::UnexpectedResponse("malformed EXPLAIN response".to_owned()))
    }

    /// Describes a collection by name or identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn describe_collection(
        &mut self,
        request: &DescribeCollectionRequest,
    ) -> Result<DescribeCollectionResponse, ClientError> {
        self.require_feature(feature::CATALOG_DESCRIBE, "collection metadata")?;
        let body = self
            .request(
                opcode::DESCRIBE_COLLECTION,
                opcode::DESCRIBE_COLLECTION,
                &request.encode_to_vec(),
            )
            .await?;
        DescribeCollectionResponse::decode(body.as_slice()).map_err(|_| {
            ClientError::UnexpectedResponse("malformed DESCRIBE_COLLECTION response".to_owned())
        })
    }

    /// Applies additive catalog definitions atomically.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn apply_catalog(
        &mut self,
        request: &CatalogApplyRequest,
    ) -> Result<CatalogApplyResponse, ClientError> {
        self.require_feature(feature::CATALOG_WRITE, "catalog changes")?;
        let body = self
            .request(
                opcode::CATALOG_APPLY,
                opcode::CATALOG_APPLY,
                &request.encode_to_vec(),
            )
            .await?;
        CatalogApplyResponse::decode(body.as_slice()).map_err(|_| {
            ClientError::UnexpectedResponse("malformed CATALOG_APPLY response".to_owned())
        })
    }

    /// Removes one catalog object.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn drop_catalog(
        &mut self,
        request: &CatalogDropRequest,
    ) -> Result<CatalogDropResponse, ClientError> {
        self.require_feature(feature::CATALOG_WRITE, "catalog changes")?;
        let body = self
            .request(
                opcode::CATALOG_DROP,
                opcode::CATALOG_DROP,
                &request.encode_to_vec(),
            )
            .await?;
        CatalogDropResponse::decode(body.as_slice()).map_err(|_| {
            ClientError::UnexpectedResponse("malformed CATALOG_DROP response".to_owned())
        })
    }

    /// Alters one catalog object.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn alter_catalog(
        &mut self,
        request: &CatalogAlterRequest,
    ) -> Result<CatalogAlterResponse, ClientError> {
        self.require_feature(feature::CATALOG_WRITE, "catalog changes")?;
        let body = self
            .request(
                opcode::CATALOG_ALTER,
                opcode::CATALOG_ALTER,
                &request.encode_to_vec(),
            )
            .await?;
        CatalogAlterResponse::decode(body.as_slice()).map_err(|_| {
            ClientError::UnexpectedResponse("malformed CATALOG_ALTER response".to_owned())
        })
    }

    /// Executes a query and streams result chunks to a callback.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the exchange fails.
    pub async fn query_stream<F>(
        &mut self,
        request: &QueryStreamRequest,
        mut on_chunk: F,
    ) -> Result<(), ClientError>
    where
        F: FnMut(&QueryChunk),
    {
        self.require_feature(feature::STREAMING, "streaming queries")?;
        if self.closed {
            return Err(ClientError::Closed);
        }

        let request_id = self.allocate_request_id();
        let frame = self.codec.encode(
            &FrameHeader::new(opcode::QUERY_STREAM, request_id),
            &request.encode_to_vec(),
        )?;
        let timeout = self.request_timeout;

        with_timeout(timeout, "streaming query", async {
            self.stream.write_all(&frame).await?;
            self.stream.flush().await?;
            let mut expected_stream = 0_u32;
            loop {
                let (header, payload) =
                    read_frame(&mut self.stream, &self.codec, self.max_frame_size).await?;
                if header.request_id() != request_id {
                    return Err(ClientError::UnexpectedResponse(
                        "stream request id mismatch".to_owned(),
                    ));
                }
                if header.opcode() == opcode::ERROR {
                    let error = ErrorResponse::decode(payload.as_slice()).map_err(|_| {
                        ClientError::UnexpectedResponse("malformed ERROR payload".to_owned())
                    })?;
                    return Err(ClientError::Server {
                        status: header.status(),
                        message: error.message,
                        retryable: error.retryable,
                    });
                }
                if header.opcode() != opcode::QUERY_CHUNK {
                    return Err(ClientError::UnexpectedOpcode {
                        expected: opcode::QUERY_CHUNK,
                        actual: header.opcode(),
                    });
                }
                if header.stream_id() != expected_stream {
                    return Err(ClientError::UnexpectedResponse(
                        "stream chunk is out of order".to_owned(),
                    ));
                }
                let chunk = QueryChunk::decode(payload.as_slice()).map_err(|_| {
                    ClientError::UnexpectedResponse("malformed QUERY_CHUNK payload".to_owned())
                })?;
                let final_chunk = chunk.r#final;
                on_chunk(&chunk);
                if final_chunk {
                    return Ok(());
                }
                expected_stream = expected_stream.wrapping_add(1);
            }
        })
        .await
    }

    /// Sends unary requests in one batch frame and returns their responses.
    ///
    /// Failed operations are returned as results with [`opcode::ERROR`] rather
    /// than failing the batch.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the batch frame itself fails.
    pub async fn batch(
        &mut self,
        operations: &[BatchOperation],
    ) -> Result<Vec<BatchResult>, ClientError> {
        self.require_feature(feature::BATCH, "batched requests")?;
        if self.closed {
            return Err(ClientError::Closed);
        }
        if operations.is_empty() {
            return Err(ProtocolError::EmptyBatch.into());
        }

        let batch_id = self.allocate_request_id();
        let mut request_ids = Vec::with_capacity(operations.len());
        for _ in operations {
            request_ids.push(self.allocate_request_id());
        }
        let subframes = operations
            .iter()
            .zip(&request_ids)
            .map(|(operation, request_id)| {
                SubFrame::new(operation.opcode, *request_id, &operation.payload)
            })
            .collect::<Vec<_>>();
        let body = encode_batch(&subframes, self.batch_limits)?;
        let frame = self
            .codec
            .encode(&FrameHeader::new(opcode::BATCH, batch_id), &body)?;
        let timeout = self.request_timeout;
        let mut results = vec![None; operations.len()];

        with_timeout(timeout, "batch", async {
            self.stream.write_all(&frame).await?;
            self.stream.flush().await?;
            for _ in 0..operations.len() {
                let (header, payload) =
                    read_frame(&mut self.stream, &self.codec, self.max_frame_size).await?;
                if header.request_id() == batch_id {
                    if header.opcode() == opcode::ERROR {
                        return Err(decode_server_error(header.status(), &payload));
                    }
                    return Err(ClientError::UnexpectedResponse(
                        "unexpected response to the batch frame".to_owned(),
                    ));
                }
                let index = request_ids
                    .iter()
                    .position(|request_id| *request_id == header.request_id())
                    .ok_or_else(|| {
                        ClientError::UnexpectedResponse(format!(
                            "response used unknown request ID {}",
                            header.request_id()
                        ))
                    })?;
                if results[index].is_some() {
                    return Err(ClientError::UnexpectedResponse(format!(
                        "duplicate response for request ID {}",
                        header.request_id()
                    )));
                }
                results[index] = Some(BatchResult {
                    request_id: header.request_id(),
                    opcode: header.opcode(),
                    status: header.status(),
                    payload,
                });
            }

            Ok(())
        })
        .await?;

        results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.ok_or_else(|| {
                    ClientError::UnexpectedResponse(format!(
                        "missing response for batch operation {index}"
                    ))
                })
            })
            .collect()
    }

    /// Sends GOAWAY and closes the session.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the shutdown exchange fails.
    pub async fn close(&mut self) -> Result<(), ClientError> {
        if self.closed {
            return Ok(());
        }

        let request = GoAway {
            message: "client closing session".to_owned(),
            retry_after_millis: 0,
        };
        let result = self
            .request(opcode::GOAWAY, opcode::GOAWAY, &request.encode_to_vec())
            .await;
        self.closed = true;

        result.map(|_| ())
    }

    async fn request(
        &mut self,
        request_opcode: u16,
        response_opcode: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, ClientError> {
        if self.closed {
            return Err(ClientError::Closed);
        }

        let request_id = self.allocate_request_id();
        let frame = self
            .codec
            .encode(&FrameHeader::new(request_opcode, request_id), payload)?;
        let timeout = self.request_timeout;

        with_timeout(timeout, "request", async {
            self.stream.write_all(&frame).await?;
            self.stream.flush().await?;
            self.receive(request_id, response_opcode).await
        })
        .await
    }

    async fn receive(
        &mut self,
        request_id: u64,
        expected_opcode: u16,
    ) -> Result<Vec<u8>, ClientError> {
        let (header, payload) =
            read_frame(&mut self.stream, &self.codec, self.max_frame_size).await?;
        if header.request_id() != request_id {
            return Err(ClientError::UnexpectedResponse(format!(
                "response used request ID {}, expected {request_id}",
                header.request_id()
            )));
        }
        if header.opcode() == opcode::ERROR {
            return Err(decode_server_error(header.status(), &payload));
        }
        if !header.is_response() {
            return Err(ClientError::UnexpectedResponse(
                "response flag is not set".to_owned(),
            ));
        }
        if header.opcode() != expected_opcode {
            return Err(ClientError::UnexpectedOpcode {
                expected: expected_opcode,
                actual: header.opcode(),
            });
        }
        if header.status() != status::OK {
            return Err(ClientError::Server {
                status: header.status(),
                message: "request failed".to_owned(),
                retryable: false,
            });
        }

        Ok(payload)
    }

    fn require_feature(&self, bit: u64, name: &'static str) -> Result<(), ClientError> {
        if self.welcome.features & bit == 0 {
            return Err(ClientError::UnsupportedFeature(name));
        }

        Ok(())
    }

    fn allocate_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        if self.next_request_id == 0 {
            self.next_request_id = 1;
        }

        request_id
    }
}

async fn with_timeout<F, T>(
    timeout: Option<Duration>,
    operation: &'static str,
    future: F,
) -> Result<T, ClientError>
where
    F: Future<Output = Result<T, ClientError>>,
{
    match timeout {
        Some(duration) => tokio::time::timeout(duration, future)
            .await
            .map_err(|_| ClientError::Timeout(operation))?,
        None => future.await,
    }
}

async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    codec: &FrameCodec,
    max_frame_size: usize,
) -> Result<(FrameHeader, Vec<u8>), ClientError> {
    let mut header_bytes = [0_u8; HEADER_LEN];
    stream.read_exact(&mut header_bytes).await?;
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
    stream.read_exact(&mut payload).await?;
    let payload = codec.decode(&header, &payload)?;

    Ok((header, payload))
}

fn build_hello(config: &ClientConfig) -> HelloRequest {
    let (auth_method, auth_token) = match &config.auth_token {
        Some(token) => (AuthMethod::Bearer as i32, token.clone()),
        None => (AuthMethod::None as i32, Vec::new()),
    };

    HelloRequest {
        protocol_min_major: u32::from(VERSION_MAJOR),
        protocol_min_minor: u32::from(VERSION_MINOR),
        protocol_max_major: u32::from(VERSION_MAJOR),
        protocol_max_minor: u32::from(VERSION_MINOR),
        client_id: config.client_id.clone(),
        client_version: config.client_version.clone(),
        auth_method,
        auth_token,
        features: feature::BATCH
            | feature::DOCUMENT_READ
            | feature::DOCUMENT_WRITE
            | feature::QUERY
            | feature::IDEMPOTENCY
            | feature::CATALOG_DESCRIBE
            | feature::EXPLAIN
            | feature::AGGREGATE
            | feature::BACKUP,
        max_frame_size: to_u32(config.max_frame_size),
        max_batch_subframes: to_u32(config.max_batch_subframes),
        max_batch_bytes: to_u32(config.max_batch_bytes),
        compression_algorithms: compression_bits(&config.compression),
        compression_threshold: to_u32(config.compression_threshold),
        encodings: encoding_bit::PROTOBUF,
    }
}

fn compression_bits(algorithms: &[CompressionAlgorithm]) -> u32 {
    algorithms.iter().fold(0, |bits, algorithm| {
        bits | match algorithm {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Zstd => compression_bit::ZSTD,
            CompressionAlgorithm::Lz4 => compression_bit::LZ4,
        }
    })
}

fn negotiated_size(value: u32, configured: usize) -> usize {
    negotiated_limit(value, configured).max(HEADER_LEN)
}

fn negotiated_limit(value: u32, configured: usize) -> usize {
    let requested = if value == 0 {
        configured
    } else {
        usize::try_from(value).unwrap_or(usize::MAX)
    };

    requested.min(configured).max(1)
}

fn to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn decode_server_error(status: u16, payload: &[u8]) -> ClientError {
    match ErrorResponse::decode(payload) {
        Ok(error) => ClientError::Server {
            status,
            message: error.message,
            retryable: error.retryable,
        },
        Err(_) => ClientError::UnexpectedResponse("malformed ERROR payload".to_owned()),
    }
}
