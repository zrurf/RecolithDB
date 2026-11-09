use std::path::PathBuf;
use std::time::Duration;

use recolith_protocol::compression::CompressionAlgorithm;

/// TLS settings for a client connection.
#[derive(Clone, Debug)]
pub struct TlsClientConfig {
    /// Name presented to the server and verified against its certificate.
    pub server_name: String,
    /// PEM certificate authority used to verify the server.
    pub ca_certificates: PathBuf,
}

/// Client connection and protocol settings.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// `host:port` endpoint.
    pub endpoint: String,
    /// Client identifier reported during the handshake.
    pub client_id: String,
    /// Client version reported during the handshake.
    pub client_version: String,
    /// Optional shared bearer token.
    pub auth_token: Option<Vec<u8>>,
    /// Optional TLS configuration. Plaintext is used when absent.
    pub tls: Option<TlsClientConfig>,
    /// Compression algorithms offered to the server, in preference order.
    pub compression: Vec<CompressionAlgorithm>,
    /// Minimum payload size that the client wants compressed.
    pub compression_threshold: usize,
    /// Maximum frame payload size.
    pub max_frame_size: usize,
    /// Maximum number of subframes in a batch.
    pub max_batch_subframes: usize,
    /// Maximum batch payload size.
    pub max_batch_bytes: usize,
    /// Maximum decompressed payload size.
    pub max_decompressed_size: usize,
    /// Connection timeout.
    pub connect_timeout: Option<Duration>,
    /// Timeout for a complete request and response exchange.
    pub request_timeout: Option<Duration>,
}

impl ClientConfig {
    /// Creates a plaintext client configuration.
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client_id: "recolith-client".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            auth_token: None,
            tls: None,
            compression: vec![CompressionAlgorithm::Zstd, CompressionAlgorithm::Lz4],
            compression_threshold: 256,
            max_frame_size: 16 * 1024 * 1024,
            max_batch_subframes: 64,
            max_batch_bytes: 1024 * 1024,
            max_decompressed_size: 16 * 1024 * 1024,
            connect_timeout: Some(Duration::from_secs(10)),
            request_timeout: Some(Duration::from_secs(30)),
        }
    }

    /// Sets the shared bearer token.
    #[must_use]
    pub fn with_auth_token(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Enables TLS 1.3 using the given server name and certificate authority.
    #[must_use]
    pub fn with_tls(
        mut self,
        server_name: impl Into<String>,
        ca_certificates: impl Into<PathBuf>,
    ) -> Self {
        self.tls = Some(TlsClientConfig {
            server_name: server_name.into(),
            ca_certificates: ca_certificates.into(),
        });
        self
    }

    /// Replaces the offered compression algorithms.
    #[must_use]
    pub fn with_compression(
        mut self,
        algorithms: impl IntoIterator<Item = CompressionAlgorithm>,
    ) -> Self {
        self.compression = algorithms.into_iter().collect();
        self
    }

    /// Replaces the compression threshold.
    #[must_use]
    pub fn with_compression_threshold(mut self, threshold: usize) -> Self {
        self.compression_threshold = threshold;
        self
    }

    /// Sets the connection timeout.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the request timeout.
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.request_timeout = timeout;
        self
    }
}
