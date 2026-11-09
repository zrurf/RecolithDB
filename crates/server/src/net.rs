//! TCP and TLS server runtime and session handling.

mod convert;
mod operations;
mod session;
mod tls;

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use recolith_backend::StorageBackend;
use recolith_protocol::messages::SecurityMode;
use tokio::net::TcpListener;

use crate::document::DocumentStore;

/// Server version reported during the handshake.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// TLS certificate and key paths.
#[derive(Clone, Debug)]
pub struct TlsServerConfig {
    /// PEM certificate chain.
    pub certificate_chain: PathBuf,
    /// PEM private key.
    pub private_key: PathBuf,
}

/// TCP server configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Address the caller binds to before calling [`serve`].
    pub listen: SocketAddr,
    /// Optional shared bearer token. When absent, plaintext is limited to loopback.
    pub auth_token: Option<Vec<u8>>,
    /// Allows plaintext non-loopback sessions. Intended only for development.
    pub allow_plaintext_remote: bool,
    /// Optional TLS 1.3 listener configuration.
    pub tls: Option<TlsServerConfig>,
    /// Maximum frame payload size.
    pub max_frame_size: usize,
    /// Maximum number of subframes in a batch.
    pub max_batch_subframes: usize,
    /// Maximum batch payload size.
    pub max_batch_bytes: usize,
    /// Maximum decompressed payload size.
    pub max_decompressed_size: usize,
    /// Server compression threshold.
    pub compression_threshold: usize,
}

impl ServerConfig {
    /// Creates a loopback-safe plaintext configuration.
    #[must_use]
    pub fn new(listen: SocketAddr) -> Self {
        Self {
            listen,
            auth_token: None,
            allow_plaintext_remote: false,
            tls: None,
            max_frame_size: 16 * 1024 * 1024,
            max_batch_subframes: 64,
            max_batch_bytes: 1024 * 1024,
            max_decompressed_size: 16 * 1024 * 1024,
            compression_threshold: 512,
        }
    }

    /// Sets the shared bearer token.
    #[must_use]
    pub fn with_auth_token(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Enables TLS 1.3 using a PEM certificate chain and private key.
    #[must_use]
    pub fn with_tls(
        mut self,
        certificate_chain: impl Into<PathBuf>,
        private_key: impl Into<PathBuf>,
    ) -> Self {
        self.tls = Some(TlsServerConfig {
            certificate_chain: certificate_chain.into(),
            private_key: private_key.into(),
        });
        self
    }
}

/// Accepts TCP connections until the listener fails.
///
/// # Errors
///
/// Returns a listener or TLS configuration I/O error. Per-session errors are
/// contained by their tasks.
pub async fn serve<B: StorageBackend>(
    listener: TcpListener,
    config: ServerConfig,
    store: Arc<DocumentStore<B>>,
) -> io::Result<()> {
    let tls = config.tls.as_ref().map(tls::acceptor).transpose()?;
    let config = Arc::new(config);

    loop {
        let (stream, peer) = listener.accept().await?;
        let config = Arc::clone(&config);
        let store = Arc::clone(&store);
        let tls = tls.clone();
        tokio::spawn(async move {
            if let Some(acceptor) = tls {
                if let Ok(stream) = acceptor.accept(stream).await {
                    let _ = session::run(stream, peer, config, store, SecurityMode::Tls13).await;
                }
            } else {
                let _ = session::run(stream, peer, config, store, SecurityMode::None).await;
            }
        });
    }
}
