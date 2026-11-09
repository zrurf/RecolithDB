//! Client connection, handshake, and typed document operations.
#![forbid(unsafe_code)]

mod batch;
mod config;
mod connection;
mod error;
mod tls;

pub use batch::{BatchOperation, BatchResult};
pub use config::{ClientConfig, TlsClientConfig};
pub use connection::Client;
pub use error::ClientError;
