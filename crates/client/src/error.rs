use std::fmt;

use recolith_protocol::error::ProtocolError;

/// Client connection or request failure.
#[derive(Debug)]
pub enum ClientError {
    /// Transport failure.
    Io(std::io::Error),
    /// Frame or payload encoding failure.
    Protocol(ProtocolError),
    /// TLS setup or handshake failure.
    Tls(String),
    /// The server rejected the request.
    Server {
        /// Frame status code.
        status: u16,
        /// Server-provided message.
        message: String,
        /// Whether the request may be retried.
        retryable: bool,
    },
    /// A required negotiated feature is unavailable.
    UnsupportedFeature(&'static str),
    /// The response opcode does not match the request.
    UnexpectedOpcode {
        /// Expected opcode.
        expected: u16,
        /// Received opcode.
        actual: u16,
    },
    /// The response payload is malformed.
    UnexpectedResponse(String),
    /// The connection has been closed by the client.
    Closed,
    /// An operation exceeded its configured timeout.
    Timeout(&'static str),
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "client I/O error: {error}"),
            Self::Protocol(error) => write!(formatter, "client protocol error: {error}"),
            Self::Tls(message) => write!(formatter, "client TLS error: {message}"),
            Self::Server {
                status, message, ..
            } => write!(
                formatter,
                "server rejected request with status {status}: {message}"
            ),
            Self::UnsupportedFeature(feature) => {
                write!(formatter, "server does not support {feature}")
            }
            Self::UnexpectedOpcode { expected, actual } => write!(
                formatter,
                "unexpected response opcode {actual:#06x}, expected {expected:#06x}"
            ),
            Self::UnexpectedResponse(message) => {
                write!(formatter, "unexpected response: {message}")
            }
            Self::Closed => formatter.write_str("client connection is closed"),
            Self::Timeout(operation) => write!(formatter, "{operation} timed out"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Tls(_)
            | Self::Server { .. }
            | Self::UnsupportedFeature(_)
            | Self::UnexpectedOpcode { .. }
            | Self::UnexpectedResponse(_)
            | Self::Closed
            | Self::Timeout(_) => None,
        }
    }
}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}
