use std::fmt;

use recolith_client::ClientError;
use recolith_security::{SecurityError, TokenError};

/// Command-line failure.
#[derive(Debug)]
pub enum CliError {
    /// TLS configuration is incomplete.
    IncompleteTls,
    /// A named parameter does not follow `name=type:value`.
    InvalidParameter(String),
    /// A parameter value cannot be parsed as its declared type.
    InvalidValue {
        /// Declared type.
        kind: &'static str,
        /// Raw value.
        value: String,
    },
    /// A value uses an unknown type name.
    UnknownValueType(String),
    /// A document key is not valid hexadecimal.
    InvalidKey(String),
    /// A field specification is malformed.
    InvalidField(String),
    /// A catalog definition is malformed.
    InvalidCatalog(String),
    /// The command conflicts with collection metadata.
    InvalidMetadata(String),
    /// A write requires a non-empty idempotency key.
    EmptyIdempotencyKey,
    /// The client exchange failed.
    Client(ClientError),
    /// Token issuance failed.
    Token(TokenError),
    /// Authentication file handling failed.
    Security(SecurityError),
    /// Token lifetime is invalid.
    InvalidTtl,
    /// The token input is empty.
    EmptyToken,
    /// Local I/O failed.
    Io(std::io::Error),
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteTls => {
                formatter.write_str("TLS requires both --tls-ca and --tls-server-name")
            }
            Self::InvalidParameter(value) => {
                write!(
                    formatter,
                    "invalid parameter `{value}`; expected name=type:value"
                )
            }
            Self::InvalidValue { kind, value } => {
                write!(formatter, "invalid {kind} value `{value}`")
            }
            Self::UnknownValueType(kind) => {
                write!(formatter, "unknown value type `{kind}`")
            }
            Self::InvalidKey(value) => {
                write!(formatter, "invalid hexadecimal document key `{value}`")
            }
            Self::InvalidField(value) => {
                write!(
                    formatter,
                    "invalid field `{value}`; expected fragment:field=type:value"
                )
            }
            Self::InvalidCatalog(message) => formatter.write_str(message),
            Self::InvalidMetadata(message) => formatter.write_str(message),
            Self::EmptyIdempotencyKey => formatter.write_str("idempotency key must not be empty"),
            Self::Client(error) => error.fmt(formatter),
            Self::Token(error) => error.fmt(formatter),
            Self::Security(error) => error.fmt(formatter),
            Self::InvalidTtl => formatter.write_str("token lifetime overflows the timestamp"),
            Self::EmptyToken => formatter.write_str("token input is empty"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Client(error) => Some(error),
            Self::Token(error) => Some(error),
            Self::Security(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::IncompleteTls
            | Self::InvalidParameter(_)
            | Self::InvalidValue { .. }
            | Self::UnknownValueType(_)
            | Self::InvalidKey(_)
            | Self::InvalidField(_)
            | Self::InvalidCatalog(_)
            | Self::InvalidMetadata(_)
            | Self::EmptyIdempotencyKey
            | Self::InvalidTtl
            | Self::EmptyToken => None,
        }
    }
}

impl From<ClientError> for CliError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<TokenError> for CliError {
    fn from(error: TokenError) -> Self {
        Self::Token(error)
    }
}

impl From<SecurityError> for CliError {
    fn from(error: SecurityError) -> Self {
        Self::Security(error)
    }
}

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
