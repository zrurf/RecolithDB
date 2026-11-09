use std::fmt;

use recolith_backend::BackendError;
use recolith_common::{FieldId, IndexId, SchemaId, SchemaVersion};

/// Storage encoding and record-frame error.
#[derive(Debug)]
pub enum StorageError {
    /// Record bytes are shorter than the fixed header.
    RecordTooShort,
    /// Record magic does not match this format.
    InvalidMagic,
    /// Record format version is unsupported.
    UnsupportedFormat(u16),
    /// Encoded payload length does not match the frame length.
    LengthMismatch { expected: usize, actual: usize },
    /// Payload checksum does not match.
    ChecksumMismatch { expected: u32, actual: u32 },
    /// Payload does not fit in the frame's length field.
    RecordTooLarge(usize),
    /// A key cannot be encoded.
    InvalidKey(String),
    /// A required schema field is absent.
    MissingField(FieldId),
    /// A payload contains the same field more than once.
    DuplicateField(FieldId),
    /// A field value does not match its schema type.
    TypeMismatch { field: FieldId, expected: String },
    /// A field value is malformed.
    InvalidValue(String),
    /// A payload uses an unknown wire type.
    InvalidWireType(u8),
    /// Record header schema does not match the schema used for encoding.
    SchemaMismatch {
        record_schema: SchemaId,
        record_version: SchemaVersion,
        expected_schema: SchemaId,
        expected_version: SchemaVersion,
    },
    /// A unique index already contains the value for another record.
    UniqueViolation { index: IndexId, value: Vec<u8> },
    /// A value type cannot be used as an index key.
    UnindexableValue(&'static str),
    /// Backend read or write failed.
    Backend(BackendError),
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecordTooShort => formatter.write_str("record is shorter than its header"),
            Self::InvalidMagic => formatter.write_str("invalid record magic"),
            Self::UnsupportedFormat(version) => {
                write!(formatter, "unsupported record format version: {version}")
            }
            Self::LengthMismatch { expected, actual } => {
                write!(
                    formatter,
                    "record length mismatch: expected {expected}, got {actual}"
                )
            }
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    formatter,
                    "record checksum mismatch: expected {expected:#x}, got {actual:#x}"
                )
            }
            Self::RecordTooLarge(length) => {
                write!(formatter, "record payload is too large: {length} bytes")
            }
            Self::InvalidKey(message) => write!(formatter, "invalid key: {message}"),
            Self::MissingField(id) => write!(formatter, "missing required field: {id}"),
            Self::DuplicateField(id) => write!(formatter, "duplicate field in payload: {id}"),
            Self::TypeMismatch { field, expected } => {
                write!(formatter, "field {field} expects {expected}")
            }
            Self::InvalidValue(message) => write!(formatter, "invalid field value: {message}"),
            Self::InvalidWireType(wire_type) => {
                write!(formatter, "invalid wire type: {wire_type}")
            }
            Self::SchemaMismatch {
                record_schema,
                record_version,
                expected_schema,
                expected_version,
            } => write!(
                formatter,
                "record schema {record_schema}@{record_version} does not match {expected_schema}@{expected_version}"
            ),
            Self::UniqueViolation { index, value } => write!(
                formatter,
                "unique index {index} already contains value with {} bytes",
                value.len()
            ),
            Self::UnindexableValue(kind) => {
                write!(formatter, "value of type {kind} cannot be an index key")
            }
            Self::Backend(error) => write!(formatter, "backend error: {error}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<BackendError> for StorageError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

/// Result alias for storage primitives.
pub type Result<T> = std::result::Result<T, StorageError>;
