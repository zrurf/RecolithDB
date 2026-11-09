use std::fmt;

/// Protocol encoding or validation error.
#[derive(Debug)]
pub enum ProtocolError {
    /// The input does not contain a complete fixed frame header.
    FrameTooShort { length: usize },
    /// The frame magic does not match this protocol.
    InvalidMagic,
    /// The frame header length is not supported.
    InvalidHeaderLength(u16),
    /// The negotiated frame size limit was exceeded.
    PayloadTooLarge { length: usize, limit: usize },
    /// The payload length stored in the header differs from the bytes read.
    PayloadLengthMismatch { expected: u32, actual: usize },
    /// The header checksum does not match.
    HeaderChecksumMismatch { expected: u32, actual: u32 },
    /// The payload checksum does not match.
    PayloadChecksumMismatch { expected: u32, actual: u32 },
    /// Reserved frame flags were set.
    ReservedFlags(u16),
    /// The frame uses a compression algorithm this session did not negotiate.
    UnsupportedCompression(u8),
    /// The declared decompressed size exceeds the session limit.
    DecompressionLimit { declared: usize, limit: usize },
    /// The actual decompressed size differs from the declared size.
    DecompressionLength { declared: usize, actual: usize },
    /// The compression envelope is truncated or malformed.
    CompressionEnvelope { length: usize },
    /// The compression codec failed.
    Compression(std::io::Error),
    /// A batch contained no subframes.
    EmptyBatch,
    /// A batch exceeded the subframe limit.
    BatchTooMany { count: usize, limit: usize },
    /// A batch exceeded the byte limit.
    BatchTooLarge { size: usize, limit: usize },
    /// A batch contained another batch.
    NestedBatch,
    /// A batch contained an opcode that is not allowed inside a batch.
    BatchForbiddenOpcode(u16),
    /// A batch contained the same request ID twice.
    DuplicateRequestId(u64),
    /// A batch subframe header or payload is truncated.
    BatchTruncated,
    /// A batch had bytes after the last subframe.
    BatchTrailingBytes,
    /// A batch subframe had non-zero flags.
    BatchSubframeFlags(u16),
    /// A request used request ID zero.
    ZeroRequestId,
    /// A protobuf payload could not be decoded.
    Decode(prost::DecodeError),
    /// A protobuf payload could not be encoded.
    Encode(prost::EncodeError),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooShort { length } => {
                write!(formatter, "frame header is truncated: {length} bytes")
            }
            Self::InvalidMagic => formatter.write_str("invalid frame magic"),
            Self::InvalidHeaderLength(length) => {
                write!(formatter, "unsupported frame header length: {length}")
            }
            Self::PayloadTooLarge { length, limit } => {
                write!(formatter, "payload is {length} bytes, limit is {limit}")
            }
            Self::PayloadLengthMismatch { expected, actual } => write!(
                formatter,
                "payload length mismatch: header says {expected}, got {actual}"
            ),
            Self::HeaderChecksumMismatch { expected, actual } => write!(
                formatter,
                "header checksum mismatch: expected {expected:#x}, got {actual:#x}"
            ),
            Self::PayloadChecksumMismatch { expected, actual } => write!(
                formatter,
                "payload checksum mismatch: expected {expected:#x}, got {actual:#x}"
            ),
            Self::ReservedFlags(flags) => {
                write!(formatter, "reserved frame flags set: {flags:#06x}")
            }
            Self::UnsupportedCompression(algorithm) => {
                write!(formatter, "unsupported compression algorithm: {algorithm}")
            }
            Self::DecompressionLimit { declared, limit } => write!(
                formatter,
                "declared decompressed size {declared} exceeds limit {limit}"
            ),
            Self::DecompressionLength { declared, actual } => write!(
                formatter,
                "decompressed size mismatch: declared {declared}, got {actual}"
            ),
            Self::CompressionEnvelope { length } => {
                write!(
                    formatter,
                    "compression envelope is malformed: {length} bytes"
                )
            }
            Self::Compression(error) => write!(formatter, "compression error: {error}"),
            Self::EmptyBatch => formatter.write_str("batch contains no subframes"),
            Self::BatchTooMany { count, limit } => {
                write!(formatter, "batch has {count} subframes, limit is {limit}")
            }
            Self::BatchTooLarge { size, limit } => {
                write!(formatter, "batch is {size} bytes, limit is {limit}")
            }
            Self::NestedBatch => formatter.write_str("nested batch is not allowed"),
            Self::BatchForbiddenOpcode(opcode) => {
                write!(formatter, "opcode {opcode:#06x} is not allowed in a batch")
            }
            Self::DuplicateRequestId(request_id) => {
                write!(formatter, "duplicate request ID in batch: {request_id}")
            }
            Self::BatchTruncated => formatter.write_str("batch subframe is truncated"),
            Self::BatchTrailingBytes => formatter.write_str("batch has trailing bytes"),
            Self::BatchSubframeFlags(flags) => {
                write!(
                    formatter,
                    "batch subframe flags must be zero, got {flags:#06x}"
                )
            }
            Self::ZeroRequestId => formatter.write_str("request ID must not be zero"),
            Self::Decode(error) => write!(formatter, "protobuf decode error: {error}"),
            Self::Encode(error) => write!(formatter, "protobuf encode error: {error}"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Compression(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Encode(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ProtocolError {
    fn from(error: std::io::Error) -> Self {
        Self::Compression(error)
    }
}

impl From<prost::DecodeError> for ProtocolError {
    fn from(error: prost::DecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<prost::EncodeError> for ProtocolError {
    fn from(error: prost::EncodeError) -> Self {
        Self::Encode(error)
    }
}

/// Result alias for protocol operations.
pub type Result<T> = std::result::Result<T, ProtocolError>;
