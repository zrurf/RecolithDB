//! Protobuf message types and protocol constants.

include!(concat!(env!("OUT_DIR"), "/recolith.protocol.v1.rs"));

/// Frame opcodes.
pub mod opcode {
    /// Client handshake.
    pub const HELLO: u16 = 0x0001;
    /// Server handshake response.
    pub const WELCOME: u16 = 0x0002;
    /// Liveness request.
    pub const PING: u16 = 0x0003;
    /// Liveness response.
    pub const PONG: u16 = 0x0004;
    /// Document fragment read.
    pub const DOC_GET: u16 = 0x0010;
    /// Document fragment write.
    pub const DOC_PUT: u16 = 0x0011;
    /// RQL document query.
    pub const QUERY: u16 = 0x0020;
    /// Batched unary requests.
    pub const BATCH: u16 = 0x0030;
    /// Collection metadata lookup.
    pub const DESCRIBE_COLLECTION: u16 = 0x0050;
    /// Structured error response.
    pub const ERROR: u16 = 0x0040;
    /// Session shutdown notice.
    pub const GOAWAY: u16 = 0x0041;
}

/// Frame status codes.
pub mod status {
    /// Success.
    pub const OK: u16 = 0;
    /// Malformed request.
    pub const INVALID_REQUEST: u16 = 1;
    /// Unknown opcode.
    pub const UNSUPPORTED_OPCODE: u16 = 2;
    /// Unsupported protocol version.
    pub const PROTOCOL_VERSION: u16 = 3;
    /// Required feature is unavailable.
    pub const UNSUPPORTED_FEATURE: u16 = 4;
    /// Authentication failed.
    pub const AUTH_FAILED: u16 = 5;
    /// Permission denied.
    pub const PERMISSION_DENIED: u16 = 6;
    /// Requested object does not exist.
    pub const NOT_FOUND: u16 = 7;
    /// Optimistic conflict.
    pub const CONFLICT: u16 = 8;
    /// Request deadline expired.
    pub const DEADLINE_EXCEEDED: u16 = 9;
    /// Resource limit reached.
    pub const RESOURCE_EXHAUSTED: u16 = 10;
    /// Record schema mismatch.
    pub const SCHEMA_MISMATCH: u16 = 11;
    /// Client catalog generation is stale.
    pub const STALE_CATALOG: u16 = 12;
    /// Frame checksum validation failed.
    pub const CHECKSUM_MISMATCH: u16 = 13;
    /// Frame exceeds the negotiated size.
    pub const FRAME_TOO_LARGE: u16 = 14;
    /// Internal server failure.
    pub const INTERNAL: u16 = 15;
    /// Server is shutting down.
    pub const SHUTTING_DOWN: u16 = 16;
    /// Payload encoding is not supported.
    pub const UNSUPPORTED_ENCODING: u16 = 17;
    /// Compression algorithm is not supported.
    pub const UNSUPPORTED_COMPRESSION: u16 = 18;
}

/// Capability feature bits used by the handshake.
pub mod feature {
    /// Explicit batch frames are supported.
    pub const BATCH: u64 = 1 << 0;
    /// Document reads are supported.
    pub const DOCUMENT_READ: u64 = 1 << 1;
    /// Document writes are supported.
    pub const DOCUMENT_WRITE: u64 = 1 << 2;
    /// RQL queries are supported.
    pub const QUERY: u64 = 1 << 3;
    /// Server-side idempotency is supported.
    pub const IDEMPOTENCY: u64 = 1 << 4;
    /// Streaming result batches are supported.
    pub const STREAMING: u64 = 1 << 5;
    /// SBE payload encoding is supported.
    pub const SBE: u64 = 1 << 6;
    /// Collection metadata lookup is supported.
    pub const CATALOG_DESCRIBE: u64 = 1 << 7;
}

/// Compression algorithm bits used by the handshake.
pub mod compression_bit {
    /// Zstandard.
    pub const ZSTD: u32 = 1 << 0;
    /// LZ4 block format.
    pub const LZ4: u32 = 1 << 1;
}

/// Payload encoding bits used by the handshake.
pub mod encoding_bit {
    /// Protobuf.
    pub const PROTOBUF: u32 = 1 << 0;
    /// SBE (reserved).
    pub const SBE: u32 = 1 << 1;
}
