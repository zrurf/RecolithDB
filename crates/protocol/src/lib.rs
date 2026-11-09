//! Wire protocol: fixed frame header, batch framing, compression, and messages.

#![forbid(unsafe_code)]

pub mod batch;
pub mod compression;
pub mod error;
pub mod frame;
pub mod messages;

pub use batch::{
    BatchLimits, DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_SUBFRAMES, SUBFRAME_HEADER_LEN, SubFrame,
    decode_batch, encode_batch, is_batchable_opcode,
};
pub use compression::{CompressionAlgorithm, Compressor, compressor};
pub use error::{ProtocolError, Result};
pub use frame::{
    COMPRESSION_ENVELOPE_LEN, DEFAULT_MAX_DECOMPRESSED_SIZE, DEFAULT_MAX_FRAME_SIZE,
    FLAG_COMPRESSED, FLAG_CONTROL, FLAG_ENCODING_MASK, FLAG_ENCODING_SHIFT, FLAG_RESPONSE,
    FrameCodec, FrameHeader, HEADER_LEN, KNOWN_FLAGS, MAGIC, PayloadEncoding, VERSION_MAJOR,
    VERSION_MINOR,
};
