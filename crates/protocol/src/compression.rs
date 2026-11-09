use crate::error::{ProtocolError, Result};

/// Wire compression algorithm identifiers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressionAlgorithm {
    /// No compression.
    None,
    /// Zstandard.
    Zstd,
    /// LZ4 block format.
    Lz4,
}

impl CompressionAlgorithm {
    /// Decodes a wire algorithm identifier.
    #[must_use]
    pub const fn from_wire(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            2 => Some(Self::Lz4),
            _ => None,
        }
    }

    /// Returns the wire identifier.
    #[must_use]
    pub const fn as_wire(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
            Self::Lz4 => 2,
        }
    }
}

/// Compression codec used by a session.
pub trait Compressor: Send + Sync {
    /// Compresses a payload.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Compression`] when the codec fails.
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>>;

    /// Decompresses a payload with a known output size.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] when the codec fails or the declared size
    /// exceeds `max_output`.
    fn decompress(&self, input: &[u8], declared_size: usize, max_output: usize) -> Result<Vec<u8>>;
}

/// Creates a codec for a negotiated algorithm.
///
/// # Errors
///
/// Returns [`ProtocolError::UnsupportedCompression`] for unknown algorithms.
pub fn compressor(algorithm: CompressionAlgorithm) -> Result<Option<Box<dyn Compressor>>> {
    let codec: Option<Box<dyn Compressor>> = match algorithm {
        CompressionAlgorithm::None => None,
        CompressionAlgorithm::Zstd => Some(Box::new(ZstdCompressor)),
        CompressionAlgorithm::Lz4 => Some(Box::new(Lz4Compressor)),
    };

    Ok(codec)
}

struct ZstdCompressor;

impl Compressor for ZstdCompressor {
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        zstd::bulk::compress(input, 1).map_err(ProtocolError::Compression)
    }

    fn decompress(&self, input: &[u8], declared_size: usize, max_output: usize) -> Result<Vec<u8>> {
        if declared_size > max_output {
            return Err(ProtocolError::DecompressionLimit {
                declared: declared_size,
                limit: max_output,
            });
        }

        zstd::bulk::decompress(input, declared_size).map_err(ProtocolError::Compression)
    }
}

struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        lz4::block::compress(input, Some(lz4::block::CompressionMode::DEFAULT), false)
            .map_err(ProtocolError::Compression)
    }

    fn decompress(&self, input: &[u8], declared_size: usize, max_output: usize) -> Result<Vec<u8>> {
        if declared_size > max_output {
            return Err(ProtocolError::DecompressionLimit {
                declared: declared_size,
                limit: max_output,
            });
        }
        let declared =
            i32::try_from(declared_size).map_err(|_| ProtocolError::DecompressionLimit {
                declared: declared_size,
                limit: max_output,
            })?;

        lz4::block::decompress(input, Some(declared)).map_err(ProtocolError::Compression)
    }
}
