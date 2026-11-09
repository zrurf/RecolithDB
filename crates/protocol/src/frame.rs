use crate::compression::{CompressionAlgorithm, Compressor, compressor};
use crate::error::{ProtocolError, Result};

/// Frame magic bytes.
pub const MAGIC: [u8; 4] = *b"RCLT";
/// Supported protocol major version.
pub const VERSION_MAJOR: u8 = 1;
/// Supported protocol minor version.
pub const VERSION_MINOR: u8 = 0;
/// Fixed frame header length.
pub const HEADER_LEN: usize = 38;
/// Default maximum payload size.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
/// Default maximum decompressed payload size.
pub const DEFAULT_MAX_DECOMPRESSED_SIZE: usize = 16 * 1024 * 1024;
/// Compression envelope overhead: algorithm byte plus uncompressed length.
pub const COMPRESSION_ENVELOPE_LEN: usize = 5;

/// Frame is a response.
pub const FLAG_RESPONSE: u16 = 0x0001;
/// Frame payload is compressed.
pub const FLAG_COMPRESSED: u16 = 0x0002;
/// Frame is a control message.
pub const FLAG_CONTROL: u16 = 0x0004;
/// Payload encoding mask.
pub const FLAG_ENCODING_MASK: u16 = 0x0018;
/// Payload encoding shift.
pub const FLAG_ENCODING_SHIFT: u16 = 3;
/// All flags defined by protocol version 1.
pub const KNOWN_FLAGS: u16 = FLAG_RESPONSE | FLAG_COMPRESSED | FLAG_CONTROL | FLAG_ENCODING_MASK;

const OFF_MAGIC: usize = 0;
const OFF_VERSION_MAJOR: usize = 4;
const OFF_VERSION_MINOR: usize = 5;
const OFF_FLAGS: usize = 6;
const OFF_OPCODE: usize = 8;
const OFF_STATUS: usize = 10;
const OFF_HEADER_LEN: usize = 12;
const OFF_REQUEST_ID: usize = 14;
const OFF_STREAM_ID: usize = 22;
const OFF_PAYLOAD_LEN: usize = 26;
const OFF_HEADER_CRC: usize = 30;
const OFF_PAYLOAD_CRC: usize = 34;

/// Payload encoding negotiated by the handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadEncoding {
    /// Protobuf.
    Protobuf,
    /// SBE, reserved for a later version.
    Sbe,
    /// Custom encoding, reserved for a later version.
    Custom,
}

impl PayloadEncoding {
    /// Decodes an encoding field value.
    #[must_use]
    pub const fn from_wire(value: u16) -> Option<Self> {
        match value {
            0 => Some(Self::Protobuf),
            1 => Some(Self::Sbe),
            2 => Some(Self::Custom),
            _ => None,
        }
    }

    /// Returns the wire value.
    #[must_use]
    pub const fn as_wire(self) -> u16 {
        match self {
            Self::Protobuf => 0,
            Self::Sbe => 1,
            Self::Custom => 2,
        }
    }
}

/// Fixed protocol frame header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    version_major: u8,
    version_minor: u8,
    flags: u16,
    opcode: u16,
    status: u16,
    header_len: u16,
    request_id: u64,
    stream_id: u32,
    payload_len: u32,
    header_crc: u32,
    payload_crc: u32,
}

impl FrameHeader {
    /// Creates a version 1 request header.
    #[must_use]
    pub const fn new(opcode: u16, request_id: u64) -> Self {
        Self {
            version_major: VERSION_MAJOR,
            version_minor: VERSION_MINOR,
            flags: 0,
            opcode,
            status: 0,
            header_len: HEADER_LEN as u16,
            request_id,
            stream_id: 0,
            payload_len: 0,
            header_crc: 0,
            payload_crc: 0,
        }
    }

    /// Adds response flag and status.
    #[must_use]
    pub const fn as_response(mut self, status: u16) -> Self {
        self.flags |= FLAG_RESPONSE;
        self.status = status;
        self
    }

    /// Sets frame flags.
    #[must_use]
    pub const fn with_flags(mut self, flags: u16) -> Self {
        self.flags = flags;
        self
    }

    /// Sets the status code.
    #[must_use]
    pub const fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    /// Sets the stream ID.
    #[must_use]
    pub const fn with_stream_id(mut self, stream_id: u32) -> Self {
        self.stream_id = stream_id;
        self
    }

    /// Sets the payload encoding bits.
    #[must_use]
    pub const fn with_encoding(mut self, encoding: PayloadEncoding) -> Self {
        self.flags =
            (self.flags & !FLAG_ENCODING_MASK) | (encoding.as_wire() << FLAG_ENCODING_SHIFT);
        self
    }

    /// Returns the major version.
    #[must_use]
    pub const fn version_major(&self) -> u8 {
        self.version_major
    }

    /// Returns the minor version.
    #[must_use]
    pub const fn version_minor(&self) -> u8 {
        self.version_minor
    }

    /// Returns whether this header uses a compatible major version.
    #[must_use]
    pub const fn is_compatible_version(&self) -> bool {
        self.version_major == VERSION_MAJOR
    }

    /// Returns raw flags.
    #[must_use]
    pub const fn flags(&self) -> u16 {
        self.flags
    }

    /// Returns whether this frame is a response.
    #[must_use]
    pub const fn is_response(&self) -> bool {
        self.flags & FLAG_RESPONSE != 0
    }

    /// Returns whether the payload is compressed.
    #[must_use]
    pub const fn is_compressed(&self) -> bool {
        self.flags & FLAG_COMPRESSED != 0
    }

    /// Returns whether this frame is a control message.
    #[must_use]
    pub const fn is_control(&self) -> bool {
        self.flags & FLAG_CONTROL != 0
    }

    /// Returns the payload encoding.
    #[must_use]
    pub const fn encoding(&self) -> Option<PayloadEncoding> {
        PayloadEncoding::from_wire((self.flags & FLAG_ENCODING_MASK) >> FLAG_ENCODING_SHIFT)
    }

    /// Returns the opcode.
    #[must_use]
    pub const fn opcode(&self) -> u16 {
        self.opcode
    }

    /// Returns the status code.
    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the header length.
    #[must_use]
    pub const fn header_len(&self) -> u16 {
        self.header_len
    }

    /// Returns the request ID.
    #[must_use]
    pub const fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns the stream ID.
    #[must_use]
    pub const fn stream_id(&self) -> u32 {
        self.stream_id
    }

    /// Returns the payload length.
    #[must_use]
    pub const fn payload_len(&self) -> u32 {
        self.payload_len
    }

    /// Returns the stored payload checksum.
    #[must_use]
    pub const fn payload_crc(&self) -> u32 {
        self.payload_crc
    }

    /// Parses and validates a fixed frame header.
    ///
    /// The version is validated separately so a server can return a structured
    /// version error for a structurally valid frame.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for truncation, magic, header length, flag, or
    /// checksum failures.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(ProtocolError::FrameTooShort {
                length: bytes.len(),
            });
        }
        let bytes = &bytes[..HEADER_LEN];
        if bytes[OFF_MAGIC..OFF_MAGIC + 4] != MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }

        let header_len = read_u16(bytes, OFF_HEADER_LEN);
        if usize::from(header_len) != HEADER_LEN {
            return Err(ProtocolError::InvalidHeaderLength(header_len));
        }

        let mut header_without_crc = [0_u8; HEADER_LEN];
        header_without_crc.copy_from_slice(bytes);
        let stored_header_crc = read_u32(bytes, OFF_HEADER_CRC);
        header_without_crc[OFF_HEADER_CRC..OFF_PAYLOAD_CRC].fill(0);
        header_without_crc[OFF_PAYLOAD_CRC..HEADER_LEN].fill(0);
        let actual_header_crc = crc32c::crc32c(&header_without_crc);
        if stored_header_crc != actual_header_crc {
            return Err(ProtocolError::HeaderChecksumMismatch {
                expected: stored_header_crc,
                actual: actual_header_crc,
            });
        }

        let flags = read_u16(bytes, OFF_FLAGS);
        if flags & !KNOWN_FLAGS != 0 {
            return Err(ProtocolError::ReservedFlags(flags));
        }

        Ok(Self {
            version_major: bytes[OFF_VERSION_MAJOR],
            version_minor: bytes[OFF_VERSION_MINOR],
            flags,
            opcode: read_u16(bytes, OFF_OPCODE),
            status: read_u16(bytes, OFF_STATUS),
            header_len,
            request_id: read_u64(bytes, OFF_REQUEST_ID),
            stream_id: read_u32(bytes, OFF_STREAM_ID),
            payload_len: read_u32(bytes, OFF_PAYLOAD_LEN),
            header_crc: stored_header_crc,
            payload_crc: read_u32(bytes, OFF_PAYLOAD_CRC),
        })
    }

    /// Validates payload length and checksum against this header.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for a length mismatch or checksum mismatch.
    pub fn validate_payload(&self, payload: &[u8]) -> Result<()> {
        if usize::try_from(self.payload_len).ok() != Some(payload.len()) {
            return Err(ProtocolError::PayloadLengthMismatch {
                expected: self.payload_len,
                actual: payload.len(),
            });
        }
        let actual = crc32c::crc32c(payload);
        if actual != self.payload_crc {
            return Err(ProtocolError::PayloadChecksumMismatch {
                expected: self.payload_crc,
                actual,
            });
        }

        Ok(())
    }

    fn encode_with_payload(&self, payload: &[u8]) -> Result<[u8; HEADER_LEN]> {
        if usize::try_from(self.payload_len).ok() != Some(payload.len()) {
            return Err(ProtocolError::PayloadLengthMismatch {
                expected: self.payload_len,
                actual: payload.len(),
            });
        }
        if self.flags & !KNOWN_FLAGS != 0 {
            return Err(ProtocolError::ReservedFlags(self.flags));
        }
        if usize::from(self.header_len) != HEADER_LEN {
            return Err(ProtocolError::InvalidHeaderLength(self.header_len));
        }

        let mut bytes = [0_u8; HEADER_LEN];
        bytes[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&MAGIC);
        bytes[OFF_VERSION_MAJOR] = self.version_major;
        bytes[OFF_VERSION_MINOR] = self.version_minor;
        write_u16(&mut bytes, OFF_FLAGS, self.flags);
        write_u16(&mut bytes, OFF_OPCODE, self.opcode);
        write_u16(&mut bytes, OFF_STATUS, self.status);
        write_u16(&mut bytes, OFF_HEADER_LEN, self.header_len);
        write_u64(&mut bytes, OFF_REQUEST_ID, self.request_id);
        write_u32(&mut bytes, OFF_STREAM_ID, self.stream_id);
        write_u32(&mut bytes, OFF_PAYLOAD_LEN, self.payload_len);
        let header_crc = crc32c::crc32c(&bytes);
        write_u32(&mut bytes, OFF_HEADER_CRC, header_crc);
        write_u32(&mut bytes, OFF_PAYLOAD_CRC, crc32c::crc32c(payload));

        Ok(bytes)
    }

    fn set_payload_len(&mut self, length: usize) -> Result<()> {
        let length = u32::try_from(length).map_err(|_| ProtocolError::PayloadTooLarge {
            length,
            limit: u32::MAX as usize,
        })?;
        self.payload_len = length;

        Ok(())
    }
}

/// Session frame codec with negotiated compression and limits.
pub struct FrameCodec {
    compression: CompressionAlgorithm,
    compressor: Option<Box<dyn Compressor>>,
    threshold: usize,
    max_frame_size: usize,
    max_decompressed_size: usize,
}

impl FrameCodec {
    /// Creates a frame codec.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for an unsupported compression algorithm.
    pub fn new(
        compression: CompressionAlgorithm,
        threshold: usize,
        max_frame_size: usize,
        max_decompressed_size: usize,
    ) -> Result<Self> {
        Ok(Self {
            compression,
            compressor: compressor(compression)?,
            threshold,
            max_frame_size,
            max_decompressed_size,
        })
    }

    /// Creates a codec without compression using default limits.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if the codec cannot be created.
    pub fn plain() -> Result<Self> {
        Self::new(
            CompressionAlgorithm::None,
            0,
            DEFAULT_MAX_FRAME_SIZE,
            DEFAULT_MAX_DECOMPRESSED_SIZE,
        )
    }

    /// Returns the negotiated compression algorithm.
    #[must_use]
    pub const fn compression(&self) -> CompressionAlgorithm {
        self.compression
    }

    /// Encodes one complete frame, applying compression when configured.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for size limits or compression failures.
    pub fn encode(&self, header: &FrameHeader, payload: &[u8]) -> Result<Vec<u8>> {
        self.encode_inner(header, payload, true)
    }

    /// Encodes one complete frame without compression.
    ///
    /// Handshake frames use this method.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for size limits.
    pub fn encode_plain(&self, header: &FrameHeader, payload: &[u8]) -> Result<Vec<u8>> {
        self.encode_inner(header, payload, false)
    }

    /// Decodes and validates one frame payload.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] for length, checksum, or compression failures.
    pub fn decode(&self, header: &FrameHeader, payload: &[u8]) -> Result<Vec<u8>> {
        if payload.len() > self.max_frame_size {
            return Err(ProtocolError::PayloadTooLarge {
                length: payload.len(),
                limit: self.max_frame_size,
            });
        }
        header.validate_payload(payload)?;

        if !header.is_compressed() {
            return Ok(payload.to_vec());
        }
        if payload.len() < COMPRESSION_ENVELOPE_LEN {
            return Err(ProtocolError::CompressionEnvelope {
                length: payload.len(),
            });
        }

        let algorithm = CompressionAlgorithm::from_wire(payload[0])
            .ok_or(ProtocolError::UnsupportedCompression(payload[0]))?;
        if algorithm != self.compression {
            return Err(ProtocolError::UnsupportedCompression(payload[0]));
        }
        let declared = u32::from_le_bytes(
            payload[1..COMPRESSION_ENVELOPE_LEN]
                .try_into()
                .expect("four bytes"),
        ) as usize;
        if declared > self.max_decompressed_size {
            return Err(ProtocolError::DecompressionLimit {
                declared,
                limit: self.max_decompressed_size,
            });
        }
        let codec = self
            .compressor
            .as_ref()
            .ok_or(ProtocolError::UnsupportedCompression(payload[0]))?;
        let output = codec.decompress(
            &payload[COMPRESSION_ENVELOPE_LEN..],
            declared,
            self.max_decompressed_size,
        )?;
        if output.len() != declared {
            return Err(ProtocolError::DecompressionLength {
                declared,
                actual: output.len(),
            });
        }

        Ok(output)
    }

    fn encode_inner(
        &self,
        header: &FrameHeader,
        payload: &[u8],
        allow_compression: bool,
    ) -> Result<Vec<u8>> {
        let mut header = *header;
        let mut wire_payload = None;

        if allow_compression
            && let Some(codec) = self.compressor.as_ref()
            && payload.len() >= self.threshold
        {
            let compressed = codec.compress(payload)?;
            if compressed.len() + COMPRESSION_ENVELOPE_LEN < payload.len() {
                let mut envelope = Vec::with_capacity(COMPRESSION_ENVELOPE_LEN + compressed.len());
                envelope.push(self.compression.as_wire());
                let declared =
                    u32::try_from(payload.len()).map_err(|_| ProtocolError::PayloadTooLarge {
                        length: payload.len(),
                        limit: u32::MAX as usize,
                    })?;
                envelope.extend_from_slice(&declared.to_le_bytes());
                envelope.extend_from_slice(&compressed);
                header.flags |= FLAG_COMPRESSED;
                wire_payload = Some(envelope);
            }
        }
        if wire_payload.is_none() {
            header.flags &= !FLAG_COMPRESSED;
        }
        let wire_payload = wire_payload.as_deref().unwrap_or(payload);
        if wire_payload.len() > self.max_frame_size {
            return Err(ProtocolError::PayloadTooLarge {
                length: wire_payload.len(),
                limit: self.max_frame_size,
            });
        }
        header.set_payload_len(wire_payload.len())?;
        let header = header.encode_with_payload(wire_payload)?;
        let mut frame = Vec::with_capacity(HEADER_LEN + wire_payload.len());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(wire_payload);

        Ok(frame)
    }
}

impl std::fmt::Debug for FrameCodec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrameCodec")
            .field("compression", &self.compression)
            .field("threshold", &self.threshold)
            .field("max_frame_size", &self.max_frame_size)
            .field("max_decompressed_size", &self.max_decompressed_size)
            .finish()
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("two bytes"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("four bytes"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("eight bytes"))
}

fn write_u16(bytes: &mut [u8; HEADER_LEN], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8; HEADER_LEN], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8; HEADER_LEN], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
