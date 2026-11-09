use recolith_common::{CommitTs, SchemaId, SchemaVersion};

use super::checksum::crc32c;
use super::header::RecordHeader;
use crate::error::{Result, StorageError};

const MAGIC: u32 = 0x5243_4C54;
const FORMAT_VERSION: u16 = 1;

/// Size of the fixed record header.
pub const HEADER_LEN: usize = 4 + 2 + 8 + 4 + 2 + 8 + 8 + 4 + 4;

/// Decoded record frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRecord<'a> {
    header: RecordHeader,
    payload: &'a [u8],
}

impl<'a> DecodedRecord<'a> {
    /// Returns the decoded header.
    #[must_use]
    pub fn header(&self) -> &RecordHeader {
        &self.header
    }

    /// Returns the payload bytes.
    #[must_use]
    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

/// Encodes a record header and opaque payload.
///
/// # Errors
///
/// Returns [`StorageError::RecordTooLarge`] when the payload does not fit the length field.
pub fn encode_record(header: &RecordHeader, payload: &[u8]) -> Result<Vec<u8>> {
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| StorageError::RecordTooLarge(payload.len()))?;
    let payload_crc = crc32c(payload);

    let mut output = Vec::with_capacity(HEADER_LEN + payload.len());
    output.extend_from_slice(&MAGIC.to_be_bytes());
    output.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    output.extend_from_slice(&header.schema_id().get().to_be_bytes());
    output.extend_from_slice(&header.schema_version().get().to_be_bytes());
    output.extend_from_slice(&header.flags().to_be_bytes());
    output.extend_from_slice(&header.commit_ts().get().to_be_bytes());
    output.extend_from_slice(&header.expire_at().to_be_bytes());
    output.extend_from_slice(&payload_len.to_be_bytes());
    output.extend_from_slice(&payload_crc.to_be_bytes());
    output.extend_from_slice(payload);

    Ok(output)
}

/// Decodes and validates a record frame.
///
/// # Errors
///
/// Returns [`StorageError`] for truncated frames, invalid magic/version, length mismatch, or checksum failure.
pub fn decode_record(bytes: &[u8]) -> Result<DecodedRecord<'_>> {
    if bytes.len() < HEADER_LEN {
        return Err(StorageError::RecordTooShort);
    }

    let magic = u32::from_be_bytes(read_array(bytes, 0));
    if magic != MAGIC {
        return Err(StorageError::InvalidMagic);
    }

    let format_version = u16::from_be_bytes(read_array(bytes, 4));
    if format_version != FORMAT_VERSION {
        return Err(StorageError::UnsupportedFormat(format_version));
    }

    let schema_id = SchemaId::new(u64::from_be_bytes(read_array(bytes, 6)));
    let schema_version = SchemaVersion::new(u32::from_be_bytes(read_array(bytes, 14)));
    let flags = u16::from_be_bytes(read_array(bytes, 18));
    let commit_ts = CommitTs::new(u64::from_be_bytes(read_array(bytes, 20)));
    let expire_at = u64::from_be_bytes(read_array(bytes, 28));
    let payload_len = u32::from_be_bytes(read_array(bytes, 36));
    let payload_crc = u32::from_be_bytes(read_array(bytes, 40));

    let expected_len = HEADER_LEN
        .checked_add(payload_len as usize)
        .ok_or(StorageError::RecordTooShort)?;
    if expected_len != bytes.len() {
        return Err(StorageError::LengthMismatch {
            expected: expected_len,
            actual: bytes.len(),
        });
    }

    let payload = &bytes[HEADER_LEN..];
    let actual_crc = crc32c(payload);
    if actual_crc != payload_crc {
        return Err(StorageError::ChecksumMismatch {
            expected: payload_crc,
            actual: actual_crc,
        });
    }

    let header = RecordHeader::new(schema_id, schema_version, commit_ts)
        .with_flags(flags)
        .with_expire_at(expire_at)
        .with_payload(payload_len, payload_crc);

    Ok(DecodedRecord { header, payload })
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    let mut array = [0_u8; N];
    array.copy_from_slice(&bytes[offset..offset + N]);
    array
}
