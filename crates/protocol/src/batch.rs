//! Explicit batch frame payload codec.

use std::collections::BTreeSet;

use crate::error::{ProtocolError, Result};
use crate::messages::opcode;

/// Fixed subframe header length.
pub const SUBFRAME_HEADER_LEN: usize = 16;
/// Default maximum number of subframes in a batch.
pub const DEFAULT_MAX_SUBFRAMES: usize = 64;
/// Default maximum uncompressed batch payload size.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 1024 * 1024;

/// Batch size limits negotiated for a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchLimits {
    /// Maximum number of subframes.
    pub max_subframes: usize,
    /// Maximum encoded batch payload size.
    pub max_bytes: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_subframes: DEFAULT_MAX_SUBFRAMES,
            max_bytes: DEFAULT_MAX_BATCH_BYTES,
        }
    }
}

/// One unary request inside a batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubFrame<'a> {
    opcode: u16,
    flags: u16,
    request_id: u64,
    payload: &'a [u8],
}

impl<'a> SubFrame<'a> {
    /// Creates a subframe.
    #[must_use]
    pub const fn new(opcode: u16, request_id: u64, payload: &'a [u8]) -> Self {
        Self {
            opcode,
            flags: 0,
            request_id,
            payload,
        }
    }

    /// Returns the opcode.
    #[must_use]
    pub const fn opcode(&self) -> u16 {
        self.opcode
    }

    /// Returns the flags.
    #[must_use]
    pub const fn flags(&self) -> u16 {
        self.flags
    }

    /// Returns the request ID.
    #[must_use]
    pub const fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns the payload bytes.
    #[must_use]
    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

/// Returns whether an opcode may appear inside a batch.
#[must_use]
pub const fn is_batchable_opcode(opcode: u16) -> bool {
    matches!(
        opcode,
        opcode::PING | opcode::DOC_GET | opcode::DOC_PUT | opcode::QUERY
    )
}

/// Encodes a batch payload.
///
/// # Errors
///
/// Returns [`ProtocolError`] for empty batches, limits, invalid opcodes,
/// non-zero subframe flags, or duplicate request IDs.
pub fn encode_batch(subframes: &[SubFrame<'_>], limits: BatchLimits) -> Result<Vec<u8>> {
    validate_subframes(subframes, limits)?;

    let mut output = Vec::new();
    output.extend_from_slice(&(subframes.len() as u16).to_le_bytes());
    for subframe in subframes {
        output.extend_from_slice(&subframe.opcode.to_le_bytes());
        output.extend_from_slice(&subframe.flags.to_le_bytes());
        output.extend_from_slice(&subframe.request_id.to_le_bytes());
        let length =
            u32::try_from(subframe.payload.len()).map_err(|_| ProtocolError::BatchTooLarge {
                size: subframe.payload.len(),
                limit: u32::MAX as usize,
            })?;
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(subframe.payload);
    }

    Ok(output)
}

/// Decodes a batch payload. Returned subframes borrow the input.
///
/// # Errors
///
/// Returns [`ProtocolError`] for truncation, trailing bytes, limits, invalid
/// opcodes, non-zero flags, or duplicate request IDs.
pub fn decode_batch<'a>(payload: &'a [u8], limits: BatchLimits) -> Result<Vec<SubFrame<'a>>> {
    if payload.len() > limits.max_bytes {
        return Err(ProtocolError::BatchTooLarge {
            size: payload.len(),
            limit: limits.max_bytes,
        });
    }
    if payload.len() < 2 {
        return Err(ProtocolError::BatchTruncated);
    }
    let count = usize::from(u16::from_le_bytes([payload[0], payload[1]]));
    if count == 0 {
        return Err(ProtocolError::EmptyBatch);
    }
    if count > limits.max_subframes {
        return Err(ProtocolError::BatchTooMany {
            count,
            limit: limits.max_subframes,
        });
    }

    let mut offset: usize = 2;
    let mut subframes = Vec::with_capacity(count);
    let mut request_ids = BTreeSet::new();
    for _ in 0..count {
        let header_end = offset
            .checked_add(SUBFRAME_HEADER_LEN)
            .ok_or(ProtocolError::BatchTruncated)?;
        let header = payload
            .get(offset..header_end)
            .ok_or(ProtocolError::BatchTruncated)?;
        let opcode = u16::from_le_bytes([header[0], header[1]]);
        let flags = u16::from_le_bytes([header[2], header[3]]);
        let request_id = u64::from_le_bytes(header[4..12].try_into().expect("eight bytes"));
        let length = u32::from_le_bytes(header[12..16].try_into().expect("four bytes")) as usize;
        let payload_end = header_end
            .checked_add(length)
            .ok_or(ProtocolError::BatchTruncated)?;
        let subframe_payload = payload
            .get(header_end..payload_end)
            .ok_or(ProtocolError::BatchTruncated)?;
        if flags != 0 {
            return Err(ProtocolError::BatchSubframeFlags(flags));
        }
        if request_id == 0 {
            return Err(ProtocolError::ZeroRequestId);
        }
        if !is_batchable_opcode(opcode) {
            return Err(ProtocolError::BatchForbiddenOpcode(opcode));
        }
        if !request_ids.insert(request_id) {
            return Err(ProtocolError::DuplicateRequestId(request_id));
        }
        subframes.push(SubFrame {
            opcode,
            flags,
            request_id,
            payload: subframe_payload,
        });
        offset = payload_end;
    }
    if offset != payload.len() {
        return Err(ProtocolError::BatchTrailingBytes);
    }

    Ok(subframes)
}

fn validate_subframes(subframes: &[SubFrame<'_>], limits: BatchLimits) -> Result<()> {
    if subframes.is_empty() {
        return Err(ProtocolError::EmptyBatch);
    }
    if subframes.len() > limits.max_subframes {
        return Err(ProtocolError::BatchTooMany {
            count: subframes.len(),
            limit: limits.max_subframes,
        });
    }

    let mut total = 2_usize;
    let mut request_ids = BTreeSet::new();
    for subframe in subframes {
        if subframe.flags != 0 {
            return Err(ProtocolError::BatchSubframeFlags(subframe.flags));
        }
        if subframe.request_id == 0 {
            return Err(ProtocolError::ZeroRequestId);
        }
        if !is_batchable_opcode(subframe.opcode) {
            return Err(ProtocolError::BatchForbiddenOpcode(subframe.opcode));
        }
        if !request_ids.insert(subframe.request_id) {
            return Err(ProtocolError::DuplicateRequestId(subframe.request_id));
        }
        total = total
            .checked_add(SUBFRAME_HEADER_LEN)
            .and_then(|size| size.checked_add(subframe.payload.len()))
            .ok_or(ProtocolError::BatchTooLarge {
                size: usize::MAX,
                limit: limits.max_bytes,
            })?;
    }
    if total > limits.max_bytes {
        return Err(ProtocolError::BatchTooLarge {
            size: total,
            limit: limits.max_bytes,
        });
    }

    Ok(())
}
