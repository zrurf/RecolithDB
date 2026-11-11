//! Opaque query pagination cursors.

use prost::Message;
use recolith_protocol::messages::QueryRequest;
use recolith_storage::crc32c;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 4] = b"RCLC";
const VERSION: u8 = 2;
const HEADER_LEN: usize = 4 + 1;
const PREFIX_LEN: usize = HEADER_LEN + 4;
const MIN_LEN: usize = PREFIX_LEN + 4 + 8 + 32 + 4;
const NO_ORDER_VALUE: u32 = u32::MAX;

/// Decoded pagination cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryCursor {
    pub(crate) last_key: Vec<u8>,
    pub(crate) order_value: Option<Vec<u8>>,
    pub(crate) catalog_generation: u64,
    pub(crate) query_hash: [u8; 32],
}

/// Cursor encoding or validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CursorError {
    Malformed,
    UnsupportedVersion,
    QueryMismatch,
    StaleCatalog,
}

pub(crate) fn encode(cursor: &QueryCursor) -> Vec<u8> {
    let mut output = Vec::with_capacity(
        MIN_LEN + cursor.last_key.len() + cursor.order_value.as_ref().map_or(0, Vec::len),
    );
    output.extend_from_slice(MAGIC);
    output.push(VERSION);
    let key_len = u32::try_from(cursor.last_key.len()).expect("cursor key fits in u32");
    output.extend_from_slice(&key_len.to_be_bytes());
    output.extend_from_slice(&cursor.last_key);
    match &cursor.order_value {
        Some(value) => {
            let order_len = u32::try_from(value.len()).expect("order value fits in u32");
            output.extend_from_slice(&order_len.to_be_bytes());
            output.extend_from_slice(value);
        }
        None => output.extend_from_slice(&NO_ORDER_VALUE.to_be_bytes()),
    }
    output.extend_from_slice(&cursor.catalog_generation.to_be_bytes());
    output.extend_from_slice(&cursor.query_hash);
    let checksum = crc32c(&output);
    output.extend_from_slice(&checksum.to_be_bytes());

    output
}

pub(crate) fn decode(bytes: &[u8]) -> Result<QueryCursor, CursorError> {
    if bytes.len() < MIN_LEN {
        return Err(CursorError::Malformed);
    }
    let (body, checksum) = bytes.split_at(bytes.len() - 4);
    let expected = u32::from_be_bytes(checksum.try_into().expect("four bytes"));
    if crc32c(body) != expected {
        return Err(CursorError::Malformed);
    }
    if body.get(..4) != Some(MAGIC.as_slice()) {
        return Err(CursorError::Malformed);
    }
    if body.get(4) != Some(&VERSION) {
        return Err(CursorError::UnsupportedVersion);
    }

    let mut offset = HEADER_LEN;
    let key_len = read_u32(body, offset)? as usize;
    offset += 4;
    let key_end = offset.checked_add(key_len).ok_or(CursorError::Malformed)?;
    let last_key = body
        .get(offset..key_end)
        .ok_or(CursorError::Malformed)?
        .to_vec();
    offset = key_end;

    let order_len = read_u32(body, offset)?;
    offset += 4;
    let order_value = if order_len == NO_ORDER_VALUE {
        None
    } else {
        let order_end = offset
            .checked_add(order_len as usize)
            .ok_or(CursorError::Malformed)?;
        let value = body
            .get(offset..order_end)
            .ok_or(CursorError::Malformed)?
            .to_vec();
        offset = order_end;
        Some(value)
    };

    let generation = body.get(offset..offset + 8).ok_or(CursorError::Malformed)?;
    let catalog_generation = u64::from_be_bytes(generation.try_into().expect("eight bytes"));
    offset += 8;
    let query_hash = body
        .get(offset..offset + 32)
        .ok_or(CursorError::Malformed)?;
    offset += 32;
    if body.len() != offset {
        return Err(CursorError::Malformed);
    }
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(query_hash);

    Ok(QueryCursor {
        last_key,
        order_value,
        catalog_generation,
        query_hash: hash,
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, CursorError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or(CursorError::Malformed)?;

    Ok(u32::from_be_bytes(value.try_into().expect("four bytes")))
}

/// Hashes the query shape that a cursor may resume.
pub(crate) fn query_hash(
    partition: u32,
    rql: &str,
    parameters: &[recolith_protocol::messages::NamedParameter],
) -> [u8; 32] {
    let canonical = QueryRequest {
        meta: None,
        partition,
        rql: rql.to_owned(),
        parameters: parameters.to_vec(),
        limit: 0,
        cursor: Vec::new(),
    };
    let digest = Sha256::digest(canonical.encode_to_vec());
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);

    output
}
