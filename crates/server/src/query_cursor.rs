//! Opaque query pagination cursors.

use prost::Message;
use recolith_protocol::messages::QueryRequest;
use recolith_storage::crc32c;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 4] = b"RCLC";
const VERSION: u8 = 1;
const FIXED_LEN: usize = 4 + 1 + 2 + 8 + 32 + 4;

/// Decoded pagination cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryCursor {
    pub(crate) last_key: Vec<u8>,
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
    let mut output = Vec::with_capacity(FIXED_LEN + cursor.last_key.len());
    output.extend_from_slice(MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&(cursor.last_key.len() as u16).to_be_bytes());
    output.extend_from_slice(&cursor.last_key);
    output.extend_from_slice(&cursor.catalog_generation.to_be_bytes());
    output.extend_from_slice(&cursor.query_hash);
    let checksum = crc32c(&output);
    output.extend_from_slice(&checksum.to_be_bytes());

    output
}

pub(crate) fn decode(bytes: &[u8]) -> Result<QueryCursor, CursorError> {
    if bytes.len() < FIXED_LEN {
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
    let key_len = u16::from_be_bytes(
        body.get(5..7)
            .ok_or(CursorError::Malformed)?
            .try_into()
            .expect("two bytes"),
    ) as usize;
    let key_start: usize = 7;
    let key_end = key_start
        .checked_add(key_len)
        .ok_or(CursorError::Malformed)?;
    let last_key = body
        .get(key_start..key_end)
        .ok_or(CursorError::Malformed)?
        .to_vec();
    let generation = body
        .get(key_end..key_end + 8)
        .ok_or(CursorError::Malformed)?;
    let catalog_generation = u64::from_be_bytes(generation.try_into().expect("eight bytes"));
    let query_hash = body
        .get(key_end + 8..key_end + 40)
        .ok_or(CursorError::Malformed)?;
    if body.len() != key_end + 40 {
        return Err(CursorError::Malformed);
    }
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(query_hash);

    Ok(QueryCursor {
        last_key,
        catalog_generation,
        query_hash: hash,
    })
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
