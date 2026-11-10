use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::base64url;
use crate::hmac::{constant_time_eq, hmac_sha256};

/// Default token lifetime.
pub const DEFAULT_TTL_SECONDS: u64 = 86_400;
/// Maximum accepted token lifetime.
pub const MAX_TTL_SECONDS: u64 = 604_800;
/// Clock skew allowed when validating token timestamps.
pub const CLOCK_SKEW_SECONDS: u64 = 60;

const TOKEN_PREFIX: &str = "rclt1";
const MAX_TOKEN_LEN: usize = 4_096;
const MAX_SUBJECT_LEN: usize = 128;
const MAX_ROLES: usize = 32;
const MAX_ROLE_LEN: usize = 64;
const VERSION: u8 = 1;

/// Token claims carried by an opaque HMAC token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenClaims {
    /// Principal identifier.
    pub subject: String,
    /// Role names resolved by the server.
    pub roles: Vec<String>,
    /// Issued-at Unix seconds.
    pub issued_at: u64,
    /// Expiration Unix seconds.
    pub expires_at: u64,
}

/// Token encoding or verification failure.
#[derive(Debug, Error)]
pub enum TokenError {
    /// Secret file could not be read.
    #[error("failed to read secret {path}: {source}")]
    SecretRead {
        /// Secret path.
        path: PathBuf,
        /// I/O failure.
        source: std::io::Error,
    },
    /// Secret file is not 32 bytes of hexadecimal text.
    #[error("secret must contain exactly 64 hexadecimal characters")]
    InvalidSecret,
    /// Token structure is invalid.
    #[error("token is malformed")]
    Malformed,
    /// Token version is unsupported.
    #[error("token version is unsupported")]
    UnsupportedVersion,
    /// Token payload is invalid.
    #[error("token payload is invalid")]
    InvalidPayload,
    /// Token signature does not match.
    #[error("token signature is invalid")]
    InvalidSignature,
    /// Token has expired.
    #[error("token has expired")]
    Expired,
    /// Token is not valid yet.
    #[error("token is not valid yet")]
    NotYetValid,
    /// Token lifetime is invalid.
    #[error("token lifetime is invalid")]
    InvalidLifetime,
    /// Token lifetime exceeds the maximum.
    #[error("token lifetime exceeds {MAX_TTL_SECONDS} seconds")]
    LifetimeTooLong,
    /// Token subject is invalid.
    #[error("token subject is invalid")]
    InvalidSubject,
    /// Token roles are invalid.
    #[error("token roles are invalid")]
    InvalidRoles,
    /// No verification key is configured.
    #[error("no token verification key is configured")]
    NoKeys,
}

/// Signs opaque HMAC tokens.
#[derive(Clone, Debug)]
pub struct TokenIssuer {
    key: [u8; 32],
}

impl TokenIssuer {
    /// Creates an issuer from a 32-byte key.
    #[must_use]
    pub const fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Loads an issuer key from a hexadecimal secret file.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError`] when the file cannot be read or decoded.
    pub fn from_secret_file(path: impl AsRef<Path>) -> Result<Self, TokenError> {
        Ok(Self::new(load_secret(path)?))
    }

    /// Signs claims into an opaque token.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError`] for invalid claims or encoding failures.
    pub fn issue(&self, claims: &TokenClaims) -> Result<String, TokenError> {
        validate_claims(claims)?;
        let payload = encode_payload(claims);
        let payload = base64url::encode(&payload);
        let signing_input = format!("{TOKEN_PREFIX}.{payload}");
        let signature = hmac_sha256(&self.key, signing_input.as_bytes());
        let signature = base64url::encode(&signature);

        Ok(format!("{signing_input}.{signature}"))
    }
}

/// Verifies opaque HMAC tokens.
#[derive(Clone, Debug)]
pub struct TokenVerifier {
    keys: Vec<[u8; 32]>,
}

impl TokenVerifier {
    /// Creates a verifier from one or more keys.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError::NoKeys`] when `keys` is empty.
    pub fn new(keys: Vec<[u8; 32]>) -> Result<Self, TokenError> {
        if keys.is_empty() {
            return Err(TokenError::NoKeys);
        }

        Ok(Self { keys })
    }

    /// Loads an active key and an optional previous key.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError`] when a secret file cannot be read or decoded.
    pub fn from_secret_files(
        secret: impl AsRef<Path>,
        previous: Option<impl AsRef<Path>>,
    ) -> Result<Self, TokenError> {
        let mut keys = vec![load_secret(secret)?];
        if let Some(previous) = previous {
            keys.push(load_secret(previous)?);
        }

        Self::new(keys)
    }

    /// Decodes token claims without verifying the signature.
    ///
    /// Intended for diagnostics only. Never use the result for authorization.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError`] for malformed tokens or invalid claim shapes.
    pub fn decode_unverified(token: &str) -> Result<TokenClaims, TokenError> {
        let (_, payload, _) = split_token(token)?;
        let payload = base64url::decode(payload).ok_or(TokenError::Malformed)?;
        let claims = decode_payload(&payload)?;
        validate_claims(&claims)?;

        Ok(claims)
    }

    /// Verifies a token and validates its timestamps.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError`] for malformed tokens, invalid signatures, invalid
    /// lifetimes, or expired/not-yet-valid tokens.
    pub fn verify(&self, token: &str, now: u64) -> Result<TokenClaims, TokenError> {
        let (prefix, payload, signature) = split_token(token)?;
        let signature = base64url::decode(signature).ok_or(TokenError::Malformed)?;
        if signature.len() != 32 {
            return Err(TokenError::Malformed);
        }
        let signing_input = &token[..prefix.len() + 1 + payload.len()];
        let mut valid = false;
        for key in &self.keys {
            let expected = hmac_sha256(key, signing_input.as_bytes());
            valid |= constant_time_eq(&expected, &signature);
        }
        if !valid {
            return Err(TokenError::InvalidSignature);
        }

        let payload = base64url::decode(payload).ok_or(TokenError::Malformed)?;
        let claims = decode_payload(&payload)?;
        validate_claims(&claims)?;
        if claims.expires_at <= now {
            return Err(TokenError::Expired);
        }
        if claims.issued_at > now.saturating_add(CLOCK_SKEW_SECONDS) {
            return Err(TokenError::NotYetValid);
        }

        Ok(claims)
    }
}

/// Returns the current Unix time in seconds.
#[must_use]
pub fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn split_token(token: &str) -> Result<(&str, &str, &str), TokenError> {
    if token.len() > MAX_TOKEN_LEN {
        return Err(TokenError::Malformed);
    }

    let mut parts = token.split('.');
    let prefix = parts.next().ok_or(TokenError::Malformed)?;
    let payload = parts.next().ok_or(TokenError::Malformed)?;
    let signature = parts.next().ok_or(TokenError::Malformed)?;
    if parts.next().is_some() || prefix != TOKEN_PREFIX {
        return Err(TokenError::Malformed);
    }

    Ok((prefix, payload, signature))
}

fn load_secret(path: impl AsRef<Path>) -> Result<[u8; 32], TokenError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|source| TokenError::SecretRead {
        path: path.to_path_buf(),
        source,
    })?;
    let contents = contents.trim();
    if contents.len() != 64 {
        return Err(TokenError::InvalidSecret);
    }

    let mut key = [0_u8; 32];
    let bytes = contents.as_bytes();
    for index in 0..32 {
        let high = hex_nibble(bytes[index * 2]).ok_or(TokenError::InvalidSecret)?;
        let low = hex_nibble(bytes[index * 2 + 1]).ok_or(TokenError::InvalidSecret)?;
        key[index] = (high << 4) | low;
    }

    Ok(key)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn validate_claims(claims: &TokenClaims) -> Result<(), TokenError> {
    if claims.subject.is_empty()
        || claims.subject.len() > MAX_SUBJECT_LEN
        || claims.subject.chars().any(char::is_control)
    {
        return Err(TokenError::InvalidSubject);
    }
    if claims.roles.is_empty() || claims.roles.len() > MAX_ROLES {
        return Err(TokenError::InvalidRoles);
    }
    let mut seen = BTreeSet::new();
    for role in &claims.roles {
        if role.is_empty()
            || role.len() > MAX_ROLE_LEN
            || role.chars().any(char::is_control)
            || !seen.insert(role)
        {
            return Err(TokenError::InvalidRoles);
        }
    }
    if claims.expires_at <= claims.issued_at {
        return Err(TokenError::InvalidLifetime);
    }
    if claims.expires_at - claims.issued_at > MAX_TTL_SECONDS {
        return Err(TokenError::LifetimeTooLong);
    }

    Ok(())
}

fn encode_payload(claims: &TokenClaims) -> Vec<u8> {
    let mut output = Vec::with_capacity(64 + claims.subject.len() + claims.roles.len() * 16);
    output.push(VERSION);
    output.extend_from_slice(&claims.issued_at.to_be_bytes());
    output.extend_from_slice(&claims.expires_at.to_be_bytes());
    put_string(&mut output, &claims.subject);
    output.extend_from_slice(&(claims.roles.len() as u16).to_be_bytes());
    for role in &claims.roles {
        put_string(&mut output, role);
    }

    output
}

fn put_string(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(&(value.len() as u16).to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

fn decode_payload(payload: &[u8]) -> Result<TokenClaims, TokenError> {
    let mut cursor = 0;
    let version = read_u8(payload, &mut cursor).ok_or(TokenError::InvalidPayload)?;
    if version != VERSION {
        return Err(TokenError::UnsupportedVersion);
    }
    let issued_at = read_u64(payload, &mut cursor).ok_or(TokenError::InvalidPayload)?;
    let expires_at = read_u64(payload, &mut cursor).ok_or(TokenError::InvalidPayload)?;
    let subject = read_string(payload, &mut cursor).ok_or(TokenError::InvalidPayload)?;
    let role_count = read_u16(payload, &mut cursor).ok_or(TokenError::InvalidPayload)?;
    let mut roles = Vec::with_capacity(usize::from(role_count));
    for _ in 0..role_count {
        roles.push(read_string(payload, &mut cursor).ok_or(TokenError::InvalidPayload)?);
    }
    if cursor != payload.len() {
        return Err(TokenError::InvalidPayload);
    }

    Ok(TokenClaims {
        subject,
        roles,
        issued_at,
        expires_at,
    })
}

fn read_u8(payload: &[u8], cursor: &mut usize) -> Option<u8> {
    let value = *payload.get(*cursor)?;
    *cursor += 1;

    Some(value)
}

fn read_u16(payload: &[u8], cursor: &mut usize) -> Option<u16> {
    let bytes = payload.get(*cursor..*cursor + 2)?;
    *cursor += 2;

    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> Option<u64> {
    let bytes = payload.get(*cursor..*cursor + 8)?;
    *cursor += 8;

    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

fn read_string(payload: &[u8], cursor: &mut usize) -> Option<String> {
    let length = usize::from(read_u16(payload, cursor)?);
    let bytes = payload.get(*cursor..*cursor + length)?;
    *cursor += length;

    String::from_utf8(bytes.to_vec()).ok()
}
