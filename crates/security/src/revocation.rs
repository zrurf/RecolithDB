//! Revoked token fingerprint list.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::roles::SecurityError;
use crate::token::fingerprint_hex;

/// Set of revoked token SHA-256 fingerprints.
#[derive(Clone, Debug, Default)]
pub struct RevocationList {
    hashes: BTreeSet<[u8; 32]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevocationFile {
    #[serde(default)]
    hashes: Vec<String>,
}

impl RevocationList {
    /// Loads a revocation list file.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] for I/O, TOML, or fingerprint format failures.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, SecurityError> {
        let path = path.as_ref();
        let contents =
            fs::read_to_string(path).map_err(|source| SecurityError::RevocationRead {
                path: path.to_path_buf(),
                source,
            })?;

        Self::parse(&contents)
    }

    /// Parses a revocation list document.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] for TOML or fingerprint format failures.
    pub fn parse(contents: &str) -> Result<Self, SecurityError> {
        let file: RevocationFile =
            toml::from_str(contents).map_err(SecurityError::RevocationParse)?;
        let mut hashes = BTreeSet::new();
        for hash in file.hashes {
            hashes.insert(parse_fingerprint(&hash)?);
        }

        Ok(Self { hashes })
    }

    /// Returns whether a fingerprint is revoked.
    #[must_use]
    pub fn contains(&self, fingerprint: &[u8; 32]) -> bool {
        self.hashes.contains(fingerprint)
    }

    /// Returns the number of revoked fingerprints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Returns whether no fingerprints are revoked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Adds a fingerprint and returns whether it was new.
    pub fn insert(&mut self, fingerprint: [u8; 32]) -> bool {
        self.hashes.insert(fingerprint)
    }

    /// Removes a fingerprint and returns whether it was present.
    pub fn remove(&mut self, fingerprint: &[u8; 32]) -> bool {
        self.hashes.remove(fingerprint)
    }

    /// Serializes the list as a TOML document.
    #[must_use]
    pub fn to_toml_string(&self) -> String {
        let mut output = String::from("hashes = [\n");
        for fingerprint in &self.hashes {
            output.push_str("  \"");
            output.push_str(&fingerprint_hex(fingerprint));
            output.push_str("\",\n");
        }
        output.push_str("]\n");

        output
    }
}

fn parse_fingerprint(value: &str) -> Result<[u8; 32], SecurityError> {
    let bytes = value.as_bytes();
    if bytes.len() != 64 {
        return Err(SecurityError::InvalidRevocation(value.to_owned()));
    }

    let mut fingerprint = [0_u8; 32];
    for index in 0..32 {
        let high = hex_nibble(bytes[index * 2])
            .ok_or_else(|| SecurityError::InvalidRevocation(value.to_owned()))?;
        let low = hex_nibble(bytes[index * 2 + 1])
            .ok_or_else(|| SecurityError::InvalidRevocation(value.to_owned()))?;
        fingerprint[index] = (high << 4) | low;
    }

    Ok(fingerprint)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
