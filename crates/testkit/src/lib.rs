//! Test fixtures shared by integration tests.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

/// Returns the directory containing TLS fixtures.
#[must_use]
pub fn tls_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/tls")
}

/// Returns the test certificate authority.
#[must_use]
pub fn tls_ca_certificate() -> PathBuf {
    tls_dir().join("ca.pem")
}

/// Returns the test server certificate chain.
#[must_use]
pub fn tls_server_certificate() -> PathBuf {
    tls_dir().join("server.pem")
}

/// Returns the test server private key.
#[must_use]
pub fn tls_server_key() -> PathBuf {
    tls_dir().join("server.key")
}
