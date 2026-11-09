use std::fs::File;
use std::io::{self, BufReader};
use std::sync::Arc;

use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use super::TlsServerConfig;

pub(crate) fn acceptor(config: &TlsServerConfig) -> io::Result<TlsAcceptor> {
    let mut certificate_reader = BufReader::new(File::open(&config.certificate_chain)?);
    let certificates =
        rustls_pemfile::certs(&mut certificate_reader).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(invalid("TLS certificate chain is empty"));
    }

    let mut key_reader = BufReader::new(File::open(&config.private_key)?);
    let private_key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| invalid("TLS private key is missing"))?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| invalid(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|error| invalid(error.to_string()))?;

    Ok(TlsAcceptor::from(Arc::new(server)))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
