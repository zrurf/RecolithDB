use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;

use crate::config::TlsClientConfig;
use crate::error::ClientError;

pub(crate) struct TlsSetup {
    pub connector: TlsConnector,
    pub server_name: ServerName<'static>,
}

pub(crate) fn setup(config: &TlsClientConfig) -> Result<TlsSetup, ClientError> {
    let mut reader = BufReader::new(File::open(&config.ca_certificates)?);
    let certificates = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(ClientError::Tls(
            "CA certificate file contains no certificates".to_owned(),
        ));
    }

    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| ClientError::Tls(error.to_string()))?;
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| ClientError::Tls(error.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from(config.server_name.as_str())
        .map_err(|error| ClientError::Tls(error.to_string()))?
        .to_owned();

    Ok(TlsSetup {
        connector: TlsConnector::from(Arc::new(client)),
        server_name,
    })
}
