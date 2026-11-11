use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use recolith_protocol::frame::HEADER_LEN;
use recolith_security::Authenticator;
use serde::Deserialize;
use thiserror::Error;

use crate::net::ServerConfig;

/// Server startup configuration loaded from TOML.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Listener and protocol limits.
    pub server: ServerSettings,
    /// Backend selection.
    pub storage: StorageSettings,
    /// Optional TLS settings.
    #[serde(default)]
    pub tls: Option<TlsSettings>,
    /// Optional catalog bootstrap settings.
    #[serde(default)]
    pub catalog: CatalogSettings,
    /// Optional token and role authentication settings.
    #[serde(default)]
    pub auth: Option<AuthSettings>,
    /// Optional online backup settings.
    #[serde(default)]
    pub backup: BackupSettings,
    /// Document expiry reaper settings.
    #[serde(default)]
    pub ttl: TtlSettings,
}

/// Listener and protocol settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSettings {
    /// `host:port` listener address.
    pub listen: String,
    /// Optional shared bearer token.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Allows plaintext non-loopback sessions.
    #[serde(default)]
    pub allow_plaintext_remote: bool,
    /// Maximum frame payload size.
    #[serde(default = "default_max_frame_size")]
    pub max_frame_size: usize,
    /// Maximum concurrent TCP sessions. Zero means unlimited.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Maximum number of subframes in a batch.
    #[serde(default = "default_max_batch_subframes")]
    pub max_batch_subframes: usize,
    /// Maximum batch payload size.
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Maximum decompressed payload size.
    #[serde(default = "default_max_decompressed_size")]
    pub max_decompressed_size: usize,
    /// Compression activation threshold.
    #[serde(default = "default_compression_threshold")]
    pub compression_threshold: usize,
}

/// Backend selection.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum StorageSettings {
    /// Volatile in-memory backend.
    Memory,
    /// Persistent fjall backend.
    Fjall {
        /// Database directory.
        path: PathBuf,
    },
}

/// TLS certificate and key paths.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSettings {
    /// PEM certificate chain.
    pub certificate_chain: PathBuf,
    /// PEM private key.
    pub private_key: PathBuf,
}

/// Token and role authentication settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSettings {
    /// Hexadecimal HMAC secret.
    pub secret_file: PathBuf,
    /// Optional previous secret accepted during rotation.
    #[serde(default)]
    pub previous_secret_file: Option<PathBuf>,
    /// Role and permission definitions.
    pub roles_file: PathBuf,
    /// Optional revoked token fingerprint list.
    #[serde(default)]
    pub revoked_tokens_file: Option<PathBuf>,
}

/// Catalog startup settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSettings {
    /// Metadata used only when the persistent catalog is empty.
    #[serde(default)]
    pub bootstrap: Option<PathBuf>,
}

/// Online backup settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupSettings {
    /// Directory that may receive online logical backups.
    #[serde(default)]
    pub directory: Option<PathBuf>,
}

/// Document expiry reaper settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TtlSettings {
    /// Sweep interval in seconds. Zero disables the reaper.
    #[serde(default = "default_ttl_sweep_interval")]
    pub sweep_interval_secs: u64,
    /// Maximum expired documents processed per sweep.
    #[serde(default = "default_ttl_batch_size")]
    pub batch_size: usize,
}

impl Default for TtlSettings {
    fn default() -> Self {
        Self {
            sweep_interval_secs: default_ttl_sweep_interval(),
            batch_size: default_ttl_batch_size(),
        }
    }
}

/// Configuration loading or validation failure.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("failed to read configuration {path}: {source}")]
    Read {
        /// Configuration path.
        path: PathBuf,
        /// I/O failure.
        source: std::io::Error,
    },
    /// The configuration file is not valid TOML.
    #[error("invalid configuration {path}: {source}")]
    Parse {
        /// Configuration path.
        path: PathBuf,
        /// TOML failure.
        source: toml::de::Error,
    },
    /// A configuration value is invalid.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl Config {
    /// Loads and validates a configuration file.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for I/O, TOML, or semantic failures.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let mut config: Self = toml::from_str(&contents).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        config.resolve_paths(base);
        config.validate()?;

        Ok(config)
    }

    /// Returns the configured listener address.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] when the address cannot be parsed.
    pub fn listen_addr(&self) -> Result<SocketAddr, ConfigError> {
        self.server.listen.parse().map_err(|error| {
            ConfigError::Invalid(format!(
                "server.listen `{}` is not a socket address: {error}",
                self.server.listen
            ))
        })
    }

    /// Builds the network server configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the listener address is invalid.
    pub fn server_config(&self) -> Result<ServerConfig, ConfigError> {
        let mut config = ServerConfig::new(self.listen_addr()?);
        config.auth_token = self
            .server
            .auth_token
            .as_ref()
            .map(|token| token.as_bytes().to_vec());
        config.allow_plaintext_remote = self.server.allow_plaintext_remote;
        config.max_frame_size = self.server.max_frame_size;
        config.max_connections = self.server.max_connections;
        config.max_batch_subframes = self.server.max_batch_subframes;
        config.max_batch_bytes = self.server.max_batch_bytes;
        config.max_decompressed_size = self.server.max_decompressed_size;
        config.compression_threshold = self.server.compression_threshold;
        if let Some(auth) = &self.auth {
            let authenticator = Authenticator::from_files_with_revocations(
                &auth.secret_file,
                auth.previous_secret_file.as_deref(),
                &auth.roles_file,
                auth.revoked_tokens_file.as_deref(),
            )
            .map_err(|error| ConfigError::Invalid(format!("authentication: {error}")))?;
            config = config.with_auth(authenticator);
        }
        if let Some(tls) = &self.tls {
            config = config.with_tls(&tls.certificate_chain, &tls.private_key);
        }

        Ok(config)
    }

    fn resolve_paths(&mut self, base: &Path) {
        match &mut self.storage {
            StorageSettings::Memory => {}
            StorageSettings::Fjall { path } => resolve(base, path),
        }
        if let Some(tls) = &mut self.tls {
            resolve(base, &mut tls.certificate_chain);
            resolve(base, &mut tls.private_key);
        }
        if let Some(bootstrap) = &mut self.catalog.bootstrap {
            resolve(base, bootstrap);
        }
        if let Some(directory) = &mut self.backup.directory {
            resolve(base, directory);
        }
        if let Some(auth) = &mut self.auth {
            resolve(base, &mut auth.secret_file);
            if let Some(previous) = &mut auth.previous_secret_file {
                resolve(base, previous);
            }
            resolve(base, &mut auth.roles_file);
            if let Some(revoked) = &mut auth.revoked_tokens_file {
                resolve(base, revoked);
            }
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.listen_addr()?;
        if self
            .server
            .auth_token
            .as_ref()
            .is_some_and(String::is_empty)
        {
            return Err(ConfigError::Invalid(
                "server.auth_token must not be empty".to_owned(),
            ));
        }
        if self.auth.is_some() && self.server.auth_token.is_some() {
            return Err(ConfigError::Invalid(
                "server.auth_token and [auth] cannot both be configured".to_owned(),
            ));
        }
        if self.server.allow_plaintext_remote
            && self.tls.is_none()
            && self.server.auth_token.is_none()
            && self.auth.is_none()
        {
            return Err(ConfigError::Invalid(
                "remote plaintext requires TLS or an auth token".to_owned(),
            ));
        }
        if self.server.max_frame_size < HEADER_LEN {
            return Err(ConfigError::Invalid(format!(
                "server.max_frame_size must be at least {HEADER_LEN}"
            )));
        }
        if self.server.max_batch_subframes == 0 {
            return Err(ConfigError::Invalid(
                "server.max_batch_subframes must be greater than zero".to_owned(),
            ));
        }
        if self.server.max_batch_bytes == 0 {
            return Err(ConfigError::Invalid(
                "server.max_batch_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.server.max_decompressed_size == 0 {
            return Err(ConfigError::Invalid(
                "server.max_decompressed_size must be greater than zero".to_owned(),
            ));
        }
        if let Some(directory) = &self.backup.directory {
            if directory.as_os_str().is_empty() {
                return Err(ConfigError::Invalid(
                    "backup.directory must not be empty".to_owned(),
                ));
            }
            if directory.exists() && !directory.is_dir() {
                return Err(ConfigError::Invalid(format!(
                    "backup.directory is not a directory: {}",
                    directory.display()
                )));
            }
        }
        if self.ttl.sweep_interval_secs > 0 && self.ttl.batch_size == 0 {
            return Err(ConfigError::Invalid(
                "ttl.batch_size must be greater than zero".to_owned(),
            ));
        }

        Ok(())
    }
}

fn resolve(base: &Path, path: &mut PathBuf) {
    if path.is_relative() {
        *path = base.join(&*path);
    }
}

const fn default_max_frame_size() -> usize {
    16 * 1024 * 1024
}

const fn default_max_connections() -> usize {
    1024
}

const fn default_max_batch_subframes() -> usize {
    64
}

const fn default_max_batch_bytes() -> usize {
    1024 * 1024
}

const fn default_max_decompressed_size() -> usize {
    16 * 1024 * 1024
}

const fn default_compression_threshold() -> usize {
    512
}

const fn default_ttl_sweep_interval() -> u64 {
    60
}

const fn default_ttl_batch_size() -> usize {
    1_000
}
