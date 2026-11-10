//! Server composition root, in-process document services, and TCP sessions.
#![forbid(unsafe_code)]

pub mod backup;
pub mod bootstrap;
pub mod config;
pub mod document;
pub mod metrics;
pub mod net;

pub use backup::{BackupError, BackupStats, backup, restore, verify};
pub use bootstrap::{BootstrapError, load_bootstrap, parse_bootstrap};
pub use config::{
    AuthSettings, CatalogSettings, Config, ConfigError, ServerSettings, StorageSettings,
    TlsSettings,
};
pub use document::{
    DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest, Idempotency,
};
pub use metrics::{MetricsSnapshot, ServerMetrics};
pub use net::{SERVER_VERSION, ServerConfig, TlsServerConfig, serve, serve_with_shutdown};
