//! Server composition root, in-process document services, and TCP sessions.
#![forbid(unsafe_code)]

pub mod backup;
pub mod bootstrap;
pub mod config;
pub mod document;
pub mod metrics;
pub mod migration;
pub mod net;
pub mod observability;
pub mod offline_migration;
mod query_cursor;
pub mod reaper;
pub mod workflow;

pub use backup::{BackupError, BackupStats, backup, backup_snapshot, restore, verify};
pub use bootstrap::{BootstrapError, load_bootstrap, parse_bootstrap};
pub use config::{
    AuthSettings, BackupSettings, CatalogSettings, Config, ConfigError, ObservabilitySettings,
    PlacementSettings, ServerSettings, StorageSettings, TlsSettings, TtlSettings,
    WorkflowExecution, WorkflowSettings,
};
pub use document::{
    BucketRecordData, CounterChange, DocumentError, DocumentStore, DocumentWrite,
    FragmentWriteRequest, Idempotency, ReconcileStats, TtlSweep,
};
pub use metrics::{MetricsSnapshot, ServerMetrics};
pub use migration::{
    FieldMapping, MigrationConversion, MigrationError, MigrationSource, SchemaMigration,
    migrate_record,
};
pub use net::{SERVER_VERSION, ServerConfig, TlsServerConfig, serve, serve_with_shutdown};
pub use offline_migration::{
    DEFAULT_MIGRATION_BATCH, MigrationBatch, MigrationStats, OfflineMigration,
    OfflineMigrationError, migration_in_progress,
};
pub use reaper::run_ttl_reaper;
pub use workflow::{MAX_WORKFLOW_ID_LEN, MAX_WORKFLOW_STEPS, WorkflowError};
