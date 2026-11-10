//! Offline bucket schema migration for datasets beyond the online limit.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound;

use recolith_backend::{KeyRange, ScanOptions, Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_catalog::{Catalog, CatalogAlter, CatalogError, load_catalog, save_catalog};
use recolith_common::{BucketId, SchemaVersion};
use recolith_model::{IndexDefinition, IndexKind};
use recolith_schema::{Compatibility, RecordSchema, check_compatibility};
use recolith_storage::{
    RecordData, RecordHeader, StorageError, decode_fields, decode_record, decode_record_key,
    delete_index_entry, encode_bucket_prefix, encode_fields, encode_index_value, encode_record,
    put_index_entry, put_unique_index_entry,
};
use thiserror::Error;

use crate::migration::{MigrationError, SchemaMigration, migrate_record};

/// Prefix for offline migration markers.
pub const MIGRATION_MARKER_PREFIX: &[u8] = b"\0recolith/maintenance/migration/";
/// Default records examined per batch.
pub const DEFAULT_MIGRATION_BATCH: usize = 1_000;

/// Offline migration progress.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MigrationStats {
    /// Records rewritten to the target schema.
    pub migrated: u64,
    /// Records already on the target schema.
    pub skipped: u64,
    /// Committed batches.
    pub batches: u64,
}

/// One committed migration batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationBatch {
    /// Records rewritten in this batch.
    pub migrated: u64,
    /// Records already on the target schema.
    pub skipped: u64,
    /// Whether the bucket scan is complete.
    pub done: bool,
}

/// Offline migration failure.
#[derive(Debug, Error)]
pub enum OfflineMigrationError {
    /// Backend access failed.
    #[error("migration backend error: {0}")]
    Backend(#[from] recolith_backend::BackendError),
    /// Catalog access failed.
    #[error("migration catalog error: {0}")]
    Catalog(#[from] CatalogError),
    /// Schema compatibility check failed.
    #[error("migration schema error: {0}")]
    Schema(#[from] recolith_schema::SchemaError),
    /// Storage encoding failed.
    #[error("migration storage error: {0}")]
    Storage(#[from] StorageError),
    /// Field migration failed.
    #[error("migration error: {0}")]
    Migration(#[from] MigrationError),
    /// The target version is lower than the current version.
    #[error("target schema version {target} is below current version {current}")]
    TargetBelowCurrent {
        /// Current version.
        current: u32,
        /// Requested version.
        target: u32,
    },
    /// A migration that requires mappings was requested without them.
    #[error("schema change requires explicit field mappings")]
    MigrationRequired,
    /// The migration has not scanned the whole bucket.
    #[error("offline migration is incomplete")]
    Incomplete,
}

/// Resumable offline bucket migration.
pub struct OfflineMigration<B: StorageBackend> {
    backend: B,
    catalog: Catalog,
    bucket: BucketId,
    target: SchemaVersion,
    migration: SchemaMigration,
    schemas: BTreeMap<SchemaVersion, RecordSchema>,
    target_schema: RecordSchema,
    indexes: Vec<IndexDefinition>,
    last_key: Option<Vec<u8>>,
    done: bool,
    changed: bool,
    stats: MigrationStats,
}

impl<B: StorageBackend> fmt::Debug for OfflineMigration<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfflineMigration")
            .field("bucket", &self.bucket)
            .field("target", &self.target)
            .field("done", &self.done)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl<B: StorageBackend> OfflineMigration<B> {
    /// Opens a bucket migration and marks the database as under maintenance.
    ///
    /// # Errors
    ///
    /// Returns [`OfflineMigrationError`] for missing metadata, invalid targets,
    /// incompatible schemas without mappings, or backend failures.
    pub fn open(
        backend: B,
        bucket: BucketId,
        target: SchemaVersion,
        migration: SchemaMigration,
    ) -> Result<Self, OfflineMigrationError> {
        let catalog = load_catalog(&backend)?;
        let snapshot = catalog.snapshot()?;
        let bucket_meta = snapshot
            .buckets
            .get(&bucket)
            .ok_or(CatalogError::NotFound("bucket"))?
            .clone();
        let current = bucket_meta.schema_version();
        if target < current {
            return Err(OfflineMigrationError::TargetBelowCurrent {
                current: current.get(),
                target: target.get(),
            });
        }
        if target == current {
            remove_marker(&backend, bucket)?;
            let schema = snapshot
                .schemas
                .schemas
                .get(&(bucket_meta.schema_id(), target))
                .ok_or(CatalogError::NotFound("schema"))?
                .clone();
            return Ok(Self {
                backend,
                catalog,
                bucket,
                target,
                migration,
                schemas: BTreeMap::new(),
                target_schema: schema,
                indexes: Vec::new(),
                last_key: None,
                done: true,
                changed: false,
                stats: MigrationStats::default(),
            });
        }

        let current_schema = snapshot
            .schemas
            .schemas
            .get(&(bucket_meta.schema_id(), current))
            .ok_or(CatalogError::NotFound("schema"))?;
        let target_schema = snapshot
            .schemas
            .schemas
            .get(&(bucket_meta.schema_id(), target))
            .ok_or(CatalogError::NotFound("schema"))?
            .clone();
        if check_compatibility(current_schema, &target_schema)? == Compatibility::RequiresMigration
            && migration.mappings.is_empty()
        {
            return Err(OfflineMigrationError::MigrationRequired);
        }
        let schemas = snapshot
            .schemas
            .schemas
            .iter()
            .filter(|((schema_id, _), _)| *schema_id == bucket_meta.schema_id())
            .map(|((_, version), schema)| (*version, schema.clone()))
            .collect();
        let indexes = snapshot
            .indexes
            .get(&bucket)
            .map(|indexes| indexes.values().cloned().collect())
            .unwrap_or_default();
        write_marker(&backend, bucket, target)?;

        Ok(Self {
            backend,
            catalog,
            bucket,
            target,
            migration,
            schemas,
            target_schema,
            indexes,
            last_key: None,
            done: false,
            changed: true,
            stats: MigrationStats::default(),
        })
    }

    /// Migrates one batch.
    ///
    /// # Errors
    ///
    /// Returns [`OfflineMigrationError`] for decoding, migration, index, or
    /// backend failures.
    pub fn run_batch(
        &mut self,
        batch_size: usize,
    ) -> Result<MigrationBatch, OfflineMigrationError> {
        if self.done {
            return Ok(MigrationBatch {
                migrated: 0,
                skipped: 0,
                done: true,
            });
        }
        let batch_size = batch_size.max(1);
        let prefix = encode_bucket_prefix(self.bucket);
        let end = KeyRange::prefix(&prefix).end_bound().clone();
        let range = match &self.last_key {
            Some(last) => KeyRange::new(Bound::Excluded(last.clone()), end),
            None => KeyRange::prefix(&prefix),
        };
        let mut transaction = self.backend.begin(TxnOptions::default())?;
        let entries = transaction.scan(
            &range,
            ScanOptions {
                limit: Some(batch_size),
                reverse: false,
            },
        )?;
        if entries.is_empty() {
            transaction.rollback();
            self.done = true;
            return Ok(MigrationBatch {
                migrated: 0,
                skipped: 0,
                done: true,
            });
        }
        let last = entries.last().expect("non-empty scan").0.clone();
        let mut migrated = 0_u64;
        let mut skipped = 0_u64;
        for (key, value) in &entries {
            let Ok(decoded) = decode_record_key(key) else {
                continue;
            };
            let record = decode_record(value)?;
            if record.header().schema_version() == self.target {
                skipped += 1;
                continue;
            }
            let source = self.source_schema(record.header())?;
            let old_data = decode_fields(&source, record.payload())?;
            let new_data = migrate_record(&old_data, &self.target_schema, &self.migration)?;
            let header = RecordHeader::new(
                self.target_schema.id(),
                self.target,
                record.header().commit_ts(),
            )
            .with_flags(record.header().flags())
            .with_expire_at(record.header().expire_at());
            let payload = encode_fields(&self.target_schema, &new_data)?;
            let frame = encode_record(&header, &payload)?;
            transaction.put(key, frame);
            update_indexes(
                &mut transaction,
                self.bucket,
                decoded.head().partition(),
                key,
                &old_data,
                &new_data,
                &self.indexes,
            )?;
            migrated += 1;
        }
        transaction.commit()?;
        self.last_key = Some(last);
        self.stats.migrated += migrated;
        self.stats.skipped += skipped;
        self.stats.batches += 1;
        self.done = entries.len() < batch_size;

        Ok(MigrationBatch {
            migrated,
            skipped,
            done: self.done,
        })
    }

    /// Completes the migration, updates the catalog, and clears the marker.
    ///
    /// # Errors
    ///
    /// Returns [`OfflineMigrationError::Incomplete`] before the bucket is fully
    /// scanned, or [`OfflineMigrationError`] for catalog or backend failures.
    pub fn finish(self) -> Result<MigrationStats, OfflineMigrationError> {
        if !self.done {
            return Err(OfflineMigrationError::Incomplete);
        }
        if self.changed {
            let updated = self.catalog.alter_object(CatalogAlter::BucketSchema {
                bucket: self.bucket,
                version: self.target,
            })?;
            save_catalog(&self.backend, &updated)?;
        }
        remove_marker(&self.backend, self.bucket)?;

        Ok(self.stats)
    }

    /// Runs all batches and finishes the migration.
    ///
    /// # Errors
    ///
    /// Returns [`OfflineMigrationError`] for batch, catalog, or backend failures.
    pub fn migrate(mut self, batch_size: usize) -> Result<MigrationStats, OfflineMigrationError> {
        while !self.done {
            self.run_batch(batch_size)?;
        }

        self.finish()
    }

    fn source_schema(&self, header: &RecordHeader) -> Result<RecordSchema, OfflineMigrationError> {
        if header.schema_id() != self.target_schema.id() {
            return Err(OfflineMigrationError::Storage(
                StorageError::SchemaMismatch {
                    record_schema: header.schema_id(),
                    record_version: header.schema_version(),
                    expected_schema: self.target_schema.id(),
                    expected_version: self.target,
                },
            ));
        }
        self.schemas
            .get(&header.schema_version())
            .cloned()
            .ok_or_else(|| {
                OfflineMigrationError::Storage(StorageError::SchemaMismatch {
                    record_schema: header.schema_id(),
                    record_version: header.schema_version(),
                    expected_schema: self.target_schema.id(),
                    expected_version: self.target,
                })
            })
    }
}

/// Returns whether an offline migration marker exists.
///
/// # Errors
///
/// Returns [`recolith_backend::BackendError`] when the scan fails.
pub fn migration_in_progress<B: StorageBackend>(
    backend: &B,
) -> Result<bool, recolith_backend::BackendError> {
    let entries = backend.snapshot()?.scan(
        &KeyRange::prefix(MIGRATION_MARKER_PREFIX),
        ScanOptions {
            limit: Some(1),
            reverse: false,
        },
    )?;

    Ok(!entries.is_empty())
}

fn marker_key(bucket: BucketId) -> Vec<u8> {
    let mut key = MIGRATION_MARKER_PREFIX.to_vec();
    key.extend_from_slice(&bucket.get().to_be_bytes());

    key
}

fn write_marker<B: StorageBackend>(
    backend: &B,
    bucket: BucketId,
    target: SchemaVersion,
) -> Result<(), OfflineMigrationError> {
    let mut transaction = backend.begin(TxnOptions::default())?;
    transaction.put(&marker_key(bucket), target.get().to_be_bytes().to_vec());
    transaction.commit()?;

    Ok(())
}

fn remove_marker<B: StorageBackend>(
    backend: &B,
    bucket: BucketId,
) -> Result<(), OfflineMigrationError> {
    let mut transaction = backend.begin(TxnOptions::default())?;
    transaction.delete(&marker_key(bucket));
    transaction.commit()?;

    Ok(())
}

fn update_indexes<T: Transaction>(
    transaction: &mut T,
    bucket: BucketId,
    partition: recolith_common::PartitionId,
    key: &[u8],
    old_data: &RecordData,
    new_data: &RecordData,
    indexes: &[IndexDefinition],
) -> Result<(), OfflineMigrationError> {
    for index in indexes {
        let old_values = encoded_index_values(index, old_data)?;
        let new_values = encoded_index_values(index, new_data)?;
        if old_values == new_values {
            continue;
        }
        if let Some(old) = old_values {
            delete_index_entry(transaction, bucket, partition, index.id(), &old, key);
        }
        if let Some(new) = new_values {
            if index.kind() == IndexKind::Unique {
                put_unique_index_entry(transaction, bucket, partition, index.id(), &new, key)?;
            } else {
                put_index_entry(transaction, bucket, partition, index.id(), &new, key);
            }
        }
    }

    Ok(())
}

fn encoded_index_values(
    index: &IndexDefinition,
    data: &RecordData,
) -> Result<Option<Vec<u8>>, OfflineMigrationError> {
    let mut output = Vec::new();
    for field in index.fields() {
        let Some(value) = data.get(*field) else {
            return Ok(None);
        };
        output.extend_from_slice(&encode_index_value(value)?);
    }

    Ok(Some(output))
}
