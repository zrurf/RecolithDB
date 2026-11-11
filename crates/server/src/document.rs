//! In-process document operations over catalog metadata and a backend.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use recolith_backend::{
    BackendError, KeyRange, ScanOptions, Snapshot, StorageBackend, Transaction, TxnOptions,
};
use recolith_catalog::{
    Catalog, CatalogAlter, CatalogChanges, CatalogDrop, CatalogError, CatalogSnapshot,
    CatalogWrite, save_catalog,
};
use recolith_common::{
    BucketId, CatalogGeneration, CollectionId, CommitTs, FragmentId, IndexId, PartitionId,
};
use recolith_model::{FragmentCardinality, IndexDefinition, IndexKind};
use recolith_query::{DocumentReadRequest, QueryEngine, QueryError};
use recolith_schema::{Compatibility, RecordSchema, check_compatibility};
use recolith_storage::{
    DocumentFragments, MAX_DOCUMENT_KEY_LEN, RecordData, RecordHeader, StorageError, decode_fields,
    decode_record, decode_record_key, delete_index_entry, encode_bucket_prefix, encode_fields,
    encode_index_prefix, encode_index_value, encode_record, encode_record_key, put_index_entry,
    put_unique_index_entry,
};
use recolith_transaction::{
    BatchedCommitClock, FragmentWrite, IndexUpdate, LocalWritePath, TransactionError,
};

use crate::backup::{self, BackupError, BackupStats};
use crate::migration::{MigrationError, SchemaMigration, migrate_record};

/// One fragment record to write as part of a document.
#[derive(Clone, Debug)]
pub struct FragmentWriteRequest {
    fragment: FragmentId,
    subkey: Vec<u8>,
    data: RecordData,
    if_match: Option<CommitTs>,
}

impl FragmentWriteRequest {
    /// Creates a fragment write. `subkey` must be empty for `one` fragments and
    /// non-empty for `many` fragments.
    #[must_use]
    pub fn new(fragment: FragmentId, subkey: impl Into<Vec<u8>>, data: RecordData) -> Self {
        Self {
            fragment,
            subkey: subkey.into(),
            data,
            if_match: None,
        }
    }

    /// Adds a per-record optimistic version check.
    #[must_use]
    pub fn with_if_match(mut self, expected: CommitTs) -> Self {
        self.if_match = Some(expected);
        self
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub fn fragment(&self) -> FragmentId {
        self.fragment
    }

    /// Returns the fragment subkey.
    #[must_use]
    pub fn subkey(&self) -> &[u8] {
        &self.subkey
    }

    /// Returns the record data.
    #[must_use]
    pub fn data(&self) -> &RecordData {
        &self.data
    }

    /// Returns the optimistic version check.
    #[must_use]
    pub fn if_match(&self) -> Option<CommitTs> {
        self.if_match
    }
}

/// Idempotency record written in the same transaction as a document write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Idempotency {
    key: Vec<u8>,
    request_hash: [u8; 32],
}

impl Idempotency {
    /// Creates an idempotency record keyed by a caller-provided system key.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, request_hash: [u8; 32]) -> Self {
        Self {
            key: key.into(),
            request_hash,
        }
    }

    /// Returns the system key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the canonical request hash.
    #[must_use]
    pub const fn request_hash(&self) -> [u8; 32] {
        self.request_hash
    }
}

/// Atomic multi-fragment document write.
#[derive(Clone, Debug)]
pub struct DocumentWrite {
    collection: CollectionId,
    document_key: Vec<u8>,
    partition: PartitionId,
    fragments: Vec<FragmentWriteRequest>,
    idempotency: Option<Idempotency>,
}

impl DocumentWrite {
    /// Creates a document write.
    #[must_use]
    pub fn new(
        collection: CollectionId,
        document_key: impl Into<Vec<u8>>,
        partition: PartitionId,
    ) -> Self {
        Self {
            collection,
            document_key: document_key.into(),
            partition,
            fragments: Vec::new(),
            idempotency: None,
        }
    }

    /// Adds one fragment write.
    #[must_use]
    pub fn with_fragment(mut self, fragment: FragmentWriteRequest) -> Self {
        self.fragments.push(fragment);
        self
    }

    /// Returns the collection identifier.
    #[must_use]
    pub fn collection_id(&self) -> CollectionId {
        self.collection
    }

    /// Returns the canonical document key.
    #[must_use]
    pub fn document_key(&self) -> &[u8] {
        &self.document_key
    }

    /// Returns the partition index.
    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the fragment writes.
    #[must_use]
    pub fn fragments(&self) -> &[FragmentWriteRequest] {
        &self.fragments
    }

    /// Adds an idempotency record written with the same transaction.
    #[must_use]
    pub fn with_idempotency(mut self, idempotency: Idempotency) -> Self {
        self.idempotency = Some(idempotency);
        self
    }

    /// Returns the idempotency record, if any.
    #[must_use]
    pub fn idempotency(&self) -> Option<&Idempotency> {
        self.idempotency.as_ref()
    }
}

/// Document operation error.
#[derive(Debug)]
pub enum DocumentError {
    /// Catalog access or validation failed.
    Catalog(CatalogError),
    /// Query binding or execution failed.
    Query(QueryError),
    /// Transaction commit failed.
    Transaction(TransactionError),
    /// The collection does not exist.
    UnknownCollection(CollectionId),
    /// The fragment does not belong to the collection.
    UnknownFragment(FragmentId),
    /// Catalog metadata is missing a bucket or schema.
    MissingMetadata(&'static str),
    /// Index value encoding failed.
    Storage(StorageError),
    /// The write request is inconsistent.
    InvalidRequest(String),
    /// The same idempotency key was reused with a different request.
    IdempotencyConflict,
    /// The stored idempotency record is malformed.
    InvalidIdempotencyRecord,
    /// A drop target still contains data and purge was not requested.
    TargetNotEmpty(String),
    /// An index backfill exceeds the configured entry or byte limit.
    BackfillTooLarge {
        /// Records examined.
        entries: usize,
        /// Record bytes examined.
        bytes: usize,
    },
    /// The expected catalog generation no longer matches.
    StaleCatalog,
    /// A schema change requires an explicit migration.
    MigrationRequired,
    /// Record migration failed.
    Migration(MigrationError),
}

impl fmt::Display for DocumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => write!(formatter, "catalog error: {error}"),
            Self::Query(error) => write!(formatter, "query error: {error}"),
            Self::Transaction(error) => write!(formatter, "transaction error: {error}"),
            Self::UnknownCollection(id) => write!(formatter, "unknown collection: {id}"),
            Self::UnknownFragment(id) => write!(formatter, "unknown fragment: {id}"),
            Self::MissingMetadata(kind) => write!(formatter, "catalog is missing {kind}"),
            Self::Storage(error) => write!(formatter, "storage error: {error}"),
            Self::IdempotencyConflict => {
                formatter.write_str("idempotency key was reused with a different request")
            }
            Self::InvalidIdempotencyRecord => {
                formatter.write_str("stored idempotency record is malformed")
            }
            Self::TargetNotEmpty(object) => {
                write!(formatter, "{object} contains data; pass purge to delete it")
            }
            Self::BackfillTooLarge { entries, bytes } => write!(
                formatter,
                "index backfill exceeds limits: {entries} entries, {bytes} bytes"
            ),
            Self::StaleCatalog => formatter.write_str("catalog generation is stale"),
            Self::MigrationRequired => {
                formatter.write_str("schema change requires an explicit migration")
            }
            Self::Migration(error) => write!(formatter, "migration error: {error}"),
            Self::InvalidRequest(message) => {
                write!(formatter, "invalid document request: {message}")
            }
        }
    }
}

impl std::error::Error for DocumentError {}

impl From<CatalogError> for DocumentError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<QueryError> for DocumentError {
    fn from(error: QueryError) -> Self {
        Self::Query(error)
    }
}

impl From<TransactionError> for DocumentError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl From<StorageError> for DocumentError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<BackendError> for DocumentError {
    fn from(error: BackendError) -> Self {
        Self::Catalog(CatalogError::Backend(error))
    }
}

impl From<MigrationError> for DocumentError {
    fn from(error: MigrationError) -> Self {
        Self::Migration(error)
    }
}

/// Document service bound to a local catalog and backend.
pub struct DocumentStore<B: StorageBackend> {
    catalog: RwLock<Arc<Catalog>>,
    backend: Arc<B>,
    clock: BatchedCommitClock,
    data_gate: RwLock<()>,
    ddl_lock: Mutex<()>,
    backup_directory: Option<PathBuf>,
    backup_lock: Mutex<()>,
}

impl<B: StorageBackend> DocumentStore<B> {
    /// Creates a document store.
    #[must_use]
    pub fn new(catalog: Arc<Catalog>, backend: Arc<B>) -> Self {
        Self {
            catalog: RwLock::new(catalog),
            backend,
            clock: BatchedCommitClock::with_default_key(),
            data_gate: RwLock::new(()),
            ddl_lock: Mutex::new(()),
            backup_directory: None,
            backup_lock: Mutex::new(()),
        }
    }

    /// Sets the directory used by online logical backups.
    #[must_use]
    pub fn with_backup_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.backup_directory = Some(directory.into());
        self
    }

    /// Returns the current catalog generation.
    #[must_use]
    pub fn catalog_generation(&self) -> CatalogGeneration {
        self.catalog().generation()
    }

    /// Returns an immutable catalog snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] if an internal catalog lock is poisoned.
    pub fn catalog_snapshot(&self) -> Result<CatalogSnapshot, DocumentError> {
        Ok(self.catalog().snapshot()?)
    }

    /// Writes a consistent logical backup into the configured directory.
    ///
    /// The snapshot is captured under the read gate, then written without
    /// blocking reads or writes.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError`] when backup is not configured, the name is
    /// invalid, the output exists, or the snapshot or file write fails.
    pub fn backup(&self, name: &str) -> Result<(BackupStats, CatalogGeneration), BackupError> {
        let directory = self
            .backup_directory
            .as_ref()
            .ok_or(BackupError::NotConfigured)?;
        backup::validate_name(name)?;
        let _backup = self
            .backup_lock
            .lock()
            .map_err(|_| BackupError::Unavailable)?;
        let (generation, snapshot) = {
            let _read = self.read_guard().map_err(|_| BackupError::Unavailable)?;
            (self.catalog_generation(), self.backend.snapshot()?)
        };
        let stats = backup::backup_snapshot(&snapshot, directory.join(name))?;

        Ok((stats, generation))
    }

    /// Creates a query engine sharing this store's catalog and backend.
    #[must_use]
    pub fn query_engine(&self) -> QueryEngine<B> {
        QueryEngine::new(self.catalog(), self.backend.clone())
    }

    /// Acquires a shared data gate for reads and query execution.
    ///
    /// Catalog changes hold the gate exclusively while purging data or
    /// backfilling indexes.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] if the gate lock is poisoned.
    pub fn read_guard(&self) -> Result<std::sync::RwLockReadGuard<'_, ()>, DocumentError> {
        self.data_gate.read().map_err(|_| lock_error())
    }

    /// Applies additive catalog definitions atomically.
    ///
    /// New indexes are backfilled from existing records in the same backend
    /// transaction that persists the catalog. Writes are excluded for the
    /// duration so the backfill cannot miss concurrent documents.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] for conflicting definitions, missing
    /// references, invalid indexes, oversized backfills, or backend failures.
    pub fn apply_catalog(
        &self,
        changes: &CatalogChanges,
        expected: Option<CatalogGeneration>,
    ) -> Result<CatalogGeneration, DocumentError> {
        let _ddl = self.ddl_lock.lock().map_err(|_| lock_error())?;
        let _gate = self.data_gate.write().map_err(|_| lock_error())?;
        let current = self.catalog();
        let snapshot = current.snapshot()?;
        if expected.is_some_and(|generation| generation != snapshot.generation) {
            return Err(DocumentError::StaleCatalog);
        }

        let Some(candidate) = current.apply_changes(changes)? else {
            return Ok(snapshot.generation);
        };
        let pending = pending_indexes(&snapshot, changes);
        let candidate_snapshot = candidate.snapshot()?;
        let backfill = collect_backfill(self.backend.as_ref(), &candidate_snapshot, pending)?;
        let catalog_write = CatalogWrite::prepare(self.backend.as_ref(), &candidate)?;
        let mut transaction = self.backend.begin(TxnOptions::default())?;
        if let Err(error) = write_backfill(&mut transaction, &backfill) {
            transaction.rollback();
            return Err(error);
        }
        catalog_write.stage(&mut transaction);
        transaction.commit()?;

        let generation = candidate.generation();
        *self.catalog.write().map_err(|_| lock_error())? = Arc::new(candidate);

        Ok(generation)
    }

    /// Removes one catalog object and optionally purges its data.
    ///
    /// Data is deleted in bounded batches before the catalog snapshot is
    /// persisted. A crash during purge leaves the object visible, so the drop
    /// can be retried safely.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] for missing or referenced objects, non-empty
    /// targets without `purge`, or backend failures.
    pub fn drop_catalog(
        &self,
        target: CatalogDrop,
        purge: bool,
        expected: Option<CatalogGeneration>,
    ) -> Result<CatalogGeneration, DocumentError> {
        let _ddl = self.ddl_lock.lock().map_err(|_| lock_error())?;
        let _gate = self.data_gate.write().map_err(|_| lock_error())?;
        let current = self.catalog();
        let snapshot = current.snapshot()?;
        if expected.is_some_and(|generation| generation != snapshot.generation) {
            return Err(DocumentError::StaleCatalog);
        }

        let candidate = current.drop_object(target)?;
        let scope = DropScope::resolve(&snapshot, target)?;
        if purge {
            scope.purge(self.backend.as_ref())?;
        } else {
            scope.ensure_empty(self.backend.as_ref())?;
        }
        save_catalog(self.backend.as_ref(), &candidate)?;

        let generation = candidate.generation();
        *self.catalog.write().map_err(|_| lock_error())? = Arc::new(candidate);

        Ok(generation)
    }

    /// Alters one catalog object and optionally migrates bucket records.
    ///
    /// Compatible schema changes can switch the bucket version without
    /// rewriting records. Incompatible changes require a declarative migration
    /// and rewrite all bucket records in the catalog transaction.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] for missing objects, missing migrations,
    /// invalid mappings, oversized migrations, conflicts, or backend failures.
    pub fn alter_catalog(
        &self,
        alter: CatalogAlter,
        migration: Option<&SchemaMigration>,
        expected: Option<CatalogGeneration>,
    ) -> Result<CatalogGeneration, DocumentError> {
        let _ddl = self.ddl_lock.lock().map_err(|_| lock_error())?;
        let _gate = self.data_gate.write().map_err(|_| lock_error())?;
        let current = self.catalog();
        let snapshot = current.snapshot()?;
        if expected.is_some_and(|generation| generation != snapshot.generation) {
            return Err(DocumentError::StaleCatalog);
        }
        if let CatalogAlter::BucketSchema { bucket, version } = &alter {
            let current_bucket = snapshot
                .buckets
                .get(bucket)
                .ok_or(DocumentError::MissingMetadata("bucket"))?;
            if current_bucket.schema_version() == *version {
                return Ok(snapshot.generation);
            }
            if *version < current_bucket.schema_version() {
                return Err(DocumentError::InvalidRequest(
                    "schema version must increase".to_owned(),
                ));
            }
        }

        let candidate = current.alter_object(alter.clone())?;
        let catalog_write = CatalogWrite::prepare(self.backend.as_ref(), &candidate)?;
        let mut transaction = self.backend.begin(TxnOptions::default())?;
        let result = (|| -> Result<(), DocumentError> {
            match &alter {
                CatalogAlter::BucketSchema { bucket, version } => {
                    let bucket_meta = snapshot
                        .buckets
                        .get(bucket)
                        .ok_or(DocumentError::MissingMetadata("bucket"))?;
                    let old_schema = snapshot
                        .schemas
                        .schemas
                        .get(&(bucket_meta.schema_id(), bucket_meta.schema_version()))
                        .ok_or(DocumentError::MissingMetadata("schema"))?;
                    let new_schema = snapshot
                        .schemas
                        .schemas
                        .get(&(bucket_meta.schema_id(), *version))
                        .ok_or(DocumentError::MissingMetadata("schema"))?;
                    let compatibility =
                        check_compatibility(old_schema, new_schema).map_err(CatalogError::from)?;
                    if compatibility == Compatibility::RequiresMigration && migration.is_none() {
                        return Err(DocumentError::MigrationRequired);
                    }
                    if let Some(migration) = migration {
                        let records =
                            collect_bucket_records(self.backend.as_ref(), &snapshot, *bucket)?;
                        let indexes: Vec<IndexDefinition> = snapshot
                            .indexes
                            .get(bucket)
                            .map(|indexes| indexes.values().cloned().collect())
                            .unwrap_or_default();
                        rewrite_records(
                            &mut transaction,
                            *bucket,
                            &records,
                            new_schema,
                            migration,
                            &indexes,
                        )?;
                    }
                }
                CatalogAlter::Index {
                    bucket,
                    index,
                    definition,
                } => {
                    let old_index = snapshot
                        .indexes
                        .get(bucket)
                        .and_then(|indexes| indexes.get(index))
                        .cloned()
                        .ok_or(DocumentError::MissingMetadata("index"))?;
                    let bucket_meta = snapshot
                        .buckets
                        .get(bucket)
                        .ok_or(DocumentError::MissingMetadata("bucket"))?;
                    let records =
                        collect_bucket_records(self.backend.as_ref(), &snapshot, *bucket)?;
                    let old_keys = collect_index_keys(
                        self.backend.as_ref(),
                        *bucket,
                        old_index.id(),
                        bucket_meta.partition_count(),
                    )?;
                    for key in &old_keys {
                        transaction.delete(key);
                    }
                    write_index_entries(&mut transaction, *bucket, &records, definition)?;
                }
            }

            Ok(())
        })();
        if let Err(error) = result {
            transaction.rollback();
            return Err(error);
        }
        catalog_write.stage(&mut transaction);
        transaction.commit()?;

        let generation = candidate.generation();
        *self.catalog.write().map_err(|_| lock_error())? = Arc::new(candidate);

        Ok(generation)
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Writes all requested fragments with one commit timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] for unknown metadata, invalid fragment targets,
    /// optimistic conflicts, schema errors, or backend failures.
    pub fn write(&self, request: &DocumentWrite) -> Result<CommitTs, DocumentError> {
        let _gate = self.data_gate.read().map_err(|_| lock_error())?;
        let snapshot = self.catalog().snapshot()?;
        let collection = snapshot
            .collections
            .get(&request.collection)
            .ok_or(DocumentError::UnknownCollection(request.collection))?;
        if request.fragments.is_empty() {
            return Err(DocumentError::InvalidRequest(
                "write must include at least one fragment".to_owned(),
            ));
        }
        if request.document_key.is_empty() {
            return Err(DocumentError::InvalidRequest(
                "document key must not be empty".to_owned(),
            ));
        }
        if request.document_key.len() > MAX_DOCUMENT_KEY_LEN {
            return Err(DocumentError::InvalidRequest(format!(
                "document key is {} bytes, limit is {MAX_DOCUMENT_KEY_LEN}",
                request.document_key.len()
            )));
        }

        let mut seen_targets = BTreeSet::new();
        let mut targets = Vec::with_capacity(request.fragments.len());
        for fragment_request in &request.fragments {
            let fragment = collection
                .fragments()
                .iter()
                .find(|fragment| fragment.id() == fragment_request.fragment)
                .ok_or(DocumentError::UnknownFragment(fragment_request.fragment))?;
            let bucket = snapshot
                .buckets
                .get(&fragment.bucket_id())
                .ok_or(DocumentError::MissingMetadata("bucket"))?;
            if request.partition.get() >= bucket.partition_count() {
                return Err(DocumentError::InvalidRequest(format!(
                    "partition {} is outside bucket {} with {} partitions",
                    request.partition,
                    bucket.id(),
                    bucket.partition_count()
                )));
            }
            match fragment.cardinality() {
                FragmentCardinality::One if !fragment_request.subkey.is_empty() => {
                    return Err(DocumentError::InvalidRequest(format!(
                        "one fragment {} requires an empty subkey",
                        fragment.id()
                    )));
                }
                FragmentCardinality::Many if fragment_request.subkey.is_empty() => {
                    return Err(DocumentError::InvalidRequest(format!(
                        "many fragment {} requires a subkey",
                        fragment.id()
                    )));
                }
                _ => {}
            }

            let schema = snapshot
                .schemas
                .schemas
                .get(&(bucket.schema_id(), bucket.schema_version()))
                .ok_or(DocumentError::MissingMetadata("schema"))?;
            let key = encode_record_key(
                bucket.id(),
                request.partition,
                &request.document_key,
                fragment.id(),
                &fragment_request.subkey,
            );
            if !seen_targets.insert(key.clone()) {
                return Err(DocumentError::InvalidRequest(format!(
                    "duplicate target for fragment {}",
                    fragment.id()
                )));
            }
            let indexes = snapshot
                .indexes
                .get(&bucket.id())
                .map(|indexes| indexes.values().cloned().collect())
                .unwrap_or_default();
            targets.push(Target {
                key,
                schema: schema.clone(),
                data: fragment_request.data.clone(),
                if_match: fragment_request.if_match,
                indexes,
            });
        }

        let mut path =
            LocalWritePath::begin_unclocked(self.backend.as_ref(), TxnOptions::default())?;
        if let Some(idempotency) = request.idempotency()
            && let Some(value) = path.get_raw(idempotency.key())?
        {
            let (hash, commit_ts) = decode_idempotency(&value)?;
            if hash != idempotency.request_hash() {
                return Err(DocumentError::IdempotencyConflict);
            }

            return Ok(commit_ts);
        }

        let mut writes = Vec::with_capacity(targets.len());
        for target in targets {
            let old = match path.get_raw(&target.key)? {
                Some(frame) => {
                    let decoded = decode_record(&frame)?;
                    let schema = record_schema(&snapshot, &target.schema, decoded.header())?;
                    Some(decode_fields(&schema, decoded.payload())?)
                }
                None => None,
            };
            let mut write = FragmentWrite::new(target.key, target.schema, target.data.clone());
            for index in &target.indexes {
                let old_values = match &old {
                    Some(data) => encoded_index_values(index, data)?,
                    None => None,
                };
                let new_values = encoded_index_values(index, &target.data)?;
                if old_values == new_values {
                    continue;
                }
                write = write.with_index(IndexUpdate::new(
                    index.id(),
                    old_values,
                    new_values,
                    index.kind() == IndexKind::Unique,
                ));
            }
            if let Some(expected) = target.if_match {
                write = write.with_if_match(expected);
            }
            writes.push(write);
        }
        let commit_ts = self.clock.reserve(self.backend.as_ref())?;
        path.write_fragments_at(&writes, commit_ts)?;
        if let Some(idempotency) = request.idempotency() {
            path.put_raw(
                idempotency.key(),
                encode_idempotency(idempotency.request_hash(), commit_ts),
            );
        }
        path.commit()?;

        Ok(commit_ts)
    }

    /// Reads selected fragments of one document.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] for binding or execution failures.
    pub fn read(&self, request: &DocumentReadRequest) -> Result<DocumentFragments, DocumentError> {
        let _gate = self.read_guard()?;
        let engine = QueryEngine::new(self.catalog(), self.backend.clone());

        Ok(engine.read_document(request)?)
    }
}

const IDEMPOTENCY_RECORD_VERSION: u8 = 1;
const IDEMPOTENCY_RECORD_LEN: usize = 1 + 32 + 8;
const BACKFILL_SCAN_ENTRIES: usize = 1_024;
const MAX_BACKFILL_ENTRIES: usize = 100_000;
const MAX_BACKFILL_BYTES: usize = 64 * 1024 * 1024;
const PURGE_BATCH_ENTRIES: usize = 1_024;

fn lock_error() -> DocumentError {
    DocumentError::Catalog(CatalogError::LockPoisoned)
}

fn record_schema(
    snapshot: &CatalogSnapshot,
    current: &RecordSchema,
    header: &RecordHeader,
) -> Result<RecordSchema, DocumentError> {
    if header.schema_id() != current.id() {
        return Err(schema_mismatch(header, current));
    }
    if header.schema_version() == current.version() {
        return Ok(current.clone());
    }
    snapshot
        .schemas
        .schemas
        .get(&(current.id(), header.schema_version()))
        .cloned()
        .ok_or_else(|| schema_mismatch(header, current))
}

fn schema_mismatch(header: &RecordHeader, expected: &RecordSchema) -> DocumentError {
    DocumentError::Storage(StorageError::SchemaMismatch {
        record_schema: header.schema_id(),
        record_version: header.schema_version(),
        expected_schema: expected.id(),
        expected_version: expected.version(),
    })
}

struct BucketRecord {
    key: Vec<u8>,
    partition: PartitionId,
    header: RecordHeader,
    data: RecordData,
}

struct BackfillBucket {
    bucket: BucketId,
    indexes: Vec<IndexDefinition>,
    records: Vec<BucketRecord>,
}

fn pending_indexes(
    snapshot: &CatalogSnapshot,
    changes: &CatalogChanges,
) -> BTreeMap<BucketId, Vec<IndexDefinition>> {
    let mut pending: BTreeMap<BucketId, Vec<IndexDefinition>> = BTreeMap::new();
    for (bucket_id, index) in &changes.indexes {
        let exists = snapshot
            .indexes
            .get(bucket_id)
            .and_then(|indexes| indexes.get(&index.id()))
            .is_some();
        if !exists {
            pending.entry(*bucket_id).or_default().push(index.clone());
        }
    }

    pending
}

fn collect_bucket_records<B: StorageBackend>(
    backend: &B,
    snapshot: &CatalogSnapshot,
    bucket_id: BucketId,
) -> Result<Vec<BucketRecord>, DocumentError> {
    let bucket = snapshot
        .buckets
        .get(&bucket_id)
        .ok_or(DocumentError::MissingMetadata("bucket"))?;
    let current_schema = snapshot
        .schemas
        .schemas
        .get(&(bucket.schema_id(), bucket.schema_version()))
        .ok_or(DocumentError::MissingMetadata("schema"))?
        .clone();
    let storage = backend.snapshot()?;
    let prefix = encode_bucket_prefix(bucket_id);
    let end = KeyRange::prefix(&prefix).end_bound().clone();
    let mut range = KeyRange::prefix(&prefix);
    let mut records = Vec::new();
    let mut checked_entries = 0_usize;
    let mut checked_bytes = 0_usize;
    loop {
        let entries = storage.scan(
            &range,
            ScanOptions {
                limit: Some(BACKFILL_SCAN_ENTRIES),
                reverse: false,
            },
        )?;
        if entries.is_empty() {
            break;
        }
        let last = entries.last().expect("non-empty scan").0.clone();
        for (key, value) in entries {
            let Ok(decoded) = decode_record_key(&key) else {
                continue;
            };
            checked_entries += 1;
            checked_bytes = checked_bytes.saturating_add(key.len() + value.len());
            if checked_entries > MAX_BACKFILL_ENTRIES || checked_bytes > MAX_BACKFILL_BYTES {
                return Err(DocumentError::BackfillTooLarge {
                    entries: checked_entries,
                    bytes: checked_bytes,
                });
            }
            let record = decode_record(&value)?;
            let schema = record_schema(snapshot, &current_schema, record.header())?;
            let data = decode_fields(&schema, record.payload())?;
            records.push(BucketRecord {
                key,
                partition: decoded.head().partition(),
                header: record.header().clone(),
                data,
            });
        }
        range = KeyRange::new(Bound::Excluded(last), end.clone());
    }

    Ok(records)
}

fn collect_backfill<B: StorageBackend>(
    backend: &B,
    snapshot: &CatalogSnapshot,
    pending: BTreeMap<BucketId, Vec<IndexDefinition>>,
) -> Result<Vec<BackfillBucket>, DocumentError> {
    let mut output = Vec::with_capacity(pending.len());
    for (bucket_id, indexes) in pending {
        let records = collect_bucket_records(backend, snapshot, bucket_id)?;
        output.push(BackfillBucket {
            bucket: bucket_id,
            indexes,
            records,
        });
    }

    Ok(output)
}

fn write_backfill<T: Transaction>(
    transaction: &mut T,
    backfill: &[BackfillBucket],
) -> Result<(), DocumentError> {
    for bucket in backfill {
        for index in &bucket.indexes {
            write_index_entries(transaction, bucket.bucket, &bucket.records, index)?;
        }
    }

    Ok(())
}

fn write_index_entries<T: Transaction>(
    transaction: &mut T,
    bucket: BucketId,
    records: &[BucketRecord],
    index: &IndexDefinition,
) -> Result<(), DocumentError> {
    for record in records {
        let Some(values) = encoded_index_values(index, &record.data)? else {
            continue;
        };
        if index.kind() == IndexKind::Unique {
            put_unique_index_entry(
                transaction,
                bucket,
                record.partition,
                index.id(),
                &values,
                &record.key,
            )?;
        } else {
            put_index_entry(
                transaction,
                bucket,
                record.partition,
                index.id(),
                &values,
                &record.key,
            );
        }
    }

    Ok(())
}

fn collect_index_keys<B: StorageBackend>(
    backend: &B,
    bucket: BucketId,
    index: IndexId,
    partitions: u32,
) -> Result<Vec<Vec<u8>>, DocumentError> {
    let storage = backend.snapshot()?;
    let mut keys = Vec::new();
    let mut checked_bytes = 0_usize;
    for partition in 0..partitions {
        let prefix = encode_index_prefix(bucket, PartitionId::new(partition), index);
        let end = KeyRange::prefix(&prefix).end_bound().clone();
        let mut range = KeyRange::prefix(&prefix);
        loop {
            let entries = storage.scan(
                &range,
                ScanOptions {
                    limit: Some(BACKFILL_SCAN_ENTRIES),
                    reverse: false,
                },
            )?;
            if entries.is_empty() {
                break;
            }
            let last = entries.last().expect("non-empty scan").0.clone();
            for (key, _) in entries {
                checked_bytes = checked_bytes.saturating_add(key.len());
                if keys.len() >= MAX_BACKFILL_ENTRIES || checked_bytes > MAX_BACKFILL_BYTES {
                    return Err(DocumentError::BackfillTooLarge {
                        entries: keys.len() + 1,
                        bytes: checked_bytes,
                    });
                }
                keys.push(key);
            }
            range = KeyRange::new(Bound::Excluded(last), end.clone());
        }
    }

    Ok(keys)
}

fn rewrite_records<T: Transaction>(
    transaction: &mut T,
    bucket: BucketId,
    records: &[BucketRecord],
    new_schema: &RecordSchema,
    migration: &SchemaMigration,
    indexes: &[IndexDefinition],
) -> Result<(), DocumentError> {
    for record in records {
        let new_data = migrate_record(&record.data, new_schema, migration)?;
        let header = RecordHeader::new(
            new_schema.id(),
            new_schema.version(),
            record.header.commit_ts(),
        )
        .with_flags(record.header.flags())
        .with_expire_at(record.header.expire_at());
        let payload = encode_fields(new_schema, &new_data)?;
        let frame = encode_record(&header, &payload)?;
        transaction.put(&record.key, frame);

        for index in indexes {
            let old_values = encoded_index_values(index, &record.data)?;
            let new_values = encoded_index_values(index, &new_data)?;
            if old_values == new_values {
                continue;
            }
            if let Some(old) = old_values {
                delete_index_entry(
                    transaction,
                    bucket,
                    record.partition,
                    index.id(),
                    &old,
                    &record.key,
                );
            }
            if let Some(new) = new_values {
                if index.kind() == IndexKind::Unique {
                    put_unique_index_entry(
                        transaction,
                        bucket,
                        record.partition,
                        index.id(),
                        &new,
                        &record.key,
                    )?;
                } else {
                    put_index_entry(
                        transaction,
                        bucket,
                        record.partition,
                        index.id(),
                        &new,
                        &record.key,
                    );
                }
            }
        }
    }

    Ok(())
}

enum DropScope {
    Index {
        bucket: BucketId,
        index: IndexId,
        partitions: u32,
    },
    Buckets(Vec<BucketId>),
}

impl DropScope {
    fn resolve(snapshot: &CatalogSnapshot, target: CatalogDrop) -> Result<Self, DocumentError> {
        match target {
            CatalogDrop::Index { bucket, index } => {
                let bucket_meta = snapshot
                    .buckets
                    .get(&bucket)
                    .ok_or(DocumentError::MissingMetadata("bucket"))?;
                Ok(Self::Index {
                    bucket,
                    index,
                    partitions: bucket_meta.partition_count(),
                })
            }
            CatalogDrop::Collection(id) => {
                let collection = snapshot
                    .collections
                    .get(&id)
                    .ok_or(DocumentError::UnknownCollection(id))?;
                let buckets: BTreeSet<BucketId> = collection
                    .fragments()
                    .iter()
                    .map(|fragment| fragment.bucket_id())
                    .collect();
                Ok(Self::Buckets(buckets.into_iter().collect()))
            }
            CatalogDrop::Bucket(id) => Ok(Self::Buckets(vec![id])),
            CatalogDrop::Keyspace(id) => {
                let buckets = snapshot
                    .bucket_keyspaces
                    .iter()
                    .filter(|(_, keyspace_id)| **keyspace_id == id)
                    .map(|(bucket_id, _)| *bucket_id)
                    .collect();
                Ok(Self::Buckets(buckets))
            }
        }
    }

    fn ensure_empty<B: StorageBackend>(&self, backend: &B) -> Result<(), DocumentError> {
        match self {
            Self::Index {
                bucket,
                index,
                partitions,
            } => {
                for partition in 0..*partitions {
                    let prefix = encode_index_prefix(*bucket, PartitionId::new(partition), *index);
                    if prefix_has_entries(backend, &prefix)? {
                        return Err(DocumentError::TargetNotEmpty(format!(
                            "index {index} in bucket {bucket}"
                        )));
                    }
                }
            }
            Self::Buckets(buckets) => {
                for bucket in buckets {
                    if prefix_has_entries(backend, &encode_bucket_prefix(*bucket))? {
                        return Err(DocumentError::TargetNotEmpty(format!("bucket {bucket}")));
                    }
                }
            }
        }

        Ok(())
    }

    fn purge<B: StorageBackend>(&self, backend: &B) -> Result<(), DocumentError> {
        match self {
            Self::Index {
                bucket,
                index,
                partitions,
            } => {
                for partition in 0..*partitions {
                    let prefix = encode_index_prefix(*bucket, PartitionId::new(partition), *index);
                    purge_prefix(backend, &prefix)?;
                }
            }
            Self::Buckets(buckets) => {
                for bucket in buckets {
                    purge_prefix(backend, &encode_bucket_prefix(*bucket))?;
                }
            }
        }

        Ok(())
    }
}

fn prefix_has_entries<B: StorageBackend>(
    backend: &B,
    prefix: &[u8],
) -> Result<bool, DocumentError> {
    let entries = backend.snapshot()?.scan(
        &KeyRange::prefix(prefix),
        ScanOptions {
            limit: Some(1),
            reverse: false,
        },
    )?;

    Ok(!entries.is_empty())
}

fn purge_prefix<B: StorageBackend>(backend: &B, prefix: &[u8]) -> Result<(), DocumentError> {
    loop {
        let entries = backend.snapshot()?.scan(
            &KeyRange::prefix(prefix),
            ScanOptions {
                limit: Some(PURGE_BATCH_ENTRIES),
                reverse: false,
            },
        )?;
        if entries.is_empty() {
            return Ok(());
        }
        let mut transaction = backend.begin(TxnOptions::default())?;
        for (key, _) in &entries {
            transaction.delete(key);
        }
        transaction.commit()?;
    }
}

fn encode_idempotency(hash: [u8; 32], commit_ts: CommitTs) -> Vec<u8> {
    let mut value = Vec::with_capacity(IDEMPOTENCY_RECORD_LEN);
    value.push(IDEMPOTENCY_RECORD_VERSION);
    value.extend_from_slice(&hash);
    value.extend_from_slice(&commit_ts.get().to_le_bytes());
    value
}

fn decode_idempotency(value: &[u8]) -> Result<([u8; 32], CommitTs), DocumentError> {
    if value.len() != IDEMPOTENCY_RECORD_LEN || value[0] != IDEMPOTENCY_RECORD_VERSION {
        return Err(DocumentError::InvalidIdempotencyRecord);
    }
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(&value[1..33]);
    let commit_ts = u64::from_le_bytes(value[33..41].try_into().expect("eight bytes"));

    Ok((hash, CommitTs::new(commit_ts)))
}

struct Target {
    key: Vec<u8>,
    schema: RecordSchema,
    data: RecordData,
    if_match: Option<CommitTs>,
    indexes: Vec<IndexDefinition>,
}

fn encoded_index_values(
    index: &IndexDefinition,
    data: &RecordData,
) -> Result<Option<Vec<u8>>, DocumentError> {
    let mut output = Vec::new();
    for field in index.fields() {
        let Some(value) = data.get(*field) else {
            return Ok(None);
        };
        output.extend_from_slice(&encode_index_value(value)?);
    }

    Ok(Some(output))
}

impl<B: StorageBackend> fmt::Debug for DocumentStore<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DocumentStore")
    }
}
