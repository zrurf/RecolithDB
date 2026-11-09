//! In-process document operations over catalog metadata and a backend.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use recolith_backend::{StorageBackend, TxnOptions};
use recolith_catalog::{Catalog, CatalogError};
use recolith_common::{CollectionId, CommitTs, FragmentId, PartitionId};
use recolith_model::{FragmentCardinality, IndexDefinition, IndexKind};
use recolith_query::{DocumentReadRequest, QueryEngine, QueryError};
use recolith_schema::RecordSchema;
use recolith_storage::{
    DocumentFragments, MAX_DOCUMENT_KEY_LEN, RecordData, StorageError, encode_index_value,
    encode_record_key,
};
use recolith_transaction::{
    FragmentWrite, IndexUpdate, LocalWritePath, PersistentCommitClock, TransactionError,
};

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

/// Atomic multi-fragment document write.
#[derive(Clone, Debug)]
pub struct DocumentWrite {
    collection: CollectionId,
    document_key: Vec<u8>,
    partition: PartitionId,
    fragments: Vec<FragmentWriteRequest>,
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

/// Document service bound to a local catalog and backend.
pub struct DocumentStore<B: StorageBackend> {
    catalog: Arc<Catalog>,
    backend: Arc<B>,
}

impl<B: StorageBackend> DocumentStore<B> {
    /// Creates a document store.
    #[must_use]
    pub fn new(catalog: Arc<Catalog>, backend: Arc<B>) -> Self {
        Self { catalog, backend }
    }

    /// Writes all requested fragments with one commit timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] for unknown metadata, invalid fragment targets,
    /// optimistic conflicts, schema errors, or backend failures.
    pub fn write(&self, request: &DocumentWrite) -> Result<CommitTs, DocumentError> {
        let snapshot = self.catalog.snapshot()?;
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

        let mut path = LocalWritePath::begin(
            self.backend.as_ref(),
            PersistentCommitClock::with_default_key(),
            TxnOptions::default(),
        )?;
        let mut writes = Vec::with_capacity(targets.len());
        for target in targets {
            let old = path
                .read_fields(&target.key, &target.schema)?
                .map(|(_, data)| data);
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
        let commit_ts = path.write_fragments(&writes)?;
        path.commit()?;

        Ok(commit_ts)
    }

    /// Reads selected fragments of one document.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] for binding or execution failures.
    pub fn read(&self, request: &DocumentReadRequest) -> Result<DocumentFragments, DocumentError> {
        let engine = QueryEngine::new(self.catalog.clone(), self.backend.clone());

        Ok(engine.read_document(request)?)
    }
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
