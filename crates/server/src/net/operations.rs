use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use recolith_backend::StorageBackend;
use recolith_catalog::{CatalogAlter, CatalogChanges, CatalogDrop, CatalogError, CatalogSnapshot};
use recolith_common::{
    BucketId, CatalogGeneration, CollectionId, CommitTs, FieldId, FragmentId, IndexId, KeyspaceId,
    PartitionId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_protocol::messages::{
    self, AggregateColumn, AggregateRequest, AggregateResponse, AggregateRow, BucketDefinition,
    CatalogAlterRequest, CatalogAlterResponse, CatalogApplyRequest, CatalogApplyResponse,
    CatalogDropRequest, CatalogDropResponse, DescribeCollectionRequest, DescribeCollectionResponse,
    DocumentGetRequest, DocumentGetResponse, DocumentPutRequest, DocumentPutResponse,
    DocumentResult, ExplainDocumentPlan, ExplainDrive, ExplainFieldSelection, ExplainFilter,
    ExplainFilterOp, ExplainIndex, ExplainKeyFilter, ExplainOrder, ExplainRequest, ExplainResponse,
    ExplainScanPlan, FragmentVersion, QueryRequest, QueryResponse, RequestMeta,
    describe_collection_request, explain_response, opcode, status, value,
};
use recolith_query::error::QueryError;
use recolith_query::{
    AggregateFunction as QueryAggregateFunction, BindParameters, BoundQuery, DocumentQuery,
    FieldSelection, Filter, FilterOp, RqlBindError, RqlError, RqlLiteral, ScanDriveInfo,
    ScanPosition, bind_aggregate_query, bind_query, parse_aggregate_query, parse_document_query,
};
use recolith_schema::parse_record;
use recolith_security::{Action, AuthTarget, Principal};
use recolith_storage::StorageError;
use recolith_transaction::TransactionError;
use sha2::{Digest, Sha256};

use crate::document::{
    DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest, Idempotency,
};
use crate::migration::{FieldMapping, MigrationConversion, MigrationSource, SchemaMigration};
use crate::query_cursor;

use super::convert;

const IDEMPOTENCY_PREFIX: &[u8] = b"\0recolith/idempotency/";
const MAX_IDEMPOTENCY_KEY_LEN: usize = 256;
const DEFAULT_QUERY_LIMIT: u64 = 1000;
const MAX_QUERY_LIMIT: u64 = 10_000;

/// Operation failure translated to a frame status and message.
#[derive(Debug)]
pub(crate) struct OperationError {
    status: u16,
    message: String,
}

impl OperationError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub(crate) fn status(&self) -> u16 {
        self.status
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(status::INTERNAL, message)
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(status::INVALID_REQUEST, message)
    }
}

impl fmt::Display for OperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for OperationError {}

/// Encoded response payload and its opcode.
pub(crate) struct OperationResponse {
    pub(crate) opcode: u16,
    pub(crate) payload: Vec<u8>,
}

pub(crate) fn document_get<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: DocumentGetRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let _read = store.read_guard().map_err(map_document)?;
    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    authorize_collection(
        &snapshot,
        principal,
        CollectionId::new(request.collection_id),
        Action::Read,
    )?;

    let mut query = DocumentQuery::new(
        CollectionId::new(request.collection_id),
        request.document_key.clone(),
        PartitionId::new(request.partition),
    );
    for fragment in request.fragments {
        query = query.with_fragment(FragmentId::new(fragment));
    }
    for selection in request.projection {
        query = query.with_projection(FieldSelection::new(
            FragmentId::new(selection.fragment_id),
            FieldId::new(selection.field_id),
        ));
    }

    let engine = store.query_engine();
    let plan = engine.plan(&query).map_err(map_query)?;
    let fragments = engine.execute(&plan).map_err(map_query)?;
    let response = DocumentGetResponse {
        document_key: request.document_key,
        fragments: convert::fragments_to_wire(&fragments),
    };

    Ok(OperationResponse {
        opcode: opcode::DOC_GET,
        payload: response.encode_to_vec(),
    })
}

pub(crate) fn describe_collection<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: DescribeCollectionRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let snapshot = store
        .catalog_snapshot()
        .map_err(|error| OperationError::internal(error.to_string()))?;
    let collection = resolve_collection(&snapshot, &request)?;
    authorize_collection(&snapshot, principal, collection.id(), Action::Describe)?;
    let metadata = super::metadata::collection_metadata(&snapshot, collection)?;
    let response = DescribeCollectionResponse {
        catalog_generation: snapshot.generation.get(),
        collection: Some(metadata),
    };

    Ok(OperationResponse {
        opcode: opcode::DESCRIBE_COLLECTION,
        payload: response.encode_to_vec(),
    })
}

pub(crate) fn catalog_apply<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: CatalogApplyRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let expected = request
        .meta
        .as_ref()
        .filter(|meta| meta.catalog_generation != 0)
        .map(|meta| CatalogGeneration::new(meta.catalog_generation));
    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    let changes = catalog_changes(request)?;
    authorize_catalog_changes(&snapshot, principal, &changes)?;
    let generation = store
        .apply_catalog(&changes, expected)
        .map_err(map_document)?;
    let response = CatalogApplyResponse {
        catalog_generation: generation.get(),
    };

    Ok(OperationResponse {
        opcode: opcode::CATALOG_APPLY,
        payload: response.encode_to_vec(),
    })
}

pub(crate) fn catalog_drop<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: CatalogDropRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let expected = request
        .meta
        .as_ref()
        .filter(|meta| meta.catalog_generation != 0)
        .map(|meta| CatalogGeneration::new(meta.catalog_generation));
    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    let target = drop_target(&request)?;
    authorize_catalog_drop(&snapshot, principal, target)?;
    let generation = store
        .drop_catalog(target, request.purge, expected)
        .map_err(map_document)?;
    let response = CatalogDropResponse {
        catalog_generation: generation.get(),
    };

    Ok(OperationResponse {
        opcode: opcode::CATALOG_DROP,
        payload: response.encode_to_vec(),
    })
}

pub(crate) fn catalog_alter<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: CatalogAlterRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let expected = request
        .meta
        .as_ref()
        .filter(|meta| meta.catalog_generation != 0)
        .map(|meta| CatalogGeneration::new(meta.catalog_generation));
    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    let (alter, migration) = alter_change(&request)?;
    authorize_catalog_alter(&snapshot, principal, &alter)?;
    let generation = store
        .alter_catalog(alter, migration.as_ref(), expected)
        .map_err(map_document)?;
    let response = CatalogAlterResponse {
        catalog_generation: generation.get(),
    };

    Ok(OperationResponse {
        opcode: opcode::CATALOG_ALTER,
        payload: response.encode_to_vec(),
    })
}

fn alter_change(
    request: &CatalogAlterRequest,
) -> Result<(CatalogAlter, Option<SchemaMigration>), OperationError> {
    match request.change.as_ref() {
        Some(messages::catalog_alter_request::Change::BucketSchema(change)) => {
            let alter = CatalogAlter::BucketSchema {
                bucket: BucketId::new(change.bucket_id),
                version: SchemaVersion::new(change.schema_version),
            };
            let migration = change
                .migration
                .as_ref()
                .map(schema_migration)
                .transpose()?;

            Ok((alter, migration))
        }
        Some(messages::catalog_alter_request::Change::Index(change)) => Ok((
            CatalogAlter::Index {
                bucket: BucketId::new(change.bucket_id),
                index: IndexId::new(change.index_id),
                definition: IndexDefinition::new(
                    IndexId::new(change.index_id),
                    change.name.clone(),
                    index_kind(change.kind)?,
                    change
                        .fields
                        .iter()
                        .map(|field| FieldId::new(*field))
                        .collect(),
                ),
            },
            None,
        )),
        None => Err(OperationError::invalid("catalog alter change is required")),
    }
}

fn schema_migration(
    migration: &messages::SchemaMigration,
) -> Result<SchemaMigration, OperationError> {
    let mut mappings = Vec::with_capacity(migration.mappings.len());
    for mapping in &migration.mappings {
        let source = match mapping.source.as_ref() {
            Some(messages::field_mapping::Source::FromField(field)) => {
                MigrationSource::Field(FieldId::new(*field))
            }
            Some(messages::field_mapping::Source::DefaultValue(value)) => {
                MigrationSource::Default(convert::value_from_wire(value).map_err(|error| {
                    OperationError::invalid(format!("invalid migration default: {error}"))
                })?)
            }
            None => return Err(OperationError::invalid("field mapping source is required")),
        };
        let conversion = match messages::FieldConversion::try_from(mapping.conversion) {
            Ok(messages::FieldConversion::Unspecified | messages::FieldConversion::None) => {
                MigrationConversion::None
            }
            Ok(messages::FieldConversion::I32ToI64) => MigrationConversion::I32ToI64,
            Ok(messages::FieldConversion::U32ToU64) => MigrationConversion::U32ToU64,
            Ok(messages::FieldConversion::U32ToI64) => MigrationConversion::U32ToI64,
            Err(_) => return Err(OperationError::invalid("unknown field conversion")),
        };
        mappings.push(FieldMapping {
            to: FieldId::new(mapping.to_field),
            source,
            conversion,
        });
    }

    Ok(SchemaMigration { mappings })
}

fn authorize_catalog_alter(
    snapshot: &CatalogSnapshot,
    principal: &Principal,
    alter: &CatalogAlter,
) -> Result<(), OperationError> {
    match alter {
        CatalogAlter::BucketSchema { bucket, .. } | CatalogAlter::Index { bucket, .. } => {
            require_admin(principal, keyspace_for_bucket(snapshot, *bucket)?, "*")
        }
    }
}

fn drop_target(request: &CatalogDropRequest) -> Result<CatalogDrop, OperationError> {
    match request.target.as_ref() {
        Some(messages::catalog_drop_request::Target::Index(index)) => Ok(CatalogDrop::Index {
            bucket: BucketId::new(index.bucket_id),
            index: IndexId::new(index.index_id),
        }),
        Some(messages::catalog_drop_request::Target::CollectionId(id)) => {
            Ok(CatalogDrop::Collection(CollectionId::new(*id)))
        }
        Some(messages::catalog_drop_request::Target::BucketId(id)) => {
            Ok(CatalogDrop::Bucket(BucketId::new(*id)))
        }
        Some(messages::catalog_drop_request::Target::KeyspaceId(id)) => {
            Ok(CatalogDrop::Keyspace(KeyspaceId::new(*id)))
        }
        None => Err(OperationError::invalid("catalog drop target is required")),
    }
}

fn authorize_catalog_drop(
    snapshot: &CatalogSnapshot,
    principal: &Principal,
    target: CatalogDrop,
) -> Result<(), OperationError> {
    match target {
        CatalogDrop::Index { bucket, .. } => {
            require_admin(principal, keyspace_for_bucket(snapshot, bucket)?, "*")
        }
        CatalogDrop::Collection(id) => {
            let collection = snapshot.collections.get(&id).ok_or_else(|| {
                OperationError::new(status::NOT_FOUND, "collection was not found")
            })?;
            let keyspace = snapshot
                .keyspaces
                .get(&collection.keyspace_id())
                .ok_or_else(|| {
                    OperationError::internal("collection references missing keyspace")
                })?;
            require_admin(principal, keyspace.name(), collection.name())
        }
        CatalogDrop::Bucket(id) => {
            require_admin(principal, keyspace_for_bucket(snapshot, id)?, "*")
        }
        CatalogDrop::Keyspace(id) => {
            let keyspace = snapshot
                .keyspaces
                .get(&id)
                .ok_or_else(|| OperationError::new(status::NOT_FOUND, "keyspace was not found"))?;
            require_admin(principal, keyspace.name(), "*")
        }
    }
}

fn keyspace_for_bucket(
    snapshot: &CatalogSnapshot,
    bucket: BucketId,
) -> Result<&str, OperationError> {
    let keyspace_id = snapshot
        .bucket_keyspaces
        .get(&bucket)
        .copied()
        .ok_or_else(|| OperationError::new(status::NOT_FOUND, "bucket was not found"))?;
    snapshot
        .keyspaces
        .get(&keyspace_id)
        .map(recolith_model::Keyspace::name)
        .ok_or_else(|| OperationError::internal("bucket references missing keyspace"))
}

fn catalog_changes(request: CatalogApplyRequest) -> Result<CatalogChanges, OperationError> {
    let mut changes = CatalogChanges::default();
    for keyspace in request.keyspaces {
        changes
            .keyspaces
            .push(Keyspace::new(KeyspaceId::new(keyspace.id), keyspace.name));
    }
    for schema in request.schemas {
        let record = parse_record(
            &schema.idl,
            SchemaId::new(schema.id),
            SchemaVersion::new(schema.version),
        )
        .map_err(|error| {
            OperationError::invalid(format!(
                "invalid schema {} version {}: {error}",
                schema.id, schema.version
            ))
        })?;
        changes.schemas.push(record);
    }
    for bucket in request.buckets {
        changes.buckets.push(bucket_change(bucket)?);
    }
    for collection in request.collections {
        let key_fields = collection
            .key_fields
            .into_iter()
            .map(|field| Ok(KeyField::new(field.name, key_type(field.r#type)?)))
            .collect::<Result<Vec<_>, OperationError>>()?;
        let fragments = collection
            .fragments
            .into_iter()
            .map(|fragment| {
                Ok(Fragment::new(
                    FragmentId::new(fragment.id),
                    fragment.name,
                    BucketId::new(fragment.bucket_id),
                    cardinality(fragment.cardinality)?,
                ))
            })
            .collect::<Result<Vec<_>, OperationError>>()?;
        let affinity_group = if collection.affinity_group.is_empty() {
            collection.name.clone()
        } else {
            collection.affinity_group
        };
        changes.collections.push(Collection::new(
            CollectionId::new(collection.id),
            KeyspaceId::new(collection.keyspace_id),
            collection.name,
            affinity_group,
            DocumentKeySchema::new(key_fields),
            fragments,
        ));
    }
    for index in request.indexes {
        changes.indexes.push((
            BucketId::new(index.bucket_id),
            IndexDefinition::new(
                IndexId::new(index.id),
                index.name,
                index_kind(index.kind)?,
                index.fields.into_iter().map(FieldId::new).collect(),
            ),
        ));
    }

    Ok(changes)
}

fn bucket_change(bucket: BucketDefinition) -> Result<(KeyspaceId, Bucket), OperationError> {
    let keyspace_id = KeyspaceId::new(bucket.keyspace_id);
    let policy = match messages::BucketPolicy::try_from(bucket.policy) {
        Ok(messages::BucketPolicy::Strict) => BucketPolicy::Strict,
        Ok(messages::BucketPolicy::Managed) => BucketPolicy::Managed,
        Ok(messages::BucketPolicy::Raw) => BucketPolicy::Raw,
        _ => return Err(OperationError::invalid("bucket policy is required")),
    };
    let bucket = Bucket::new(
        BucketId::new(bucket.id),
        bucket.name,
        bucket.partitions,
        policy,
        SchemaId::new(bucket.schema_id),
        SchemaVersion::new(bucket.schema_version),
    );

    Ok((keyspace_id, bucket))
}

fn key_type(value: i32) -> Result<KeyType, OperationError> {
    match messages::KeyFieldType::try_from(value) {
        Ok(messages::KeyFieldType::U64) => Ok(KeyType::U64),
        Ok(messages::KeyFieldType::I64) => Ok(KeyType::I64),
        Ok(messages::KeyFieldType::String) => Ok(KeyType::String),
        Ok(messages::KeyFieldType::Bytes) => Ok(KeyType::Bytes),
        Ok(messages::KeyFieldType::Uuid) => Ok(KeyType::Uuid),
        _ => Err(OperationError::invalid("key field type is required")),
    }
}

fn cardinality(value: i32) -> Result<FragmentCardinality, OperationError> {
    match messages::FragmentCardinality::try_from(value) {
        Ok(messages::FragmentCardinality::One) => Ok(FragmentCardinality::One),
        Ok(messages::FragmentCardinality::Many) => Ok(FragmentCardinality::Many),
        _ => Err(OperationError::invalid("fragment cardinality is required")),
    }
}

fn index_kind(value: i32) -> Result<IndexKind, OperationError> {
    match messages::IndexKind::try_from(value) {
        Ok(messages::IndexKind::Equality) => Ok(IndexKind::Equality),
        Ok(messages::IndexKind::Range) => Ok(IndexKind::Range),
        Ok(messages::IndexKind::Composite) => Ok(IndexKind::Composite),
        Ok(messages::IndexKind::Unique) => Ok(IndexKind::Unique),
        _ => Err(OperationError::invalid("index kind is required")),
    }
}

fn authorize_catalog_changes(
    snapshot: &CatalogSnapshot,
    principal: &Principal,
    changes: &CatalogChanges,
) -> Result<(), OperationError> {
    if !changes.schemas.is_empty() {
        require_admin(principal, "*", "*")?;
    }

    let mut keyspaces: BTreeMap<KeyspaceId, &str> = snapshot
        .keyspaces
        .iter()
        .map(|(id, keyspace)| (*id, keyspace.name()))
        .collect();
    for keyspace in &changes.keyspaces {
        keyspaces.insert(keyspace.id(), keyspace.name());
    }
    let mut bucket_keyspaces = snapshot.bucket_keyspaces.clone();
    for (keyspace_id, bucket) in &changes.buckets {
        bucket_keyspaces.insert(bucket.id(), *keyspace_id);
    }

    for keyspace in &changes.keyspaces {
        require_admin(principal, keyspace.name(), "*")?;
    }
    for (keyspace_id, _) in &changes.buckets {
        let name = keyspaces.get(keyspace_id).copied().ok_or_else(|| {
            OperationError::new(status::NOT_FOUND, "bucket references an unknown keyspace")
        })?;
        require_admin(principal, name, "*")?;
    }
    for collection in &changes.collections {
        let name = keyspaces
            .get(&collection.keyspace_id())
            .copied()
            .ok_or_else(|| {
                OperationError::new(
                    status::NOT_FOUND,
                    "collection references an unknown keyspace",
                )
            })?;
        require_admin(principal, name, collection.name())?;
    }
    for (bucket_id, _) in &changes.indexes {
        let keyspace_id = bucket_keyspaces.get(bucket_id).copied().ok_or_else(|| {
            OperationError::new(status::NOT_FOUND, "index references an unknown bucket")
        })?;
        let name = keyspaces
            .get(&keyspace_id)
            .copied()
            .ok_or_else(|| OperationError::internal("bucket references an unknown keyspace"))?;
        require_admin(principal, name, "*")?;
    }

    Ok(())
}

fn require_admin(
    principal: &Principal,
    keyspace: &str,
    collection: &str,
) -> Result<(), OperationError> {
    if principal.authorize(
        AuthTarget {
            keyspace,
            collection,
        },
        Action::Admin,
    ) {
        Ok(())
    } else {
        Err(OperationError::new(
            status::PERMISSION_DENIED,
            "permission denied",
        ))
    }
}

fn authorize_collection(
    snapshot: &CatalogSnapshot,
    principal: &Principal,
    collection_id: CollectionId,
    action: Action,
) -> Result<(), OperationError> {
    let collection = snapshot
        .collections
        .get(&collection_id)
        .ok_or_else(|| OperationError::new(status::NOT_FOUND, "collection was not found"))?;
    let keyspace = snapshot
        .keyspaces
        .get(&collection.keyspace_id())
        .ok_or_else(|| OperationError::internal("collection references missing keyspace"))?;
    let target = AuthTarget {
        keyspace: keyspace.name(),
        collection: collection.name(),
    };
    if principal.authorize(target, action) {
        Ok(())
    } else {
        Err(OperationError::new(
            status::PERMISSION_DENIED,
            "permission denied",
        ))
    }
}

fn resolve_collection<'a>(
    snapshot: &'a recolith_catalog::CatalogSnapshot,
    request: &DescribeCollectionRequest,
) -> Result<&'a recolith_model::Collection, OperationError> {
    let target = request
        .target
        .as_ref()
        .ok_or_else(|| OperationError::invalid("collection target is required"))?;
    match target {
        describe_collection_request::Target::CollectionId(id) => {
            let collection = snapshot
                .collections
                .get(&CollectionId::new(*id))
                .ok_or_else(|| {
                    OperationError::new(status::NOT_FOUND, "collection was not found")
                })?;
            if !request.keyspace.is_empty()
                && !keyspace_matches(snapshot, collection, &request.keyspace)
            {
                return Err(OperationError::new(
                    status::NOT_FOUND,
                    "collection was not found in the requested keyspace",
                ));
            }

            Ok(collection)
        }
        describe_collection_request::Target::CollectionName(name) => {
            let mut matches = snapshot
                .collections
                .values()
                .filter(|collection| collection.name() == name)
                .filter(|collection| {
                    request.keyspace.is_empty()
                        || keyspace_matches(snapshot, collection, &request.keyspace)
                });
            let Some(collection) = matches.next() else {
                return Err(OperationError::new(
                    status::NOT_FOUND,
                    "collection was not found",
                ));
            };
            if matches.next().is_some() {
                return Err(OperationError::invalid(
                    "collection name is ambiguous; specify a keyspace",
                ));
            }

            Ok(collection)
        }
    }
}

fn keyspace_matches(
    snapshot: &recolith_catalog::CatalogSnapshot,
    collection: &recolith_model::Collection,
    keyspace: &str,
) -> bool {
    snapshot
        .keyspaces
        .get(&collection.keyspace_id())
        .is_some_and(|candidate| candidate.name() == keyspace)
}

pub(crate) fn document_put<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: DocumentPutRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    authorize_collection(
        &snapshot,
        principal,
        CollectionId::new(request.collection_id),
        Action::Write,
    )?;

    let idempotency_key = request
        .meta
        .as_ref()
        .map(|meta| meta.idempotency_key.as_slice())
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            OperationError::new(
                status::INVALID_REQUEST,
                "mutations require a non-empty idempotency key",
            )
        })?;
    if idempotency_key.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(OperationError::new(
            status::INVALID_REQUEST,
            "idempotency key is too long",
        ));
    }
    let request_hash = hash_document_put(&request);
    let record_key = idempotency_record_key(principal.id(), idempotency_key);

    let mut write = DocumentWrite::new(
        CollectionId::new(request.collection_id),
        request.document_key.clone(),
        PartitionId::new(request.partition),
    );
    for fragment in &request.fragments {
        let data = convert::record_from_wire(&fragment.fields)
            .map_err(|error| OperationError::new(status::INVALID_REQUEST, error.to_string()))?;
        let mut fragment_write = FragmentWriteRequest::new(
            FragmentId::new(fragment.fragment_id),
            fragment.subkey.clone(),
            data,
        );
        if let Some(expected) = fragment.if_match {
            fragment_write = fragment_write.with_if_match(CommitTs::new(expected));
        }
        write = write.with_fragment(fragment_write);
    }
    write = write.with_idempotency(Idempotency::new(record_key, request_hash));

    let commit_ts = store.write(&write).map_err(map_document)?;
    let versions = request
        .fragments
        .iter()
        .map(|fragment| FragmentVersion {
            fragment_id: fragment.fragment_id,
            subkey: fragment.subkey.clone(),
            commit_ts: commit_ts.get(),
        })
        .collect();
    let response = DocumentPutResponse {
        commit_ts: commit_ts.get(),
        versions,
    };

    Ok(OperationResponse {
        opcode: opcode::DOC_PUT,
        payload: response.encode_to_vec(),
    })
}

pub(crate) struct QueryPage {
    pub(crate) documents: Vec<DocumentResult>,
    pub(crate) next_cursor: Vec<u8>,
}

pub(crate) fn query<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: QueryRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let limit = query_limit(request.limit, &request.rql)?;
    let page = query_page(
        store,
        principal,
        request.partition,
        &request.rql,
        &request.parameters,
        limit,
        &request.cursor,
    )?;
    let response = QueryResponse {
        documents: page.documents,
        next_cursor: page.next_cursor,
    };

    Ok(OperationResponse {
        opcode: opcode::QUERY,
        payload: response.encode_to_vec(),
    })
}

pub(crate) fn bind_wire_query<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    partition: u32,
    rql: &str,
    wire_parameters: &[messages::NamedParameter],
) -> Result<(CatalogSnapshot, BoundQuery), OperationError> {
    let parsed = parse_document_query(rql).map_err(map_rql)?;
    let parameters = parse_wire_parameters(wire_parameters)?;
    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    let query = bind_query(&snapshot, &parsed, PartitionId::new(partition), &parameters)
        .map_err(map_bind)?;
    authorize_collection(&snapshot, principal, query.collection_id(), Action::Read)?;

    Ok((snapshot, query))
}

fn parse_wire_parameters(
    wire_parameters: &[messages::NamedParameter],
) -> Result<BindParameters, OperationError> {
    let mut parameters = BindParameters::new();
    for parameter in wire_parameters {
        let value = parameter.value.as_ref().ok_or_else(|| {
            OperationError::new(
                status::INVALID_REQUEST,
                format!("parameter {} has no value", parameter.name),
            )
        })?;
        parameters.insert(
            parameter.name.clone(),
            literal_from_wire(value)
                .map_err(|error| OperationError::new(status::INVALID_REQUEST, error.to_string()))?,
        );
    }

    Ok(parameters)
}

pub(crate) fn aggregate<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: AggregateRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let _read = store.read_guard().map_err(map_document)?;
    let parsed = parse_aggregate_query(&request.rql).map_err(map_rql)?;
    let parameters = parse_wire_parameters(&request.parameters)?;
    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    let query = bind_aggregate_query(
        &snapshot,
        &parsed,
        PartitionId::new(request.partition),
        &parameters,
    )
    .map_err(map_bind)?;
    authorize_collection(&snapshot, principal, query.collection_id(), Action::Read)?;
    let limit = aggregate_limit(request.limit, &parsed);
    let page = store
        .query_engine()
        .execute_aggregate(&query, limit)
        .map_err(map_query)?;
    let columns = query
        .aggregates()
        .iter()
        .map(|aggregate| AggregateColumn {
            function: wire_aggregate_function(aggregate.function()) as i32,
            fragment_id: aggregate.fragment().map(FragmentId::get),
            field_id: aggregate.field().map(FieldId::get),
        })
        .collect();
    let rows = page
        .rows()
        .iter()
        .map(|row| AggregateRow {
            group: row.group().map(convert::value_to_wire),
            values: row
                .values()
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(convert::value_to_wire)
                        .unwrap_or_default()
                })
                .collect(),
        })
        .collect();
    let response = AggregateResponse {
        catalog_generation: snapshot.generation.get(),
        columns,
        group_fragment_id: query.group().map(|group| group.fragment().get()),
        group_field_id: query.group().map(|group| group.field().get()),
        rows,
        truncated: page.truncated(),
    };

    Ok(OperationResponse {
        opcode: opcode::AGGREGATE,
        payload: response.encode_to_vec(),
    })
}

fn aggregate_limit(request_limit: u64, parsed: &recolith_query::RqlAggregateQuery) -> usize {
    let limit = match parsed.limit() {
        Some(limit) if request_limit > 0 => request_limit.min(limit),
        Some(limit) => limit,
        None if request_limit > 0 => request_limit,
        None => DEFAULT_QUERY_LIMIT,
    };

    limit.min(MAX_QUERY_LIMIT) as usize
}

const fn wire_aggregate_function(function: QueryAggregateFunction) -> messages::AggregateFunction {
    match function {
        QueryAggregateFunction::Count => messages::AggregateFunction::Count,
        QueryAggregateFunction::Sum => messages::AggregateFunction::Sum,
        QueryAggregateFunction::Min => messages::AggregateFunction::Min,
        QueryAggregateFunction::Max => messages::AggregateFunction::Max,
        QueryAggregateFunction::Avg => messages::AggregateFunction::Avg,
    }
}

pub(crate) fn explain<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: ExplainRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let _read = store.read_guard().map_err(map_document)?;
    let (snapshot, query) = bind_wire_query(
        store,
        principal,
        request.partition,
        &request.rql,
        &request.parameters,
    )?;
    let collection_id = query.collection_id();
    let engine = store.query_engine();
    let plan = match query {
        BoundQuery::Document(query) => {
            let plan = engine.plan(&query).map_err(map_query)?;
            explain_response::Plan::Document(explain_document_plan(
                collection_id,
                request.partition,
                &plan,
            ))
        }
        BoundQuery::Scan(query) => {
            let plan = engine.plan_scan(&query).map_err(map_query)?;
            explain_response::Plan::Scan(explain_scan_plan(
                &snapshot,
                collection_id,
                request.partition,
                &plan,
            ))
        }
    };
    let response = ExplainResponse {
        catalog_generation: snapshot.generation.get(),
        plan: Some(plan),
    };

    Ok(OperationResponse {
        opcode: opcode::EXPLAIN,
        payload: response.encode_to_vec(),
    })
}

fn explain_document_plan(
    collection_id: CollectionId,
    partition: u32,
    plan: &recolith_query::QueryPlan,
) -> ExplainDocumentPlan {
    ExplainDocumentPlan {
        collection_id: collection_id.get(),
        document_key: plan.document_key().to_vec(),
        fragment_ids: plan.fragment_ids().iter().map(|id| id.get()).collect(),
        projections: plan.projection().iter().map(explain_selection).collect(),
        filters: plan.filters().iter().map(explain_filter).collect(),
        partition,
    }
}

fn explain_scan_plan(
    snapshot: &CatalogSnapshot,
    collection_id: CollectionId,
    partition: u32,
    plan: &recolith_query::ScanPlan,
) -> ExplainScanPlan {
    ExplainScanPlan {
        collection_id: collection_id.get(),
        partition,
        fragment_ids: plan.fragment_ids().iter().map(|id| id.get()).collect(),
        drives: plan
            .drives()
            .iter()
            .map(|drive| explain_drive(snapshot, drive))
            .collect(),
        filters: plan.filters().iter().map(explain_filter).collect(),
        projections: plan.projection().iter().map(explain_selection).collect(),
        order: plan.order().map(|order| ExplainOrder {
            fragment_id: order.fragment().get(),
            field_id: order.field().get(),
            descending: order.descending(),
            index_id: plan.order_index().map(IndexId::get),
        }),
        key_filters: plan
            .key_filters()
            .iter()
            .map(|filter| ExplainKeyFilter {
                field: u32::try_from(filter.field()).unwrap_or(u32::MAX),
                op: explain_op(filter.op()) as i32,
            })
            .collect(),
        candidate_limit: u64::try_from(plan.candidate_limit()).unwrap_or(u64::MAX),
    }
}

fn explain_drive(snapshot: &CatalogSnapshot, drive: &ScanDriveInfo) -> ExplainDrive {
    let indexes = snapshot
        .indexes
        .get(&drive.bucket())
        .map_or_else(Vec::new, |definitions| {
            drive
                .indexes()
                .iter()
                .filter_map(|index| definitions.get(index))
                .map(explain_index)
                .collect()
        });

    ExplainDrive {
        bucket_id: drive.bucket().get(),
        fragment_id: drive.fragment().get(),
        indexes,
        bucket_scan: drive.is_bucket_scan(),
    }
}

fn explain_index(index: &IndexDefinition) -> ExplainIndex {
    ExplainIndex {
        index_id: index.id().get(),
        field_ids: index.fields().iter().map(|field| field.get()).collect(),
        kind: super::metadata::index_kind(index.kind()) as i32,
    }
}

fn explain_filter(filter: &Filter) -> ExplainFilter {
    ExplainFilter {
        fragment_id: filter.fragment().get(),
        field_id: filter.field().get(),
        op: explain_op(filter.op()) as i32,
    }
}

fn explain_selection(selection: &FieldSelection) -> ExplainFieldSelection {
    ExplainFieldSelection {
        fragment_id: selection.fragment().get(),
        field_id: selection.field().get(),
    }
}

const fn explain_op(op: FilterOp) -> ExplainFilterOp {
    match op {
        FilterOp::Eq => ExplainFilterOp::Eq,
        FilterOp::Ne => ExplainFilterOp::Ne,
        FilterOp::Lt => ExplainFilterOp::Lt,
        FilterOp::Le => ExplainFilterOp::Le,
        FilterOp::Gt => ExplainFilterOp::Gt,
        FilterOp::Ge => ExplainFilterOp::Ge,
    }
}

pub(crate) fn query_page<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    partition: u32,
    rql: &str,
    wire_parameters: &[messages::NamedParameter],
    limit: usize,
    cursor: &[u8],
) -> Result<QueryPage, OperationError> {
    let _read = store.read_guard().map_err(map_document)?;
    let (snapshot, query) = bind_wire_query(store, principal, partition, rql, wire_parameters)?;
    let query_hash = query_cursor::query_hash(partition, rql, wire_parameters);
    let cursor = if cursor.is_empty() {
        None
    } else {
        let cursor = query_cursor::decode(cursor).map_err(map_cursor)?;
        if cursor.query_hash != query_hash {
            return Err(map_cursor(query_cursor::CursorError::QueryMismatch));
        }
        if cursor.catalog_generation != snapshot.generation.get() {
            return Err(map_cursor(query_cursor::CursorError::StaleCatalog));
        }
        Some(cursor)
    };

    let engine = store.query_engine();
    let (documents, next_cursor) = match query {
        BoundQuery::Document(query) => {
            if cursor.is_some() {
                return Err(OperationError::invalid(
                    "cursor is not valid for a point query",
                ));
            }
            let plan = engine.plan(&query).map_err(map_query)?;
            let mut fragments = engine.execute(&plan).map_err(map_query)?;
            for records in fragments.values_mut() {
                records.truncate(limit);
            }
            let documents = if fragments.is_empty() {
                Vec::new()
            } else {
                vec![DocumentResult {
                    document_key: query.document_key().to_vec(),
                    fragments: convert::fragments_to_wire(&fragments),
                }]
            };
            (documents, Vec::new())
        }
        BoundQuery::Scan(query) => {
            let plan = engine.plan_scan(&query).map_err(map_query)?;
            let page = engine
                .execute_scan_page(
                    &plan,
                    limit,
                    cursor.as_ref().map(|cursor| ScanPosition {
                        document_key: &cursor.last_key,
                        order_value: cursor.order_value.as_deref(),
                    }),
                )
                .map_err(map_query)?;
            let documents = page
                .documents
                .into_iter()
                .map(|(document_key, fragments)| DocumentResult {
                    document_key,
                    fragments: convert::fragments_to_wire(&fragments),
                })
                .collect::<Vec<_>>();
            let next_cursor = if page.has_more {
                let last_key = documents
                    .last()
                    .expect("has_more implies a non-empty page")
                    .document_key
                    .clone();
                query_cursor::encode(&query_cursor::QueryCursor {
                    last_key,
                    order_value: page.last_order_value,
                    catalog_generation: snapshot.generation.get(),
                    query_hash,
                })
            } else {
                Vec::new()
            };
            (documents, next_cursor)
        }
    };

    Ok(QueryPage {
        documents,
        next_cursor,
    })
}

pub(crate) fn query_limit(request_limit: u64, rql: &str) -> Result<usize, OperationError> {
    if request_limit > 0 {
        return Ok(request_limit.min(MAX_QUERY_LIMIT) as usize);
    }
    let parsed = parse_document_query(rql).map_err(map_rql)?;

    Ok(parsed
        .limit()
        .unwrap_or(DEFAULT_QUERY_LIMIT)
        .min(MAX_QUERY_LIMIT) as usize)
}

fn map_cursor(error: query_cursor::CursorError) -> OperationError {
    match error {
        query_cursor::CursorError::StaleCatalog => {
            OperationError::new(status::STALE_CATALOG, "cursor catalog generation is stale")
        }
        query_cursor::CursorError::Malformed
        | query_cursor::CursorError::UnsupportedVersion
        | query_cursor::CursorError::QueryMismatch => {
            OperationError::invalid("query cursor is invalid")
        }
    }
}

pub(crate) fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
        })
}

pub(crate) fn check_meta<B: StorageBackend>(
    store: &DocumentStore<B>,
    meta: Option<&RequestMeta>,
) -> Result<(), OperationError> {
    let Some(meta) = meta else {
        return Ok(());
    };
    if meta.deadline_unix_micros != 0 && now_micros() > meta.deadline_unix_micros {
        return Err(OperationError::new(
            status::DEADLINE_EXCEEDED,
            "request deadline exceeded",
        ));
    }
    if meta.catalog_generation != 0 && meta.catalog_generation != store.catalog_generation().get() {
        return Err(OperationError::new(
            status::STALE_CATALOG,
            "catalog generation is stale",
        ));
    }

    Ok(())
}

fn hash_document_put(request: &DocumentPutRequest) -> [u8; 32] {
    let mut canonical = request.clone();
    if let Some(meta) = &mut canonical.meta {
        meta.deadline_unix_micros = 0;
        meta.trace_id.clear();
        meta.catalog_generation = 0;
    }
    let digest = Sha256::digest(canonical.encode_to_vec());
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);

    output
}

fn idempotency_record_key(client_id: &str, key: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(client_id.as_bytes());
    hasher.update([0_u8]);
    hasher.update(key);
    let digest = hasher.finalize();
    let mut output = Vec::with_capacity(IDEMPOTENCY_PREFIX.len() + digest.len());
    output.extend_from_slice(IDEMPOTENCY_PREFIX);
    output.extend_from_slice(&digest);

    output
}

fn literal_from_wire(value: &messages::Value) -> Result<RqlLiteral, ConversionError> {
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| ConversionError("parameter has no value".to_owned()))?;

    let literal = match kind {
        value::Kind::BoolValue(value) => RqlLiteral::Bool(*value),
        value::Kind::I32Value(value) => RqlLiteral::Integer(i64::from(*value)),
        value::Kind::I64Value(value) => RqlLiteral::Integer(*value),
        value::Kind::U32Value(value) => RqlLiteral::Integer(i64::from(*value)),
        value::Kind::U64Value(value) if *value <= i64::MAX as u64 => {
            RqlLiteral::Integer(*value as i64)
        }
        value::Kind::U64Value(value) => RqlLiteral::Unsigned(*value),
        value::Kind::F32Value(value) => RqlLiteral::Float(f64::from(*value)),
        value::Kind::F64Value(value) => RqlLiteral::Float(*value),
        value::Kind::StringValue(value) => RqlLiteral::String(value.clone()),
        _ => {
            return Err(ConversionError(
                "parameter type is not supported by RQL binding".to_owned(),
            ));
        }
    };

    Ok(literal)
}

#[derive(Debug)]
struct ConversionError(String);

impl fmt::Display for ConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ConversionError {}

fn map_document(error: DocumentError) -> OperationError {
    match error {
        DocumentError::UnknownCollection(_)
        | DocumentError::UnknownFragment(_)
        | DocumentError::MissingMetadata(_) => {
            OperationError::new(status::NOT_FOUND, error.to_string())
        }
        DocumentError::InvalidRequest(_) => {
            OperationError::new(status::INVALID_REQUEST, error.to_string())
        }
        DocumentError::IdempotencyConflict => {
            OperationError::new(status::CONFLICT, error.to_string())
        }
        DocumentError::InvalidIdempotencyRecord => {
            OperationError::new(status::INTERNAL, error.to_string())
        }
        DocumentError::TargetNotEmpty(_) | DocumentError::BackfillTooLarge { .. } => {
            OperationError::new(status::FAILED_PRECONDITION, error.to_string())
        }
        DocumentError::MigrationRequired | DocumentError::Migration(_) => {
            OperationError::new(status::FAILED_PRECONDITION, error.to_string())
        }
        DocumentError::StaleCatalog => {
            OperationError::new(status::STALE_CATALOG, error.to_string())
        }
        DocumentError::Catalog(CatalogError::Conflict(_) | CatalogError::Duplicate(_)) => {
            OperationError::new(status::CONFLICT, error.to_string())
        }
        DocumentError::Catalog(CatalogError::Referenced(_)) => {
            OperationError::new(status::FAILED_PRECONDITION, error.to_string())
        }
        DocumentError::Catalog(CatalogError::NotFound(_)) => {
            OperationError::new(status::NOT_FOUND, error.to_string())
        }
        DocumentError::Catalog(
            CatalogError::InvalidReference(_)
            | CatalogError::InvalidIndex(_)
            | CatalogError::Schema(_)
            | CatalogError::SchemaVersionConflict
            | CatalogError::SchemaRequiresMigration,
        ) => OperationError::new(status::INVALID_REQUEST, error.to_string()),
        DocumentError::Transaction(TransactionError::VersionConflict { .. }) => {
            OperationError::new(status::CONFLICT, error.to_string())
        }
        DocumentError::Transaction(TransactionError::Storage(StorageError::UniqueViolation {
            ..
        }))
        | DocumentError::Storage(StorageError::UniqueViolation { .. }) => {
            OperationError::new(status::CONFLICT, error.to_string())
        }
        DocumentError::Transaction(TransactionError::Storage(StorageError::SchemaMismatch {
            ..
        }))
        | DocumentError::Storage(StorageError::SchemaMismatch { .. }) => {
            OperationError::new(status::SCHEMA_MISMATCH, error.to_string())
        }
        _ => OperationError::new(status::INTERNAL, error.to_string()),
    }
}

fn map_query(error: QueryError) -> OperationError {
    match error {
        QueryError::NotFound(_) => OperationError::new(status::NOT_FOUND, error.to_string()),
        QueryError::InvalidRequest(_) => {
            OperationError::new(status::INVALID_REQUEST, error.to_string())
        }
        QueryError::ResourceExhausted(_) | QueryError::AggregateOverflow(_) => {
            OperationError::new(status::RESOURCE_EXHAUSTED, error.to_string())
        }
        QueryError::Storage(StorageError::SchemaMismatch { .. }) => {
            OperationError::new(status::SCHEMA_MISMATCH, error.to_string())
        }
        _ => OperationError::new(status::INTERNAL, error.to_string()),
    }
}

fn map_bind(error: RqlBindError) -> OperationError {
    let status_code = match error {
        RqlBindError::UnknownCollection(_)
        | RqlBindError::UnknownFragment(_)
        | RqlBindError::UnknownKeyField(_) => status::NOT_FOUND,
        RqlBindError::UnknownField { .. }
        | RqlBindError::MissingKeyField(_)
        | RqlBindError::DuplicateKeyField(_)
        | RqlBindError::NotPointQuery
        | RqlBindError::MissingParameter(_)
        | RqlBindError::InvalidLiteral { .. }
        | RqlBindError::UnqualifiedPath(_)
        | RqlBindError::InvalidOrder(_)
        | RqlBindError::UnsortableField(_)
        | RqlBindError::InvalidAggregate(_)
        | RqlBindError::UnsortableGroup(_)
        | RqlBindError::InvalidAggregateType { .. }
        | RqlBindError::FragmentNotIncluded(_)
        | RqlBindError::Key(_) => status::INVALID_REQUEST,
        RqlBindError::MissingMetadata(_) => status::INTERNAL,
    };

    OperationError::new(status_code, error.to_string())
}

fn map_rql(error: RqlError) -> OperationError {
    OperationError::new(status::INVALID_REQUEST, error.to_string())
}
