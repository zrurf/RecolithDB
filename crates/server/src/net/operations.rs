use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use recolith_backend::StorageBackend;
use recolith_catalog::{CatalogChanges, CatalogDrop, CatalogError, CatalogSnapshot};
use recolith_common::{
    BucketId, CatalogGeneration, CollectionId, CommitTs, FieldId, FragmentId, IndexId, KeyspaceId,
    PartitionId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_protocol::messages::{
    self, BucketDefinition, CatalogApplyRequest, CatalogApplyResponse, CatalogDropRequest,
    CatalogDropResponse, DescribeCollectionRequest, DescribeCollectionResponse, DocumentGetRequest,
    DocumentGetResponse, DocumentPutRequest, DocumentPutResponse, DocumentResult, FragmentVersion,
    QueryRequest, QueryResponse, RequestMeta, describe_collection_request, opcode, status, value,
};
use recolith_query::error::QueryError;
use recolith_query::{
    BindParameters, DocumentQuery, FieldSelection, RqlBindError, RqlError, RqlLiteral,
    bind_document_query, parse_document_query,
};
use recolith_schema::parse_record;
use recolith_security::{Action, AuthTarget, Principal};
use recolith_storage::StorageError;
use recolith_transaction::TransactionError;
use sha2::{Digest, Sha256};

use crate::document::{
    DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest, Idempotency,
};

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

pub(crate) fn query<B: StorageBackend>(
    store: &DocumentStore<B>,
    principal: &Principal,
    request: QueryRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;
    let _read = store.read_guard().map_err(map_document)?;

    let parsed = parse_document_query(&request.rql).map_err(map_rql)?;
    let mut parameters = BindParameters::new();
    for parameter in &request.parameters {
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

    let snapshot = store.catalog_snapshot().map_err(map_document)?;
    let query = bind_document_query(
        &snapshot,
        &parsed,
        PartitionId::new(request.partition),
        &parameters,
    )
    .map_err(map_bind)?;
    authorize_collection(&snapshot, principal, query.collection_id(), Action::Read)?;
    let limit = if request.limit > 0 {
        request.limit
    } else {
        parsed.limit().unwrap_or(DEFAULT_QUERY_LIMIT)
    }
    .min(MAX_QUERY_LIMIT) as usize;

    let engine = store.query_engine();
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
    let response = QueryResponse { documents };

    Ok(OperationResponse {
        opcode: opcode::QUERY,
        payload: response.encode_to_vec(),
    })
}

pub(crate) fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
        })
}

fn check_meta<B: StorageBackend>(
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
        | RqlBindError::MissingParameter(_)
        | RqlBindError::InvalidLiteral { .. }
        | RqlBindError::UnqualifiedPath(_)
        | RqlBindError::FragmentNotIncluded(_)
        | RqlBindError::Key(_) => status::INVALID_REQUEST,
        RqlBindError::MissingMetadata(_) => status::INTERNAL,
    };

    OperationError::new(status_code, error.to_string())
}

fn map_rql(error: RqlError) -> OperationError {
    OperationError::new(status::INVALID_REQUEST, error.to_string())
}
