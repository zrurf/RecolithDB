use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use recolith_backend::StorageBackend;
use recolith_common::{CollectionId, CommitTs, FieldId, FragmentId, PartitionId};
use recolith_protocol::messages::{
    self, DocumentGetRequest, DocumentGetResponse, DocumentPutRequest, DocumentPutResponse,
    DocumentResult, FragmentVersion, QueryRequest, QueryResponse, RequestMeta, opcode, status,
    value,
};
use recolith_query::error::QueryError;
use recolith_query::{
    BindParameters, DocumentQuery, FieldSelection, RqlBindError, RqlError, RqlLiteral,
    bind_document_query, parse_document_query,
};
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
    request: DocumentGetRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;

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

pub(crate) fn document_put<B: StorageBackend>(
    store: &DocumentStore<B>,
    client_id: &str,
    request: DocumentPutRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;

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
    let record_key = idempotency_record_key(client_id, idempotency_key);

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
    request: QueryRequest,
) -> Result<OperationResponse, OperationError> {
    check_meta(store, request.meta.as_ref())?;

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
        DocumentError::Transaction(TransactionError::VersionConflict { .. }) => {
            OperationError::new(status::CONFLICT, error.to_string())
        }
        DocumentError::Transaction(TransactionError::Storage(StorageError::UniqueViolation {
            ..
        })) => OperationError::new(status::CONFLICT, error.to_string()),
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
