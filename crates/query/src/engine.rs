use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use recolith_backend::StorageBackend;
use recolith_catalog::Catalog;
use recolith_common::{FieldId, FragmentId};
use recolith_model::{Fragment, FragmentCardinality};
use recolith_storage::{
    DocumentFragments, FragmentData, FragmentMode, FragmentReader, RecordData, Value,
    encode_index_value, read_document,
};

use crate::error::{QueryError, Result};
use crate::ir::{DocumentQuery, FieldSelection, Filter, FilterOp};
use crate::request::DocumentReadRequest;

/// Bound document read plan.
#[derive(Clone, Debug)]
pub struct QueryPlan {
    document_key: Vec<u8>,
    fragments: Vec<FragmentReader>,
    filters: Vec<Filter>,
    projection: Vec<FieldSelection>,
}

impl QueryPlan {
    /// Returns the fragment readers selected by the plan.
    #[must_use]
    pub fn readers(&self) -> &[FragmentReader] {
        &self.fragments
    }
}

/// Query engine bound to a local catalog and backend.
pub struct QueryEngine<B: StorageBackend> {
    catalog: Arc<Catalog>,
    backend: Arc<B>,
}

impl<B: StorageBackend> QueryEngine<B> {
    /// Creates a query engine.
    #[must_use]
    pub fn new(catalog: Arc<Catalog>, backend: Arc<B>) -> Self {
        Self { catalog, backend }
    }

    /// Binds a document query to catalog metadata and fragments.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for missing metadata, schema, or invalid field references.
    pub fn plan(&self, query: &DocumentQuery) -> Result<QueryPlan> {
        let catalog = self.catalog.snapshot()?;
        let collection = catalog
            .collections
            .get(&query.collection_id())
            .ok_or(QueryError::NotFound("collection"))?;

        let selected: Vec<&Fragment> = if query.fragments().is_empty() {
            collection.fragments().iter().collect()
        } else {
            query
                .fragments()
                .iter()
                .map(|fragment_id| {
                    collection
                        .fragments()
                        .iter()
                        .find(|fragment| fragment.id() == *fragment_id)
                        .ok_or(QueryError::NotFound("fragment"))
                })
                .collect::<Result<Vec<_>>>()?
        };

        let mut readers = Vec::with_capacity(selected.len());
        let mut schemas = BTreeMap::new();
        for fragment in selected {
            let bucket = catalog
                .buckets
                .get(&fragment.bucket_id())
                .ok_or(QueryError::NotFound("bucket"))?;
            let schema = catalog
                .schemas
                .schemas
                .get(&(bucket.schema_id(), bucket.schema_version()))
                .ok_or(QueryError::NotFound("schema"))?;
            let mode = match fragment.cardinality() {
                FragmentCardinality::One => FragmentMode::One,
                FragmentCardinality::Many => FragmentMode::Many,
            };
            schemas.insert(fragment.id(), schema.clone());
            let mut reader = FragmentReader::new(
                fragment.bucket_id(),
                query.partition(),
                fragment.id(),
                mode,
                schema.clone(),
            );
            if mode == FragmentMode::Many
                && let Some(filter) = query.filters().iter().find(|filter| {
                    filter.fragment() == fragment.id() && filter.op() == FilterOp::Eq
                })
                && let Some(index) = catalog.index_for_field(bucket.id(), filter.field())
            {
                reader = reader.with_index(index.id(), encode_index_value(filter.value())?);
            }
            readers.push(reader);
        }

        for filter in query.filters() {
            validate_field(&schemas, filter.fragment(), filter.field())?;
        }
        for selection in query.projection() {
            validate_field(&schemas, selection.fragment(), selection.field())?;
        }

        Ok(QueryPlan {
            document_key: query.document_key().to_vec(),
            fragments: readers,
            filters: query.filters().to_vec(),
            projection: query.projection().to_vec(),
        })
    }

    /// Executes a bound plan against a backend snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for backend or storage assembly failures.
    pub fn execute(&self, plan: &QueryPlan) -> Result<DocumentFragments> {
        let snapshot = self.backend.snapshot()?;
        let assembled = read_document(&snapshot, &plan.document_key, &plan.fragments)?;

        Ok(apply_post_processing(
            assembled,
            &plan.filters,
            &plan.projection,
        ))
    }

    /// Binds and executes a document read request.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for binding or execution failures.
    pub fn read_document(&self, request: &DocumentReadRequest) -> Result<DocumentFragments> {
        let query = DocumentQuery::from(request);

        self.execute(&self.plan(&query)?)
    }
}

impl<B: StorageBackend> fmt::Debug for QueryEngine<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QueryEngine")
    }
}

fn validate_field(
    schemas: &BTreeMap<FragmentId, recolith_schema::RecordSchema>,
    fragment: FragmentId,
    field: FieldId,
) -> Result<()> {
    let schema = schemas
        .get(&fragment)
        .ok_or(QueryError::InvalidRequest(format!(
            "fragment {fragment} is not selected"
        )))?;
    if schema.field(field).is_none() {
        return Err(QueryError::InvalidRequest(format!(
            "field {field} is not in fragment {fragment}"
        )));
    }

    Ok(())
}

fn apply_post_processing(
    assembled: DocumentFragments,
    filters: &[Filter],
    projection: &[FieldSelection],
) -> DocumentFragments {
    let mut output = BTreeMap::new();

    for (fragment_id, records) in assembled {
        let fragment_filters: Vec<&Filter> = filters
            .iter()
            .filter(|filter| filter.fragment() == fragment_id)
            .collect();
        let projected_fields: BTreeSet<FieldId> = projection
            .iter()
            .filter(|selection| selection.fragment() == fragment_id)
            .map(|selection| selection.field())
            .collect();

        let mut filtered = Vec::new();
        for record in records {
            if fragment_filters
                .iter()
                .all(|filter| filter_matches(filter, record.data()))
            {
                let record = project_record(record, &projected_fields);
                filtered.push(record);
            }
        }

        if !filtered.is_empty() {
            output.insert(fragment_id, filtered);
        }
    }

    output
}

fn filter_matches(filter: &Filter, data: &RecordData) -> bool {
    let Some(value) = data.get(filter.field()) else {
        return false;
    };

    match filter.op() {
        FilterOp::Eq => value == filter.value(),
        FilterOp::Ne => value != filter.value(),
        FilterOp::Lt => compare_values(value, filter.value()) == Some(Ordering::Less),
        FilterOp::Le => matches!(
            compare_values(value, filter.value()),
            Some(Ordering::Less | Ordering::Equal)
        ),
        FilterOp::Gt => compare_values(value, filter.value()) == Some(Ordering::Greater),
        FilterOp::Ge => matches!(
            compare_values(value, filter.value()),
            Some(Ordering::Greater | Ordering::Equal)
        ),
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Bool(left), Value::Bool(right)) => left.partial_cmp(right),
        (Value::I32(left), Value::I32(right)) => left.partial_cmp(right),
        (Value::I64(left), Value::I64(right)) => left.partial_cmp(right),
        (Value::U32(left), Value::U32(right)) => left.partial_cmp(right),
        (Value::U64(left), Value::U64(right)) => left.partial_cmp(right),
        (Value::F32(left), Value::F32(right)) => left.partial_cmp(right),
        (Value::F64(left), Value::F64(right)) => left.partial_cmp(right),
        (Value::String(left), Value::String(right)) => left.partial_cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.partial_cmp(right),
        (Value::Timestamp(left), Value::Timestamp(right)) => {
            left.as_micros().partial_cmp(&right.as_micros())
        }
        (Value::Duration(left), Value::Duration(right)) => left.partial_cmp(right),
        (Value::Enum(left), Value::Enum(right)) => left.partial_cmp(right),
        _ => None,
    }
}

fn project_record(record: FragmentData, fields: &BTreeSet<FieldId>) -> FragmentData {
    if fields.is_empty() {
        record
    } else {
        record.project(fields)
    }
}
