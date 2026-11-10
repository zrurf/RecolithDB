use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use recolith_backend::{KeyRange, ScanOptions, Snapshot, StorageBackend};
use recolith_catalog::Catalog;
use recolith_common::{BucketId, FieldId, FragmentId, PartitionId};
use recolith_model::{DocumentKeySchema, Fragment, FragmentCardinality};
use recolith_storage::{
    DocumentFragments, FragmentData, FragmentMode, FragmentReader, IndexBound, IndexFilter,
    RecordData, StorageError, Value, decode_record_key, encode_bucket_prefix, encode_index_value,
    read_document, scan_index, scan_index_components, scan_index_range,
};

use crate::error::{QueryError, Result};
use crate::ir::{DocumentQuery, FieldSelection, Filter, FilterOp};
use crate::request::DocumentReadRequest;
use crate::scan::{DocumentScan, KeyFilter};

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

/// Bound multi-document scan plan.
#[derive(Clone, Debug)]
pub struct ScanPlan {
    key_schema: DocumentKeySchema,
    key_filters: Vec<KeyFilter>,
    fragments: Vec<FragmentReader>,
    drives: Vec<ScanDrive>,
    filters: Vec<Filter>,
    projection: Vec<FieldSelection>,
}

#[derive(Clone, Debug)]
struct ScanDrive {
    bucket: BucketId,
    partition: PartitionId,
    fragment: FragmentId,
    index_filters: Vec<IndexFilter>,
}

const SCAN_BATCH_ENTRIES: usize = 1_024;
const MAX_SCAN_DOCUMENTS: usize = 10_000;

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
            let versions = catalog
                .schemas
                .schemas
                .iter()
                .filter(|((schema_id, _), _)| *schema_id == bucket.schema_id())
                .map(|(_, schema)| schema.clone())
                .collect::<Vec<_>>();
            let mut reader = FragmentReader::new(
                fragment.bucket_id(),
                query.partition(),
                fragment.id(),
                mode,
                schema.clone(),
            )
            .with_schema_versions(versions);
            if mode == FragmentMode::Many {
                for filter in query
                    .filters()
                    .iter()
                    .filter(|filter| filter.fragment() == fragment.id())
                {
                    let Some(index) = leading_field_index(&catalog, bucket.id(), filter.field())
                    else {
                        continue;
                    };
                    match filter.op() {
                        FilterOp::Eq => {
                            reader = reader.with_index_components(
                                index.id(),
                                encode_index_value(filter.value())?,
                                index.fields().len(),
                            );
                        }
                        FilterOp::Lt | FilterOp::Le | FilterOp::Gt | FilterOp::Ge => {
                            let encoded = encode_index_value(filter.value())?;
                            let (lower, upper) = match filter.op() {
                                FilterOp::Lt => (None, Some(IndexBound::new(encoded, false))),
                                FilterOp::Le => (None, Some(IndexBound::new(encoded, true))),
                                FilterOp::Gt => (Some(IndexBound::new(encoded, false)), None),
                                FilterOp::Ge => (Some(IndexBound::new(encoded, true)), None),
                                FilterOp::Eq | FilterOp::Ne => unreachable!(),
                            };
                            reader = reader.with_index_range_components(
                                index.id(),
                                lower,
                                upper,
                                index.fields().len(),
                            );
                        }
                        FilterOp::Ne => {}
                    }
                }
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

    /// Binds a multi-document scan query.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for missing metadata, invalid field references,
    /// or index encoding failures.
    pub fn plan_scan(&self, query: &DocumentScan) -> Result<ScanPlan> {
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
        let mut drives = Vec::new();
        let mut indexed_drive = None;
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
            let versions = catalog
                .schemas
                .schemas
                .iter()
                .filter(|((schema_id, _), _)| *schema_id == bucket.schema_id())
                .map(|(_, schema)| schema.clone())
                .collect::<Vec<_>>();
            readers.push(
                FragmentReader::new(
                    fragment.bucket_id(),
                    query.partition(),
                    fragment.id(),
                    mode,
                    schema.clone(),
                )
                .with_schema_versions(versions),
            );
            schemas.insert(fragment.id(), schema.clone());

            let mut index_filters = Vec::new();
            if mode == FragmentMode::Many {
                for filter in query
                    .filters()
                    .iter()
                    .filter(|filter| filter.fragment() == fragment.id())
                {
                    let Some(index) = leading_field_index(&catalog, bucket.id(), filter.field())
                    else {
                        continue;
                    };
                    match filter.op() {
                        FilterOp::Eq => {
                            index_filters.push(IndexFilter::new_with_components(
                                index.id(),
                                encode_index_value(filter.value())?,
                                index.fields().len(),
                            ));
                        }
                        FilterOp::Lt | FilterOp::Le | FilterOp::Gt | FilterOp::Ge => {
                            let encoded = encode_index_value(filter.value())?;
                            let (lower, upper) = match filter.op() {
                                FilterOp::Lt => (None, Some(IndexBound::new(encoded, false))),
                                FilterOp::Le => (None, Some(IndexBound::new(encoded, true))),
                                FilterOp::Gt => (Some(IndexBound::new(encoded, false)), None),
                                FilterOp::Ge => (Some(IndexBound::new(encoded, true)), None),
                                FilterOp::Eq | FilterOp::Ne => unreachable!(),
                            };
                            index_filters.push(IndexFilter::range_with_components(
                                index.id(),
                                lower,
                                upper,
                                index.fields().len(),
                            ));
                        }
                        FilterOp::Ne => {}
                    }
                }
            }

            if !index_filters.is_empty() && indexed_drive.is_none() {
                indexed_drive = Some(ScanDrive {
                    bucket: fragment.bucket_id(),
                    partition: query.partition(),
                    fragment: fragment.id(),
                    index_filters,
                });
            } else if index_filters.is_empty() {
                drives.push(ScanDrive {
                    bucket: fragment.bucket_id(),
                    partition: query.partition(),
                    fragment: fragment.id(),
                    index_filters: Vec::new(),
                });
            }
        }
        if let Some(drive) = indexed_drive {
            drives = vec![drive];
        }
        let mut seen = BTreeSet::new();
        drives.retain(|drive| seen.insert((drive.bucket, drive.fragment)));

        for filter in query.filters() {
            validate_field(&schemas, filter.fragment(), filter.field())?;
        }
        for selection in query.projection() {
            validate_field(&schemas, selection.fragment(), selection.field())?;
        }

        Ok(ScanPlan {
            key_schema: query.key_schema().clone(),
            key_filters: query.key_filters().to_vec(),
            fragments: readers,
            drives,
            filters: query.filters().to_vec(),
            projection: query.projection().to_vec(),
        })
    }

    /// Executes a scan plan and returns up to `limit` matching documents.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for backend or storage assembly failures.
    pub fn execute_scan(
        &self,
        plan: &ScanPlan,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, DocumentFragments)>> {
        Ok(self.execute_scan_page(plan, limit, None)?.documents)
    }

    /// Executes one scan page after an optional document key.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for backend or storage assembly failures.
    pub fn execute_scan_page(
        &self,
        plan: &ScanPlan,
        limit: usize,
        after: Option<&[u8]>,
    ) -> Result<ScanPage> {
        if limit == 0 {
            return Ok(ScanPage {
                documents: Vec::new(),
                has_more: false,
            });
        }

        let snapshot = self.backend.snapshot()?;
        let mut candidates = BTreeSet::new();
        for drive in &plan.drives {
            if drive.index_filters.is_empty() {
                collect_bucket_candidates(&snapshot, drive, &mut candidates)?;
            } else {
                collect_index_candidates(&snapshot, drive, &mut candidates)?;
            }
            if candidates.len() >= MAX_SCAN_DOCUMENTS {
                break;
            }
        }

        let mut documents = Vec::new();
        let mut has_more = false;
        for key in candidates {
            if after.is_some_and(|after| key.as_slice() <= after) {
                continue;
            }
            if !key_matches(&plan.key_schema, &plan.key_filters, &key)? {
                continue;
            }
            let assembled = read_document(&snapshot, &key, &plan.fragments)?;
            let Some(fragments) = apply_scan_filters(assembled, &plan.filters, &plan.projection)
            else {
                continue;
            };
            if documents.len() < limit {
                documents.push((key, fragments));
            } else {
                has_more = true;
                break;
            }
        }

        Ok(ScanPage {
            documents,
            has_more,
        })
    }
}

/// One scan result page.
#[derive(Clone, Debug)]
pub struct ScanPage {
    /// Documents in document-key order.
    pub documents: Vec<(Vec<u8>, DocumentFragments)>,
    /// Whether another matching document exists after this page.
    pub has_more: bool,
}

impl<B: StorageBackend> fmt::Debug for QueryEngine<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QueryEngine")
    }
}

fn leading_field_index(
    catalog: &recolith_catalog::CatalogSnapshot,
    bucket: BucketId,
    field: FieldId,
) -> Option<&recolith_model::IndexDefinition> {
    catalog
        .indexes
        .get(&bucket)?
        .values()
        .find(|index| index.leading_field() == Some(field))
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

fn collect_bucket_candidates<S: Snapshot>(
    snapshot: &S,
    drive: &ScanDrive,
    candidates: &mut BTreeSet<Vec<u8>>,
) -> Result<()> {
    let prefix = encode_bucket_prefix(drive.bucket);
    let end = KeyRange::prefix(&prefix).end_bound().clone();
    let mut range = KeyRange::prefix(&prefix);
    loop {
        let entries = snapshot.scan(
            &range,
            ScanOptions {
                limit: Some(SCAN_BATCH_ENTRIES),
                reverse: false,
            },
        )?;
        if entries.is_empty() {
            break;
        }
        let last = entries.last().expect("non-empty scan").0.clone();
        for (key, _) in entries {
            let Ok(decoded) = decode_record_key(&key) else {
                continue;
            };
            if decoded.head().partition() != drive.partition || decoded.fragment() != drive.fragment
            {
                continue;
            }
            candidates.insert(decoded.document_key().to_vec());
            if candidates.len() >= MAX_SCAN_DOCUMENTS {
                return Ok(());
            }
        }
        range = KeyRange::new(Bound::Excluded(last), end.clone());
    }

    Ok(())
}

fn collect_index_candidates<S: Snapshot>(
    snapshot: &S,
    drive: &ScanDrive,
    candidates: &mut BTreeSet<Vec<u8>>,
) -> Result<()> {
    let mut record_keys: Option<BTreeSet<Vec<u8>>> = None;
    for filter in &drive.index_filters {
        let keys = scan_drive_index(snapshot, drive, filter)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        record_keys = Some(match record_keys {
            Some(current) => current.intersection(&keys).cloned().collect(),
            None => keys,
        });
    }
    let Some(record_keys) = record_keys else {
        return Ok(());
    };
    for key in record_keys {
        let Ok(decoded) = decode_record_key(&key) else {
            continue;
        };
        if decoded.head().partition() != drive.partition || decoded.fragment() != drive.fragment {
            continue;
        }
        candidates.insert(decoded.document_key().to_vec());
        if candidates.len() >= MAX_SCAN_DOCUMENTS {
            break;
        }
    }

    Ok(())
}

fn scan_drive_index<S: Snapshot>(
    snapshot: &S,
    drive: &ScanDrive,
    filter: &IndexFilter,
) -> Result<Vec<Vec<u8>>> {
    match filter {
        IndexFilter::Equality { .. } => {
            let encoded = filter.encoded_values().expect("equality values");
            if filter.components() == 1 {
                Ok(scan_index(
                    snapshot,
                    drive.bucket,
                    drive.partition,
                    filter.index(),
                    encoded,
                    ScanOptions::default(),
                )?)
            } else {
                Ok(scan_index_components(
                    snapshot,
                    drive.bucket,
                    drive.partition,
                    filter.index(),
                    encoded,
                    filter.components(),
                    ScanOptions::default(),
                )?)
            }
        }
        IndexFilter::Range { .. } => {
            let (lower, upper) = filter.bounds().expect("range bounds");
            Ok(scan_index_range(
                snapshot,
                drive.bucket,
                drive.partition,
                filter.index(),
                lower.map(|bound| (bound.encoded_values(), bound.inclusive())),
                upper.map(|bound| (bound.encoded_values(), bound.inclusive())),
                filter.components(),
                ScanOptions::default(),
            )?)
        }
    }
}

fn key_matches(schema: &DocumentKeySchema, filters: &[KeyFilter], key: &[u8]) -> Result<bool> {
    if filters.is_empty() {
        return Ok(true);
    }
    let values = schema
        .decode(key)
        .map_err(|error| QueryError::Storage(StorageError::InvalidKey(error.to_string())))?;
    for filter in filters {
        let Some(value) = values.get(filter.field()) else {
            return Ok(false);
        };
        let Some(ordering) = value.compare(filter.value()) else {
            return Ok(false);
        };
        let matches = match filter.op() {
            FilterOp::Eq => ordering == Ordering::Equal,
            FilterOp::Ne => ordering != Ordering::Equal,
            FilterOp::Lt => ordering == Ordering::Less,
            FilterOp::Le => ordering != Ordering::Greater,
            FilterOp::Gt => ordering == Ordering::Greater,
            FilterOp::Ge => ordering != Ordering::Less,
        };
        if !matches {
            return Ok(false);
        }
    }

    Ok(true)
}

fn apply_scan_filters(
    assembled: DocumentFragments,
    filters: &[Filter],
    projection: &[FieldSelection],
) -> Option<DocumentFragments> {
    let mut filtered_fragments: BTreeSet<FragmentId> =
        filters.iter().map(Filter::fragment).collect();
    let mut output = BTreeMap::new();

    for (fragment_id, records) in assembled {
        let fragment_filters: Vec<&Filter> = filters
            .iter()
            .filter(|filter| filter.fragment() == fragment_id)
            .collect();
        let projected: BTreeSet<FieldId> = projection
            .iter()
            .filter(|selection| selection.fragment() == fragment_id)
            .map(|selection| selection.field())
            .collect();
        if fragment_filters.is_empty() {
            if !records.is_empty() {
                output.insert(
                    fragment_id,
                    records
                        .into_iter()
                        .map(|record| project_record(record, &projected))
                        .collect(),
                );
            }
            continue;
        }

        filtered_fragments.remove(&fragment_id);
        let matched: Vec<FragmentData> = records
            .into_iter()
            .filter(|record| {
                fragment_filters
                    .iter()
                    .all(|filter| filter_matches(filter, record.data()))
            })
            .map(|record| project_record(record, &projected))
            .collect();
        if matched.is_empty() {
            return None;
        }
        output.insert(fragment_id, matched);
    }

    if !filtered_fragments.is_empty() {
        return None;
    }

    Some(output)
}
