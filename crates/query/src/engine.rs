use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use recolith_backend::{KeyRange, ScanOptions, Snapshot, StorageBackend};
use recolith_catalog::Catalog;
use recolith_common::{BucketId, FieldId, FragmentId, IndexId, PartitionId};
use recolith_model::{Collection, DocumentKeySchema, Fragment, FragmentCardinality};
use recolith_storage::{
    DocumentFragments, FragmentData, FragmentMode, FragmentReader, IndexBound, IndexFilter,
    RecordData, StorageError, Value, decode_record_key, encode_bucket_prefix, encode_index_value,
    read_document, scan_index, scan_index_components, scan_index_range,
};

use crate::aggregate::{AggregateBuilder, AggregatePage, AggregateQuery};
use crate::compare::compare_values;
use crate::error::{QueryError, Result};
use crate::ir::{DocumentQuery, FieldSelection, Filter, FilterOp};
use crate::request::DocumentReadRequest;
use crate::scan::{DocumentScan, KeyFilter, ScanOrder};

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

    /// Returns the document key.
    #[must_use]
    pub fn document_key(&self) -> &[u8] {
        &self.document_key
    }

    /// Returns the selected fragment identifiers.
    #[must_use]
    pub fn fragment_ids(&self) -> Vec<FragmentId> {
        self.fragments
            .iter()
            .map(FragmentReader::fragment_id)
            .collect()
    }

    /// Returns fragment filters.
    #[must_use]
    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }

    /// Returns the projection.
    #[must_use]
    pub fn projection(&self) -> &[FieldSelection] {
        &self.projection
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
    order: Option<ScanOrder>,
    order_index: Option<IndexId>,
}

impl ScanPlan {
    /// Returns the index used as the ordering source, if any.
    #[must_use]
    pub const fn order_index(&self) -> Option<IndexId> {
        self.order_index
    }

    /// Returns whether candidate enumeration uses an index.
    #[must_use]
    pub fn uses_index_scan(&self) -> bool {
        self.drives
            .iter()
            .any(|drive| !drive.index_filters.is_empty())
    }

    /// Returns the selected fragment identifiers.
    #[must_use]
    pub fn fragment_ids(&self) -> Vec<FragmentId> {
        self.fragments
            .iter()
            .map(FragmentReader::fragment_id)
            .collect()
    }

    /// Returns fragment filters.
    #[must_use]
    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }

    /// Returns the projection.
    #[must_use]
    pub fn projection(&self) -> &[FieldSelection] {
        &self.projection
    }

    /// Returns document key filters.
    #[must_use]
    pub fn key_filters(&self) -> &[KeyFilter] {
        &self.key_filters
    }

    /// Returns the ordering, if any.
    #[must_use]
    pub fn order(&self) -> Option<&ScanOrder> {
        self.order.as_ref()
    }

    /// Returns the candidate document limit.
    #[must_use]
    pub const fn candidate_limit(&self) -> usize {
        MAX_SCAN_DOCUMENTS
    }

    /// Returns the candidate sources used by the plan.
    #[must_use]
    pub fn drives(&self) -> Vec<ScanDriveInfo> {
        self.drives
            .iter()
            .map(|drive| ScanDriveInfo {
                bucket: drive.bucket,
                fragment: drive.fragment,
                indexes: drive.index_filters.iter().map(IndexFilter::index).collect(),
            })
            .collect()
    }
}

/// Candidate source in a scan plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanDriveInfo {
    bucket: BucketId,
    fragment: FragmentId,
    indexes: Vec<IndexId>,
}

impl ScanDriveInfo {
    /// Returns the bucket identifier.
    #[must_use]
    pub const fn bucket(&self) -> BucketId {
        self.bucket
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }

    /// Returns index identifiers used by the candidate source.
    #[must_use]
    pub fn indexes(&self) -> &[IndexId] {
        &self.indexes
    }

    /// Returns whether the candidate source is a bucket scan.
    #[must_use]
    pub fn is_bucket_scan(&self) -> bool {
        self.indexes.is_empty()
    }
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
        let selected_len = selected.len();
        let selected_fragment = selected.first().map(|fragment| fragment.id());
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
            for filter in query
                .filters()
                .iter()
                .filter(|filter| filter.fragment() == fragment.id())
            {
                let Some(index) = leading_field_index(&catalog, bucket.id(), filter.field()) else {
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
        let order_index = order_index_source(
            &catalog,
            collection,
            query.order(),
            selected_len,
            selected_fragment,
            &drives,
        );

        for filter in query.filters() {
            validate_field(&schemas, filter.fragment(), filter.field())?;
        }
        for selection in query.projection() {
            validate_field(&schemas, selection.fragment(), selection.field())?;
        }
        if let Some(order) = query.order() {
            validate_field(&schemas, order.fragment(), order.field())?;
        }

        Ok(ScanPlan {
            key_schema: query.key_schema().clone(),
            key_filters: query.key_filters().to_vec(),
            fragments: readers,
            drives,
            filters: query.filters().to_vec(),
            projection: query.projection().to_vec(),
            order: query.order().cloned(),
            order_index,
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

    /// Executes one scan page after an optional position.
    ///
    /// Ordered scans read a single-field leading index when the plan is
    /// eligible; otherwise they sort every matching candidate in memory. Both
    /// paths then apply the cursor position and limit. Unordered scans keep
    /// document-key order and stop at the first document beyond the limit.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for backend or storage assembly failures.
    pub fn execute_scan_page(
        &self,
        plan: &ScanPlan,
        limit: usize,
        after: Option<ScanPosition<'_>>,
    ) -> Result<ScanPage> {
        if limit == 0 {
            return Ok(ScanPage {
                documents: Vec::new(),
                has_more: false,
                last_order_value: None,
            });
        }

        let snapshot = self.backend.snapshot()?;
        if let (Some(order), Some(index)) = (&plan.order, plan.order_index)
            && let Some(page) =
                execute_index_ordered_page(plan, &snapshot, order, index, limit, after)?
        {
            return Ok(page);
        }
        let candidates = collect_candidates(plan, &snapshot)?;

        if let Some(order) = &plan.order {
            return execute_ordered_page(plan, &snapshot, order, candidates, limit, after);
        }

        let after_key = after.map(|position| position.document_key);
        let mut documents = Vec::new();
        let mut has_more = false;
        for key in candidates {
            if after_key.is_some_and(|after| key.as_slice() <= after) {
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
            last_order_value: None,
        })
    }

    /// Executes an aggregate query over at most `limit` groups.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for backend, storage, group-limit, or aggregate
    /// overflow failures.
    pub fn execute_aggregate(&self, query: &AggregateQuery, limit: usize) -> Result<AggregatePage> {
        let scan = query.as_scan();
        let plan = self.plan_scan(&scan)?;
        let snapshot = self.backend.snapshot()?;
        let mut builder = AggregateBuilder::new(query.group().cloned(), query.aggregates());

        if let Some(document_key) = query.document_key() {
            if key_matches(&plan.key_schema, &plan.key_filters, document_key)? {
                let assembled = read_document(&snapshot, document_key, &plan.fragments)?;
                if let Some(fragments) = apply_scan_filters(assembled, &plan.filters, &[]) {
                    builder.add_document(&fragments)?;
                }
            }
        } else {
            for key in collect_candidates(&plan, &snapshot)? {
                if !key_matches(&plan.key_schema, &plan.key_filters, &key)? {
                    continue;
                }
                let assembled = read_document(&snapshot, &key, &plan.fragments)?;
                let Some(fragments) = apply_scan_filters(assembled, &plan.filters, &[]) else {
                    continue;
                };
                builder.add_document(&fragments)?;
            }
        }

        builder.finish(limit)
    }
}

/// One scan result page.
#[derive(Clone, Debug)]
pub struct ScanPage {
    /// Documents in document-key or ordered-value order.
    pub documents: Vec<(Vec<u8>, DocumentFragments)>,
    /// Whether another matching document exists after this page.
    pub has_more: bool,
    /// Encoded ordering value of the last document, if the scan is ordered.
    pub last_order_value: Option<Vec<u8>>,
}

/// Resume position for one scan page.
#[derive(Clone, Copy, Debug)]
pub struct ScanPosition<'a> {
    /// Document key of the last returned document.
    pub document_key: &'a [u8],
    /// Encoded ordering value of the last returned document.
    pub order_value: Option<&'a [u8]>,
}

struct OrderedDocument {
    key: Vec<u8>,
    order_value: Option<Vec<u8>>,
    fragments: DocumentFragments,
}

fn execute_index_ordered_page<S: Snapshot>(
    plan: &ScanPlan,
    snapshot: &S,
    order: &ScanOrder,
    index: IndexId,
    limit: usize,
    after: Option<ScanPosition<'_>>,
) -> Result<Option<ScanPage>> {
    let Some(drive) = plan.drives.first() else {
        return Ok(None);
    };
    let record_keys = scan_index_range(
        snapshot,
        drive.bucket,
        drive.partition,
        index,
        None,
        None,
        1,
        ScanOptions {
            limit: Some(MAX_SCAN_DOCUMENTS),
            reverse: order.descending(),
        },
    )?;

    let mut documents = Vec::new();
    let mut last_order_value = None;
    let mut has_more = false;
    let mut seen = BTreeSet::new();
    for record_key in record_keys {
        let Ok(decoded) = decode_record_key(&record_key) else {
            continue;
        };
        if decoded.head().partition() != drive.partition || decoded.fragment() != drive.fragment {
            continue;
        }
        let key = decoded.document_key().to_vec();
        if !seen.insert(key.clone()) {
            continue;
        }
        if !key_matches(&plan.key_schema, &plan.key_filters, &key)? {
            continue;
        }
        let assembled = read_document(snapshot, &key, &plan.fragments)?;
        let Some(order_value) = scan_order_value(&assembled, order)? else {
            return Ok(None);
        };
        if let Some(position) = after {
            let ordering = compare_ordered(
                Some(&order_value),
                &key,
                position.order_value,
                position.document_key,
                order.descending(),
            );
            if ordering != Ordering::Greater {
                continue;
            }
        }
        let Some(fragments) = apply_scan_filters(assembled, &plan.filters, &plan.projection) else {
            continue;
        };
        if documents.len() == limit {
            has_more = true;
            break;
        }
        last_order_value = Some(order_value);
        documents.push((key, fragments));
    }

    Ok(Some(ScanPage {
        documents,
        has_more,
        last_order_value,
    }))
}

fn execute_ordered_page<S: Snapshot>(
    plan: &ScanPlan,
    snapshot: &S,
    order: &ScanOrder,
    candidates: BTreeSet<Vec<u8>>,
    limit: usize,
    after: Option<ScanPosition<'_>>,
) -> Result<ScanPage> {
    let mut matched = Vec::new();
    for key in candidates {
        if !key_matches(&plan.key_schema, &plan.key_filters, &key)? {
            continue;
        }
        let assembled = read_document(snapshot, &key, &plan.fragments)?;
        let order_value = scan_order_value(&assembled, order)?;
        let Some(fragments) = apply_scan_filters(assembled, &plan.filters, &plan.projection) else {
            continue;
        };
        matched.push(OrderedDocument {
            key,
            order_value,
            fragments,
        });
    }

    matched.sort_by(|left, right| {
        compare_ordered(
            left.order_value.as_deref(),
            &left.key,
            right.order_value.as_deref(),
            &right.key,
            order.descending(),
        )
    });

    let start = after.map_or(0, |position| {
        matched.partition_point(|document| {
            compare_ordered(
                document.order_value.as_deref(),
                &document.key,
                position.order_value,
                position.document_key,
                order.descending(),
            ) != Ordering::Greater
        })
    });
    let end = start.saturating_add(limit).min(matched.len());
    let has_more = end < matched.len();
    let mut documents = Vec::with_capacity(end.saturating_sub(start));
    let mut last_order_value = None;
    for document in matched.drain(start..end) {
        last_order_value = document.order_value;
        documents.push((document.key, document.fragments));
    }

    Ok(ScanPage {
        documents,
        has_more,
        last_order_value,
    })
}

fn scan_order_value(assembled: &DocumentFragments, order: &ScanOrder) -> Result<Option<Vec<u8>>> {
    let Some(records) = assembled.get(&order.fragment()) else {
        return Ok(None);
    };
    let mut selected: Option<&Value> = None;
    for record in records {
        let Some(value) = record.data().get(order.field()) else {
            continue;
        };
        if selected.is_none_or(|current| compare_values(value, current) == Some(Ordering::Less)) {
            selected = Some(value);
        }
    }

    Ok(selected.map(encode_index_value).transpose()?)
}

fn compare_ordered(
    left_value: Option<&[u8]>,
    left_key: &[u8],
    right_value: Option<&[u8]>,
    right_key: &[u8],
    descending: bool,
) -> Ordering {
    let ordering = match (left_value, right_value) {
        (Some(left), Some(right)) => {
            let ordering = left.cmp(right);
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    };
    if ordering == Ordering::Equal {
        let keys = left_key.cmp(right_key);
        if descending { keys.reverse() } else { keys }
    } else {
        ordering
    }
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

fn order_index_source(
    catalog: &recolith_catalog::CatalogSnapshot,
    collection: &Collection,
    order: Option<&ScanOrder>,
    selected_len: usize,
    selected_fragment: Option<FragmentId>,
    drives: &[ScanDrive],
) -> Option<IndexId> {
    let order = order?;
    if selected_len != 1 || selected_fragment != Some(order.fragment()) {
        return None;
    }
    let drive = drives.first()?;
    if drives.len() != 1 || !drive.index_filters.is_empty() || drive.fragment != order.fragment() {
        return None;
    }
    let fragment = collection
        .fragments()
        .iter()
        .find(|fragment| fragment.id() == order.fragment())?;
    if fragment.cardinality() != FragmentCardinality::One {
        return None;
    }
    let bucket = catalog.buckets.get(&fragment.bucket_id())?;
    let mut versions = catalog
        .schemas
        .schemas
        .iter()
        .filter(|((schema_id, _), _)| *schema_id == bucket.schema_id())
        .peekable();
    versions.peek()?;
    for (_, schema) in versions {
        match schema.field(order.field()) {
            Some(field) if !field.optional() => {}
            _ => return None,
        }
    }
    catalog
        .indexes
        .get(&bucket.id())?
        .values()
        .find(|index| index.fields().len() == 1 && index.leading_field() == Some(order.field()))
        .map(|index| index.id())
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

fn project_record(record: FragmentData, fields: &BTreeSet<FieldId>) -> FragmentData {
    if fields.is_empty() {
        record
    } else {
        record.project(fields)
    }
}

fn collect_candidates<S: Snapshot>(plan: &ScanPlan, snapshot: &S) -> Result<BTreeSet<Vec<u8>>> {
    let mut candidates = BTreeSet::new();
    for drive in &plan.drives {
        if drive.index_filters.is_empty() {
            collect_bucket_candidates(snapshot, drive, &mut candidates)?;
        } else {
            collect_index_candidates(snapshot, drive, &mut candidates)?;
        }
        if candidates.len() >= MAX_SCAN_DOCUMENTS {
            break;
        }
    }

    Ok(candidates)
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
