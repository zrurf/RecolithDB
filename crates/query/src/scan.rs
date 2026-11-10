//! Multi-document scan queries.

use recolith_common::{CollectionId, FragmentId, PartitionId};
use recolith_model::{DocumentKeySchema, KeyValue};

use crate::ir::{DocumentQuery, FieldSelection, Filter, FilterOp};

/// Predicate over a document key component.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyFilter {
    field: usize,
    op: FilterOp,
    value: KeyValue,
}

impl KeyFilter {
    /// Creates a key filter for a key field position.
    #[must_use]
    pub const fn new(field: usize, op: FilterOp, value: KeyValue) -> Self {
        Self { field, op, value }
    }

    /// Returns the key field position.
    #[must_use]
    pub const fn field(&self) -> usize {
        self.field
    }

    /// Returns the comparison operator.
    #[must_use]
    pub const fn op(&self) -> FilterOp {
        self.op
    }

    /// Returns the comparison value.
    #[must_use]
    pub fn value(&self) -> &KeyValue {
        &self.value
    }
}

/// Multi-document scan query.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentScan {
    collection_id: CollectionId,
    partition: PartitionId,
    key_schema: DocumentKeySchema,
    fragments: Vec<FragmentId>,
    filters: Vec<Filter>,
    key_filters: Vec<KeyFilter>,
    projection: Vec<FieldSelection>,
}

impl DocumentScan {
    /// Creates a scan query. An empty fragment list means all fragments.
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        partition: PartitionId,
        key_schema: DocumentKeySchema,
    ) -> Self {
        Self {
            collection_id,
            partition,
            key_schema,
            fragments: Vec::new(),
            filters: Vec::new(),
            key_filters: Vec::new(),
            projection: Vec::new(),
        }
    }

    /// Adds a fragment to the selection.
    #[must_use]
    pub fn with_fragment(mut self, fragment: FragmentId) -> Self {
        self.fragments.push(fragment);
        self
    }

    /// Adds a fragment filter.
    #[must_use]
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Adds a document key filter.
    #[must_use]
    pub fn with_key_filter(mut self, filter: KeyFilter) -> Self {
        self.key_filters.push(filter);
        self
    }

    /// Adds a projected field.
    #[must_use]
    pub fn with_projection(mut self, selection: FieldSelection) -> Self {
        self.projection.push(selection);
        self
    }

    /// Returns the collection identifier.
    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Returns the partition index.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the document key schema.
    #[must_use]
    pub fn key_schema(&self) -> &DocumentKeySchema {
        &self.key_schema
    }

    /// Returns the selected fragments.
    #[must_use]
    pub fn fragments(&self) -> &[FragmentId] {
        &self.fragments
    }

    /// Returns fragment filters.
    #[must_use]
    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }

    /// Returns document key filters.
    #[must_use]
    pub fn key_filters(&self) -> &[KeyFilter] {
        &self.key_filters
    }

    /// Returns the projection.
    #[must_use]
    pub fn projection(&self) -> &[FieldSelection] {
        &self.projection
    }
}

/// Bound query produced by the RQL binder.
#[derive(Clone, Debug, PartialEq)]
pub enum BoundQuery {
    /// Point document read.
    Document(DocumentQuery),
    /// Multi-document scan.
    Scan(DocumentScan),
}

impl BoundQuery {
    /// Returns the collection identifier.
    #[must_use]
    pub fn collection_id(&self) -> CollectionId {
        match self {
            Self::Document(query) => query.collection_id(),
            Self::Scan(query) => query.collection_id(),
        }
    }
}
