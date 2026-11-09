use recolith_common::{CollectionId, FieldId, FragmentId, PartitionId};
use recolith_storage::Value;

use crate::request::DocumentReadRequest;

/// Predicate comparison operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterOp {
    /// Equality.
    Eq,
    /// Inequality.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Ge,
}

/// Filter scoped to one document fragment.
#[derive(Clone, Debug, PartialEq)]
pub struct Filter {
    fragment: FragmentId,
    field: FieldId,
    op: FilterOp,
    value: Value,
}

impl Filter {
    /// Creates a filter.
    #[must_use]
    pub fn new(fragment: FragmentId, field: FieldId, op: FilterOp, value: Value) -> Self {
        Self {
            fragment,
            field,
            op,
            value,
        }
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub fn fragment(&self) -> FragmentId {
        self.fragment
    }

    /// Returns the field identifier.
    #[must_use]
    pub fn field(&self) -> FieldId {
        self.field
    }

    /// Returns the comparison operator.
    #[must_use]
    pub fn op(&self) -> FilterOp {
        self.op
    }

    /// Returns the comparison value.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// Projected field scoped to one document fragment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldSelection {
    fragment: FragmentId,
    field: FieldId,
}

impl FieldSelection {
    /// Creates a field selection.
    #[must_use]
    pub const fn new(fragment: FragmentId, field: FieldId) -> Self {
        Self { fragment, field }
    }

    /// Returns the fragment identifier.
    #[must_use]
    pub const fn fragment(self) -> FragmentId {
        self.fragment
    }

    /// Returns the field identifier.
    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }
}

/// Document read query with optional filters and projection.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentQuery {
    collection_id: CollectionId,
    document_key: Vec<u8>,
    partition: PartitionId,
    fragments: Vec<FragmentId>,
    filters: Vec<Filter>,
    projection: Vec<FieldSelection>,
}

impl DocumentQuery {
    /// Creates a document query. An empty fragment list means all fragments.
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        document_key: impl Into<Vec<u8>>,
        partition: PartitionId,
    ) -> Self {
        Self {
            collection_id,
            document_key: document_key.into(),
            partition,
            fragments: Vec::new(),
            filters: Vec::new(),
            projection: Vec::new(),
        }
    }

    /// Adds a fragment to the selection.
    #[must_use]
    pub fn with_fragment(mut self, fragment: FragmentId) -> Self {
        self.fragments.push(fragment);
        self
    }

    /// Adds a filter.
    #[must_use]
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
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
    pub fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    /// Returns the document key.
    #[must_use]
    pub fn document_key(&self) -> &[u8] {
        &self.document_key
    }

    /// Returns the partition index.
    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the selected fragments.
    #[must_use]
    pub fn fragments(&self) -> &[FragmentId] {
        &self.fragments
    }

    /// Returns the filters.
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

impl From<&DocumentReadRequest> for DocumentQuery {
    fn from(request: &DocumentReadRequest) -> Self {
        Self {
            collection_id: request.collection_id(),
            document_key: request.document_key().to_vec(),
            partition: request.partition(),
            fragments: request.fragments().to_vec(),
            filters: Vec::new(),
            projection: Vec::new(),
        }
    }
}
