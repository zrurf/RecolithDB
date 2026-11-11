//! Aggregate query plans and group accumulation.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use recolith_common::{CollectionId, FieldId, FragmentId, PartitionId};
use recolith_model::DocumentKeySchema;
use recolith_schema::Type;
use recolith_storage::{DocumentFragments, Value, encode_index_value};

use crate::compare::compare_values;
use crate::error::{QueryError, Result};
use crate::ir::Filter;
use crate::scan::{DocumentScan, KeyFilter};

/// Maximum number of groups one aggregate query may create.
pub const MAX_AGGREGATE_GROUPS: usize = 10_000;

/// Aggregate function applied to one fragment field or document set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateFunction {
    /// Counts matching documents.
    Count,
    /// Sums numeric values.
    Sum,
    /// Returns the minimum comparable value.
    Min,
    /// Returns the maximum comparable value.
    Max,
    /// Returns the arithmetic mean of numeric values.
    Avg,
}

/// Bound aggregate expression.
#[derive(Clone, Debug)]
pub struct AggregateExpression {
    function: AggregateFunction,
    fragment: Option<FragmentId>,
    field: Option<FieldId>,
    ty: Option<Type>,
}

impl AggregateExpression {
    /// Creates a bound aggregate expression.
    #[must_use]
    pub fn new(
        function: AggregateFunction,
        fragment: Option<FragmentId>,
        field: Option<FieldId>,
        ty: Option<Type>,
    ) -> Self {
        Self {
            function,
            fragment,
            field,
            ty,
        }
    }

    /// Returns the aggregate function.
    #[must_use]
    pub const fn function(&self) -> AggregateFunction {
        self.function
    }

    /// Returns the aggregated fragment; `COUNT(*)` has none.
    #[must_use]
    pub const fn fragment(&self) -> Option<FragmentId> {
        self.fragment
    }

    /// Returns the aggregated field; `COUNT(*)` has none.
    #[must_use]
    pub const fn field(&self) -> Option<FieldId> {
        self.field
    }

    /// Returns the bound field type.
    #[must_use]
    pub fn ty(&self) -> Option<&Type> {
        self.ty.as_ref()
    }
}

/// Bound grouping field.
#[derive(Clone, Debug)]
pub struct AggregateGroup {
    fragment: FragmentId,
    field: FieldId,
}

impl AggregateGroup {
    /// Creates a grouping field.
    #[must_use]
    pub const fn new(fragment: FragmentId, field: FieldId) -> Self {
        Self { fragment, field }
    }

    /// Returns the grouping fragment.
    #[must_use]
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }

    /// Returns the grouping field.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }
}

/// Bound aggregate query.
#[derive(Clone, Debug)]
pub struct AggregateQuery {
    collection_id: CollectionId,
    partition: PartitionId,
    key_schema: DocumentKeySchema,
    fragments: Vec<FragmentId>,
    filters: Vec<Filter>,
    key_filters: Vec<KeyFilter>,
    document_key: Option<Vec<u8>>,
    group: Option<AggregateGroup>,
    aggregates: Vec<AggregateExpression>,
}

impl AggregateQuery {
    /// Creates an aggregate query.
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
            document_key: None,
            group: None,
            aggregates: Vec::new(),
        }
    }

    /// Adds a selected fragment.
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

    /// Sets a fully bound document key.
    #[must_use]
    pub fn with_document_key(mut self, document_key: impl Into<Vec<u8>>) -> Self {
        self.document_key = Some(document_key.into());
        self
    }

    /// Sets the grouping field.
    #[must_use]
    pub fn with_group(mut self, group: AggregateGroup) -> Self {
        self.group = Some(group);
        self
    }

    /// Adds an aggregate expression.
    #[must_use]
    pub fn with_aggregate(mut self, aggregate: AggregateExpression) -> Self {
        self.aggregates.push(aggregate);
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

    /// Returns the selected fragments.
    #[must_use]
    pub fn fragments(&self) -> &[FragmentId] {
        &self.fragments
    }

    /// Returns the fully bound document key, if any.
    #[must_use]
    pub fn document_key(&self) -> Option<&[u8]> {
        self.document_key.as_deref()
    }

    /// Returns the grouping field.
    #[must_use]
    pub fn group(&self) -> Option<&AggregateGroup> {
        self.group.as_ref()
    }

    /// Returns the aggregate expressions.
    #[must_use]
    pub fn aggregates(&self) -> &[AggregateExpression] {
        &self.aggregates
    }

    /// Returns the equivalent scan used for planning and candidate enumeration.
    #[must_use]
    pub fn as_scan(&self) -> DocumentScan {
        let mut scan =
            DocumentScan::new(self.collection_id, self.partition, self.key_schema.clone());
        for fragment in &self.fragments {
            scan = scan.with_fragment(*fragment);
        }
        for filter in &self.filters {
            scan = scan.with_filter(filter.clone());
        }
        for filter in &self.key_filters {
            scan = scan.with_key_filter(filter.clone());
        }

        scan
    }
}

/// One aggregate result row.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateRow {
    group: Option<Value>,
    values: Vec<Option<Value>>,
}

impl AggregateRow {
    /// Creates a result row.
    #[must_use]
    pub fn new(group: Option<Value>, values: Vec<Option<Value>>) -> Self {
        Self { group, values }
    }

    /// Returns the group value.
    #[must_use]
    pub fn group(&self) -> Option<&Value> {
        self.group.as_ref()
    }

    /// Returns aggregate values in SELECT order.
    #[must_use]
    pub fn values(&self) -> &[Option<Value>] {
        &self.values
    }
}

/// Aggregate result page.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregatePage {
    rows: Vec<AggregateRow>,
    truncated: bool,
}

impl AggregatePage {
    /// Creates a result page.
    #[must_use]
    pub fn new(rows: Vec<AggregateRow>, truncated: bool) -> Self {
        Self { rows, truncated }
    }

    /// Returns result rows.
    #[must_use]
    pub fn rows(&self) -> &[AggregateRow] {
        &self.rows
    }

    /// Returns whether more groups exist beyond the requested limit.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// Accumulates groups for one aggregate query.
#[derive(Debug)]
pub struct AggregateBuilder {
    group: Option<AggregateGroup>,
    aggregates: Vec<AggregateExpression>,
    groups: BTreeMap<GroupKey, GroupState>,
}

impl AggregateBuilder {
    /// Creates a builder. A global query starts with one empty group.
    #[must_use]
    pub fn new(group: Option<AggregateGroup>, aggregates: &[AggregateExpression]) -> Self {
        let aggregates = aggregates.to_vec();
        let mut groups = BTreeMap::new();
        if group.is_none() {
            groups.insert(GroupKey(None), GroupState::new(None, aggregates.as_slice()));
        }
        Self {
            group,
            aggregates,
            groups,
        }
    }

    /// Adds one filtered document.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::ResourceExhausted`] when the group limit is
    /// exceeded or an aggregate value cannot be represented.
    pub fn add_document(&mut self, fragments: &DocumentFragments) -> Result<()> {
        let (group_value, key) = match &self.group {
            Some(group) => {
                let value = group_value(fragments, group);
                let key = match &value {
                    Some(value) => GroupKey(Some(encode_index_value(value)?)),
                    None => GroupKey(None),
                };
                (value, key)
            }
            None => (None, GroupKey(None)),
        };

        if !self.groups.contains_key(&key) {
            if self.groups.len() >= MAX_AGGREGATE_GROUPS {
                return Err(QueryError::ResourceExhausted(format!(
                    "aggregate group limit of {MAX_AGGREGATE_GROUPS} exceeded"
                )));
            }
            self.groups
                .insert(key.clone(), GroupState::new(group_value, &self.aggregates));
        }
        let state = self.groups.get_mut(&key).expect("group was inserted above");
        state.add(&self.aggregates, fragments)
    }

    /// Finishes accumulation and returns at most `limit` groups.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::AggregateOverflow`] when an integer sum exceeds
    /// the output range.
    pub fn finish(self, limit: usize) -> Result<AggregatePage> {
        let truncated = self.groups.len() > limit;
        let mut rows = Vec::with_capacity(limit.min(self.groups.len()));
        for (_, state) in self.groups.into_iter().take(limit) {
            let GroupState {
                group,
                documents,
                accumulators,
            } = state;
            let mut values = Vec::with_capacity(self.aggregates.len());
            for (expression, accumulator) in self.aggregates.iter().zip(accumulators) {
                let value = if expression.function() == AggregateFunction::Count {
                    Some(Value::U64(documents))
                } else {
                    accumulator.finish()?
                };
                values.push(value);
            }
            rows.push(AggregateRow::new(group, values));
        }

        Ok(AggregatePage::new(rows, truncated))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GroupKey(Option<Vec<u8>>);

impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0, &other.0) {
            (Some(left), Some(right)) => left.cmp(right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct GroupState {
    group: Option<Value>,
    documents: u64,
    accumulators: Vec<Accumulator>,
}

impl GroupState {
    fn new(group: Option<Value>, aggregates: &[AggregateExpression]) -> Self {
        Self {
            group,
            documents: 0,
            accumulators: aggregates.iter().map(Accumulator::new).collect(),
        }
    }

    fn add(
        &mut self,
        aggregates: &[AggregateExpression],
        fragments: &DocumentFragments,
    ) -> Result<()> {
        self.documents = self.documents.saturating_add(1);
        for (expression, accumulator) in aggregates.iter().zip(&mut self.accumulators) {
            if expression.function() == AggregateFunction::Count {
                continue;
            }
            let Some(field) = expression.field() else {
                continue;
            };
            let Some(fragment) = expression.fragment() else {
                continue;
            };
            let Some(records) = fragments.get(&fragment) else {
                continue;
            };
            for record in records {
                if let Some(value) = record.data().get(field) {
                    accumulator.update(value)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum Accumulator {
    Count,
    SignedSum { total: i128, seen: bool },
    UnsignedSum { total: u128, seen: bool },
    FloatSum { total: f64, seen: bool },
    Avg { total: f64, count: u64 },
    Min(Option<Value>),
    Max(Option<Value>),
    Empty,
}

impl Accumulator {
    fn new(expression: &AggregateExpression) -> Self {
        match expression.function() {
            AggregateFunction::Count => Self::Count,
            AggregateFunction::Sum => match expression.ty() {
                Some(Type::I32 | Type::I64) => Self::SignedSum {
                    total: 0,
                    seen: false,
                },
                Some(Type::U32 | Type::U64) => Self::UnsignedSum {
                    total: 0,
                    seen: false,
                },
                Some(Type::F32 | Type::F64) => Self::FloatSum {
                    total: 0.0,
                    seen: false,
                },
                _ => Self::Empty,
            },
            AggregateFunction::Avg => Self::Avg {
                total: 0.0,
                count: 0,
            },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
        }
    }

    fn update(&mut self, value: &Value) -> Result<()> {
        match self {
            Self::Count | Self::Empty => {}
            Self::SignedSum { total, seen } => {
                let value = match value {
                    Value::I32(value) => i128::from(*value),
                    Value::I64(value) => i128::from(*value),
                    _ => return Err(type_mismatch("signed integer")),
                };
                *total = total.saturating_add(value);
                *seen = true;
            }
            Self::UnsignedSum { total, seen } => {
                let value = match value {
                    Value::U32(value) => u128::from(*value),
                    Value::U64(value) => u128::from(*value),
                    _ => return Err(type_mismatch("unsigned integer")),
                };
                *total = total.saturating_add(value);
                *seen = true;
            }
            Self::FloatSum { total, seen } => {
                *total += numeric_float(value)?;
                *seen = true;
            }
            Self::Avg { total, count } => {
                *total += numeric_float(value)?;
                *count = count.saturating_add(1);
            }
            Self::Min(current) => {
                update_min(current, value);
            }
            Self::Max(current) => {
                update_max(current, value);
            }
        }

        Ok(())
    }

    fn finish(&self) -> Result<Option<Value>> {
        match self {
            Self::Count | Self::Empty => Ok(None),
            Self::SignedSum { total, seen } => {
                if !seen {
                    return Ok(None);
                }
                let value = i64::try_from(*total).map_err(|_| {
                    QueryError::AggregateOverflow("signed sum exceeds i64 range".to_owned())
                })?;
                Ok(Some(Value::I64(value)))
            }
            Self::UnsignedSum { total, seen } => {
                if !seen {
                    return Ok(None);
                }
                let value = u64::try_from(*total).map_err(|_| {
                    QueryError::AggregateOverflow("unsigned sum exceeds u64 range".to_owned())
                })?;
                Ok(Some(Value::U64(value)))
            }
            Self::FloatSum { total, seen } => Ok(seen.then_some(Value::F64(*total))),
            Self::Avg { total, count } => {
                if *count == 0 {
                    Ok(None)
                } else {
                    Ok(Some(Value::F64(*total / *count as f64)))
                }
            }
            Self::Min(value) | Self::Max(value) => Ok(value.clone()),
        }
    }
}

fn group_value(fragments: &DocumentFragments, group: &AggregateGroup) -> Option<Value> {
    let records = fragments.get(&group.fragment())?;
    let mut selected: Option<&Value> = None;
    for record in records {
        let Some(value) = record.data().get(group.field()) else {
            continue;
        };
        if selected.is_none_or(|current| compare_values(value, current) == Some(Ordering::Less)) {
            selected = Some(value);
        }
    }

    selected.cloned()
}

fn update_min(current: &mut Option<Value>, value: &Value) {
    if current
        .as_ref()
        .is_none_or(|current| compare_values(value, current) == Some(Ordering::Less))
    {
        *current = Some(value.clone());
    }
}

fn update_max(current: &mut Option<Value>, value: &Value) {
    if current
        .as_ref()
        .is_none_or(|current| compare_values(value, current) == Some(Ordering::Greater))
    {
        *current = Some(value.clone());
    }
}

fn numeric_float(value: &Value) -> Result<f64> {
    match value {
        Value::I32(value) => Ok(f64::from(*value)),
        Value::I64(value) => Ok(*value as f64),
        Value::U32(value) => Ok(f64::from(*value)),
        Value::U64(value) => Ok(*value as f64),
        Value::F32(value) => Ok(f64::from(*value)),
        Value::F64(value) => Ok(*value),
        _ => Err(type_mismatch("numeric")),
    }
}

fn type_mismatch(expected: &str) -> QueryError {
    QueryError::InvalidRequest(format!("aggregate field is not {expected}"))
}
