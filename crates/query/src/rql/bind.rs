use std::collections::BTreeMap;
use std::fmt;

use recolith_catalog::CatalogSnapshot;
use recolith_common::{PartitionId, Timestamp};
use recolith_model::{Collection, Fragment, KeyField, KeyType, KeyValue};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{StorageError, Value, encode_document_key};

use super::ast::{
    RqlAggregate, RqlAggregateFunction, RqlAggregateQuery, RqlDocumentQuery, RqlLiteral, RqlOp,
    RqlOperand, RqlPath, RqlPredicate,
};
use crate::aggregate::{AggregateExpression, AggregateFunction, AggregateGroup, AggregateQuery};
use crate::ir::{DocumentQuery, FieldSelection, Filter, FilterOp};
use crate::scan::{BoundQuery, DocumentScan, KeyFilter, ScanOrder};

/// RQL binding error.
#[derive(Debug)]
pub enum RqlBindError {
    /// The collection name is unknown.
    UnknownCollection(String),
    /// The fragment name is not part of the collection.
    UnknownFragment(String),
    /// The fragment is not listed in `INCLUDE FRAGMENTS`.
    FragmentNotIncluded(String),
    /// The field name is unknown in the fragment schema.
    UnknownField { fragment: String, field: String },
    /// The document key field name is unknown.
    UnknownKeyField(String),
    /// A document key field has no value.
    MissingKeyField(String),
    /// A document key field is bound more than once.
    DuplicateKeyField(String),
    /// The query does not bind a single document.
    NotPointQuery,
    /// A bind parameter has no value.
    MissingParameter(String),
    /// A literal does not fit the target field type.
    InvalidLiteral { field: String, expected: String },
    /// A path has no fragment qualifier.
    UnqualifiedPath(String),
    /// The ordering clause is invalid.
    InvalidOrder(String),
    /// The ordered field has a type that cannot be sorted.
    UnsortableField(String),
    /// The aggregate expression is invalid.
    InvalidAggregate(String),
    /// The grouping field cannot be used for grouping.
    UnsortableGroup(String),
    /// The aggregate field type does not support the function.
    InvalidAggregateType { function: String, field: String },
    /// Catalog metadata is missing a bucket or schema.
    MissingMetadata(&'static str),
    /// Document key encoding failed.
    Key(StorageError),
}

impl fmt::Display for RqlBindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCollection(name) => write!(formatter, "unknown collection: {name}"),
            Self::UnknownFragment(name) => write!(formatter, "unknown fragment: {name}"),
            Self::FragmentNotIncluded(name) => {
                write!(formatter, "fragment is not included: {name}")
            }
            Self::UnknownField { fragment, field } => {
                write!(formatter, "unknown field {field} in fragment {fragment}")
            }
            Self::UnknownKeyField(name) => write!(formatter, "unknown document key field: {name}"),
            Self::MissingKeyField(name) => {
                write!(formatter, "missing document key field value: {name}")
            }
            Self::DuplicateKeyField(name) => {
                write!(formatter, "duplicate document key field: {name}")
            }
            Self::NotPointQuery => formatter.write_str("query does not bind a single document"),
            Self::MissingParameter(name) => write!(formatter, "missing bind parameter: {name}"),
            Self::InvalidLiteral { field, expected } => {
                write!(
                    formatter,
                    "invalid value for field {field}, expected {expected}"
                )
            }
            Self::UnqualifiedPath(path) => {
                write!(formatter, "path requires a fragment qualifier: {path}")
            }
            Self::InvalidOrder(message) => write!(formatter, "invalid ORDER BY: {message}"),
            Self::UnsortableField(name) => {
                write!(formatter, "field cannot be ordered: {name}")
            }
            Self::InvalidAggregate(message) => {
                write!(formatter, "invalid aggregate: {message}")
            }
            Self::UnsortableGroup(name) => {
                write!(formatter, "field cannot be grouped: {name}")
            }
            Self::InvalidAggregateType { function, field } => {
                write!(formatter, "field {field} does not support {function}")
            }
            Self::MissingMetadata(kind) => write!(formatter, "catalog is missing {kind}"),
            Self::Key(error) => write!(formatter, "document key error: {error}"),
        }
    }
}

impl std::error::Error for RqlBindError {}

impl From<StorageError> for RqlBindError {
    fn from(error: StorageError) -> Self {
        Self::Key(error)
    }
}

/// Values supplied for RQL bind parameters.
#[derive(Clone, Debug, Default)]
pub struct BindParameters {
    values: BTreeMap<String, RqlLiteral>,
}

impl BindParameters {
    /// Creates an empty parameter set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a parameter value.
    pub fn insert(&mut self, name: impl Into<String>, value: RqlLiteral) -> Option<RqlLiteral> {
        self.values.insert(name.into(), value)
    }

    /// Returns a parameter value.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RqlLiteral> {
        self.values.get(name)
    }
}

struct BoundFragment<'a> {
    fragment: &'a Fragment,
    schema: &'a RecordSchema,
}

/// Binds a parsed document query to catalog identifiers and a document key.
///
/// # Errors
///
/// Returns [`RqlBindError`] for unknown names, missing parameters, value type
/// mismatches, or an incomplete document key.
pub fn bind_query(
    catalog: &CatalogSnapshot,
    query: &RqlDocumentQuery,
    partition: PartitionId,
    parameters: &BindParameters,
) -> Result<BoundQuery, RqlBindError> {
    let collection = catalog
        .collection_by_name(query.collection())
        .ok_or_else(|| RqlBindError::UnknownCollection(query.collection().to_owned()))?;
    let fragments = bind_fragments(catalog, collection, query.fragments())?;
    let order = bind_order(&fragments, query)?;

    let mut selected = Vec::with_capacity(fragments.len());
    for fragment in &fragments {
        selected.push(fragment.fragment.id());
    }
    let mut projections = Vec::with_capacity(query.projections().len());
    let mut filters = Vec::new();
    for projection in query.projections() {
        let (bound, field) = resolve_path(&fragments, projection)?;
        projections.push(FieldSelection::new(bound.fragment.id(), field.id()));
        if let Some(selector) = projection.selector() {
            let selector_field = schema_field(bound, selector.field())?;
            let value = resolve_value(parameters, selector.operand(), selector_field)?;
            filters.push(Filter::new(
                bound.fragment.id(),
                selector_field.id(),
                filter_op(selector.op()),
                value,
            ));
        }
    }

    let mut predicates = bind_predicates(collection, &fragments, query.predicates(), parameters)?;
    filters.append(&mut predicates.filters);
    let key_filters = predicates.key_filters;
    let point_values = predicates.point_values;

    if predicates.point && point_values.iter().all(Option::is_some) {
        let mut ordered = Vec::with_capacity(point_values.len());
        for value in point_values {
            ordered.push(value.expect("point values are complete"));
        }
        let key = encode_document_key(collection.key_schema(), &ordered)?;
        let mut document_query = DocumentQuery::new(collection.id(), key, partition);
        for fragment in selected {
            document_query = document_query.with_fragment(fragment);
        }
        for projection in projections {
            document_query = document_query.with_projection(projection);
        }
        for filter in filters {
            document_query = document_query.with_filter(filter);
        }

        return Ok(BoundQuery::Document(document_query));
    }

    let mut scan = DocumentScan::new(collection.id(), partition, collection.key_schema().clone());
    for fragment in selected {
        scan = scan.with_fragment(fragment);
    }
    for projection in projections {
        scan = scan.with_projection(projection);
    }
    for filter in filters {
        scan = scan.with_filter(filter);
    }
    for filter in key_filters {
        scan = scan.with_key_filter(filter);
    }
    if let Some(order) = order {
        scan = scan.with_order(order);
    }

    Ok(BoundQuery::Scan(scan))
}

/// Binds a parsed aggregate query to catalog identifiers.
///
/// # Errors
///
/// Returns [`RqlBindError`] for unknown names, missing parameters, invalid
/// grouping fields, or unsupported aggregate types.
pub fn bind_aggregate_query(
    catalog: &CatalogSnapshot,
    query: &RqlAggregateQuery,
    partition: PartitionId,
    parameters: &BindParameters,
) -> Result<AggregateQuery, RqlBindError> {
    let collection = catalog
        .collection_by_name(query.collection())
        .ok_or_else(|| RqlBindError::UnknownCollection(query.collection().to_owned()))?;
    let fragments = bind_fragments(catalog, collection, query.fragments())?;
    let mut predicates = bind_predicates(collection, &fragments, query.predicates(), parameters)?;
    let group = query
        .group_by()
        .map(|path| bind_group(&fragments, path))
        .transpose()?;
    let mut aggregate_query =
        AggregateQuery::new(collection.id(), partition, collection.key_schema().clone());
    for fragment in &fragments {
        aggregate_query = aggregate_query.with_fragment(fragment.fragment.id());
    }
    for filter in predicates.filters.drain(..) {
        aggregate_query = aggregate_query.with_filter(filter);
    }
    for filter in predicates.key_filters {
        aggregate_query = aggregate_query.with_key_filter(filter);
    }
    if let Some(group) = group {
        aggregate_query = aggregate_query.with_group(group);
    }
    for aggregate in query.aggregates() {
        aggregate_query =
            aggregate_query.with_aggregate(bind_aggregate_expression(&fragments, aggregate)?);
    }
    if predicates.point && predicates.point_values.iter().all(Option::is_some) {
        let ordered = predicates
            .point_values
            .into_iter()
            .map(|value| value.expect("point values are complete"))
            .collect::<Vec<_>>();
        let key = encode_document_key(collection.key_schema(), &ordered)?;
        aggregate_query = aggregate_query.with_document_key(key);
    }

    Ok(aggregate_query)
}

/// Binds a query that must resolve to one document.
///
/// # Errors
///
/// Returns [`RqlBindError`] for binding failures or when the query is a scan.
pub fn bind_document_query(
    catalog: &CatalogSnapshot,
    query: &RqlDocumentQuery,
    partition: PartitionId,
    parameters: &BindParameters,
) -> Result<DocumentQuery, RqlBindError> {
    match bind_query(catalog, query, partition, parameters)? {
        BoundQuery::Document(query) => Ok(query),
        BoundQuery::Scan(_) => Err(RqlBindError::NotPointQuery),
    }
}

struct BoundPredicates {
    filters: Vec<Filter>,
    key_filters: Vec<KeyFilter>,
    point_values: Vec<Option<KeyValue>>,
    point: bool,
}

fn bind_predicates(
    collection: &Collection,
    fragments: &[BoundFragment<'_>],
    predicates: &[RqlPredicate],
    parameters: &BindParameters,
) -> Result<BoundPredicates, RqlBindError> {
    let mut filters = Vec::new();
    let mut key_filters = Vec::new();
    let mut point_values: Vec<Option<KeyValue>> =
        vec![None; collection.key_schema().fields().len()];
    let mut point = true;
    for predicate in predicates {
        if let Some(fragment) = predicate.path().fragment() {
            let bound = fragment_by_name(fragments, fragment)?;
            if let Some(selector) = predicate.path().selector() {
                let field = schema_field(bound, selector.field())?;
                let value = resolve_value(parameters, selector.operand(), field)?;
                filters.push(Filter::new(
                    bound.fragment.id(),
                    field.id(),
                    filter_op(selector.op()),
                    value,
                ));
            }
            let field = schema_field(bound, predicate.path().field())?;
            let value = resolve_value(parameters, predicate.operand(), field)?;
            filters.push(Filter::new(
                bound.fragment.id(),
                field.id(),
                filter_op(predicate.op()),
                value,
            ));
        } else {
            let key_field = collection
                .key_schema()
                .field(predicate.path().field())
                .ok_or_else(|| {
                    RqlBindError::UnknownKeyField(predicate.path().field().to_owned())
                })?;
            let index = collection
                .key_schema()
                .fields()
                .iter()
                .position(|field| field.name() == key_field.name())
                .expect("key field came from the schema");
            let op = filter_op(predicate.op());
            let value = resolve_key_value(parameters, predicate.operand(), key_field)?;
            if op == FilterOp::Eq && point_values[index].is_none() {
                point_values[index] = Some(value.clone());
            } else {
                point = false;
            }
            key_filters.push(KeyFilter::new(index, op, value));
        }
    }

    Ok(BoundPredicates {
        filters,
        key_filters,
        point_values,
        point,
    })
}

fn bind_group(
    fragments: &[BoundFragment<'_>],
    path: &RqlPath,
) -> Result<AggregateGroup, RqlBindError> {
    if path.selector().is_some() {
        return Err(RqlBindError::InvalidAggregate(
            "GROUP BY does not support selectors".to_owned(),
        ));
    }
    let name = path.fragment().ok_or_else(|| {
        RqlBindError::InvalidAggregate(format!(
            "GROUP BY requires a fragment qualifier: {}",
            path.field()
        ))
    })?;
    let fragment = fragment_by_name(fragments, name)?;
    let field = schema_field(fragment, path.field())?;
    if !sortable_type(field.ty()) {
        return Err(RqlBindError::UnsortableGroup(format!(
            "{}.{}",
            fragment.fragment.name(),
            field.name()
        )));
    }

    Ok(AggregateGroup::new(fragment.fragment.id(), field.id()))
}

fn bind_aggregate_expression(
    fragments: &[BoundFragment<'_>],
    aggregate: &RqlAggregate,
) -> Result<AggregateExpression, RqlBindError> {
    let function = match aggregate.function() {
        RqlAggregateFunction::Count => {
            if aggregate.path().is_some() {
                return Err(RqlBindError::InvalidAggregate(
                    "COUNT only supports COUNT(*)".to_owned(),
                ));
            }
            return Ok(AggregateExpression::new(
                AggregateFunction::Count,
                None,
                None,
                None,
            ));
        }
        RqlAggregateFunction::Sum => AggregateFunction::Sum,
        RqlAggregateFunction::Min => AggregateFunction::Min,
        RqlAggregateFunction::Max => AggregateFunction::Max,
        RqlAggregateFunction::Avg => AggregateFunction::Avg,
    };
    let path = aggregate.path().ok_or_else(|| {
        RqlBindError::InvalidAggregate("aggregate field path is missing".to_owned())
    })?;
    if path.selector().is_some() {
        return Err(RqlBindError::InvalidAggregate(
            "aggregate expressions do not support selectors".to_owned(),
        ));
    }
    let name = path.fragment().ok_or_else(|| {
        RqlBindError::InvalidAggregate(format!(
            "aggregate requires a fragment qualifier: {}",
            path.field()
        ))
    })?;
    let fragment = fragment_by_name(fragments, name)?;
    let field = schema_field(fragment, path.field())?;
    let supported = match function {
        AggregateFunction::Sum | AggregateFunction::Avg => numeric_type(field.ty()),
        AggregateFunction::Min | AggregateFunction::Max => comparable_type(field.ty()),
        AggregateFunction::Count => true,
    };
    if !supported {
        return Err(RqlBindError::InvalidAggregateType {
            function: function_name(function).to_owned(),
            field: format!("{}.{}", fragment.fragment.name(), field.name()),
        });
    }

    Ok(AggregateExpression::new(
        function,
        Some(fragment.fragment.id()),
        Some(field.id()),
        Some(field.ty().clone()),
    ))
}

const fn function_name(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
        AggregateFunction::Avg => "AVG",
    }
}

fn numeric_type(ty: &Type) -> bool {
    matches!(
        ty,
        Type::I32 | Type::I64 | Type::U32 | Type::U64 | Type::F32 | Type::F64
    )
}

fn comparable_type(ty: &Type) -> bool {
    sortable_type(ty) || matches!(ty, Type::F32 | Type::F64)
}

fn bind_fragments<'a>(
    catalog: &'a CatalogSnapshot,
    collection: &'a Collection,
    names: &[String],
) -> Result<Vec<BoundFragment<'a>>, RqlBindError> {
    let mut bound = Vec::new();
    for name in names {
        if bound
            .iter()
            .any(|fragment: &BoundFragment<'_>| fragment.fragment.name() == name)
        {
            continue;
        }
        let fragment = collection
            .fragments()
            .iter()
            .find(|fragment| fragment.name() == name)
            .ok_or_else(|| RqlBindError::UnknownFragment(name.clone()))?;
        bound.push(bound_fragment(catalog, fragment)?);
    }

    Ok(bound)
}

fn bound_fragment<'a>(
    catalog: &'a CatalogSnapshot,
    fragment: &'a Fragment,
) -> Result<BoundFragment<'a>, RqlBindError> {
    let bucket = catalog
        .buckets
        .get(&fragment.bucket_id())
        .ok_or(RqlBindError::MissingMetadata("bucket"))?;
    let schema = catalog
        .schemas
        .schemas
        .get(&(bucket.schema_id(), bucket.schema_version()))
        .ok_or(RqlBindError::MissingMetadata("schema"))?;

    Ok(BoundFragment { fragment, schema })
}

fn bind_order(
    fragments: &[BoundFragment<'_>],
    query: &RqlDocumentQuery,
) -> Result<Option<ScanOrder>, RqlBindError> {
    let Some(order) = query.order() else {
        return Ok(None);
    };
    let path = order.path();
    if path.selector().is_some() {
        return Err(RqlBindError::InvalidOrder(
            "selectors are not supported".to_owned(),
        ));
    }
    let name = path.fragment().ok_or_else(|| {
        RqlBindError::InvalidOrder(format!("{} requires a fragment qualifier", path.field()))
    })?;
    let fragment = fragment_by_name(fragments, name)?;
    let field = schema_field(fragment, path.field())?;
    if !sortable_type(field.ty()) {
        return Err(RqlBindError::UnsortableField(format!(
            "{}.{}",
            fragment.fragment.name(),
            field.name()
        )));
    }

    Ok(Some(ScanOrder::new(
        fragment.fragment.id(),
        field.id(),
        order.descending(),
    )))
}

fn sortable_type(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Bool
            | Type::I32
            | Type::I64
            | Type::U32
            | Type::U64
            | Type::String
            | Type::Bytes
            | Type::Timestamp
            | Type::Duration
            | Type::Enum(_)
    )
}

fn fragment_by_name<'a>(
    fragments: &'a [BoundFragment<'a>],
    name: &str,
) -> Result<&'a BoundFragment<'a>, RqlBindError> {
    fragments
        .iter()
        .find(|fragment| fragment.fragment.name() == name)
        .ok_or_else(|| RqlBindError::FragmentNotIncluded(name.to_owned()))
}

fn resolve_path<'a>(
    fragments: &'a [BoundFragment<'a>],
    path: &RqlPath,
) -> Result<(&'a BoundFragment<'a>, &'a Field), RqlBindError> {
    let name = path
        .fragment()
        .ok_or_else(|| RqlBindError::UnqualifiedPath(path.field().to_owned()))?;
    let fragment = fragment_by_name(fragments, name)?;
    let field = schema_field(fragment, path.field())?;

    Ok((fragment, field))
}

fn schema_field<'a>(
    fragment: &'a BoundFragment<'a>,
    name: &str,
) -> Result<&'a Field, RqlBindError> {
    fragment
        .schema
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .ok_or_else(|| RqlBindError::UnknownField {
            fragment: fragment.fragment.name().to_owned(),
            field: name.to_owned(),
        })
}

fn filter_op(op: RqlOp) -> FilterOp {
    match op {
        RqlOp::Eq => FilterOp::Eq,
        RqlOp::Ne => FilterOp::Ne,
        RqlOp::Lt => FilterOp::Lt,
        RqlOp::Le => FilterOp::Le,
        RqlOp::Gt => FilterOp::Gt,
        RqlOp::Ge => FilterOp::Ge,
    }
}

fn resolve_value(
    parameters: &BindParameters,
    operand: &RqlOperand,
    field: &Field,
) -> Result<Value, RqlBindError> {
    let literal = resolve_literal(parameters, operand)?;
    literal_to_value(field, literal)
}

fn resolve_key_value(
    parameters: &BindParameters,
    operand: &RqlOperand,
    field: &KeyField,
) -> Result<KeyValue, RqlBindError> {
    let literal = resolve_literal(parameters, operand)?;
    literal_to_key_value(field, literal)
}

fn resolve_literal<'a>(
    parameters: &'a BindParameters,
    operand: &'a RqlOperand,
) -> Result<&'a RqlLiteral, RqlBindError> {
    match operand {
        RqlOperand::Literal(literal) => Ok(literal),
        RqlOperand::Parameter(name) => parameters
            .get(name)
            .ok_or_else(|| RqlBindError::MissingParameter(name.clone())),
    }
}

fn literal_to_value(field: &Field, literal: &RqlLiteral) -> Result<Value, RqlBindError> {
    let value = match (field.ty(), literal) {
        (Type::Bool, RqlLiteral::Bool(value)) => Value::Bool(*value),
        (Type::I32, RqlLiteral::Integer(value)) => {
            Value::I32(i32::try_from(*value).map_err(|_| invalid_literal(field, "i32"))?)
        }
        (Type::I64, RqlLiteral::Integer(value)) => Value::I64(*value),
        (Type::U32, literal) => Value::U32(
            u32::try_from(unsigned(literal, field)?).map_err(|_| invalid_literal(field, "u32"))?,
        ),
        (Type::U64, literal) => Value::U64(unsigned(literal, field)?),
        (Type::F32, literal) => Value::F32(float(literal, field)? as f32),
        (Type::F64, literal) => Value::F64(float(literal, field)?),
        (Type::String, RqlLiteral::String(value)) => Value::String(value.clone()),
        (Type::Timestamp, literal) => {
            Value::Timestamp(Timestamp::from_micros(unsigned(literal, field)?))
        }
        (Type::Duration, literal) => Value::Duration(unsigned(literal, field)?),
        (Type::Enum(enum_schema), literal) => {
            let value = u32::try_from(unsigned(literal, field)?)
                .map_err(|_| invalid_literal(field, "enum variant"))?;
            if usize::try_from(value).map_or(true, |index| index >= enum_schema.variants().len()) {
                return Err(invalid_literal(field, "enum variant"));
            }
            Value::Enum(value)
        }
        (ty, _) => return Err(invalid_literal(field, &ty.to_string())),
    };

    Ok(value)
}

fn literal_to_key_value(field: &KeyField, literal: &RqlLiteral) -> Result<KeyValue, RqlBindError> {
    let value = match (field.ty(), literal) {
        (KeyType::U64, literal) => KeyValue::U64(unsigned_key(field, literal)?),
        (KeyType::I64, RqlLiteral::Integer(value)) => KeyValue::I64(*value),
        (KeyType::String, RqlLiteral::String(value)) => KeyValue::String(value.clone()),
        (ty, _) => return Err(invalid_key(field, ty)),
    };

    Ok(value)
}

fn unsigned(literal: &RqlLiteral, field: &Field) -> Result<u64, RqlBindError> {
    match literal {
        RqlLiteral::Unsigned(value) => Ok(*value),
        RqlLiteral::Integer(value) if *value >= 0 => Ok(*value as u64),
        _ => Err(invalid_literal(field, "unsigned integer")),
    }
}

fn unsigned_key(field: &KeyField, literal: &RqlLiteral) -> Result<u64, RqlBindError> {
    match literal {
        RqlLiteral::Unsigned(value) => Ok(*value),
        RqlLiteral::Integer(value) if *value >= 0 => Ok(*value as u64),
        _ => Err(invalid_key(field, KeyType::U64)),
    }
}

fn float(literal: &RqlLiteral, field: &Field) -> Result<f64, RqlBindError> {
    match literal {
        RqlLiteral::Float(value) => Ok(*value),
        RqlLiteral::Integer(value) => Ok(*value as f64),
        RqlLiteral::Unsigned(value) => Ok(*value as f64),
        _ => Err(invalid_literal(field, "float")),
    }
}

fn invalid_literal(field: &Field, expected: &str) -> RqlBindError {
    RqlBindError::InvalidLiteral {
        field: field.name().to_owned(),
        expected: expected.to_owned(),
    }
}

fn invalid_key(field: &KeyField, expected: KeyType) -> RqlBindError {
    RqlBindError::InvalidLiteral {
        field: field.name().to_owned(),
        expected: expected.to_string(),
    }
}
