use std::collections::BTreeMap;
use std::fmt;

use recolith_catalog::CatalogSnapshot;
use recolith_common::{PartitionId, Timestamp};
use recolith_model::{Collection, Fragment, KeyField, KeyType, KeyValue};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_storage::{StorageError, Value, encode_document_key};

use super::ast::{RqlDocumentQuery, RqlLiteral, RqlOp, RqlOperand, RqlPath};
use crate::ir::{DocumentQuery, FieldSelection, Filter, FilterOp};

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
    /// A bind parameter has no value.
    MissingParameter(String),
    /// A literal does not fit the target field type.
    InvalidLiteral { field: String, expected: String },
    /// A path has no fragment qualifier.
    UnqualifiedPath(String),
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
pub fn bind_document_query(
    catalog: &CatalogSnapshot,
    query: &RqlDocumentQuery,
    partition: PartitionId,
    parameters: &BindParameters,
) -> Result<DocumentQuery, RqlBindError> {
    let collection = catalog
        .collection_by_name(query.collection())
        .ok_or_else(|| RqlBindError::UnknownCollection(query.collection().to_owned()))?;
    let fragments = bind_fragments(catalog, collection, query.fragments())?;

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

    let mut key_values: Vec<Option<KeyValue>> = vec![None; collection.key_schema().fields().len()];
    for predicate in query.predicates() {
        if let Some(fragment) = predicate.path().fragment() {
            let bound = fragment_by_name(&fragments, fragment)?;
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
            if key_values[index].is_some() {
                return Err(RqlBindError::DuplicateKeyField(key_field.name().to_owned()));
            }
            key_values[index] = Some(resolve_key_value(
                parameters,
                predicate.operand(),
                key_field,
            )?);
        }
    }

    let mut ordered = Vec::with_capacity(key_values.len());
    for (field, value) in collection.key_schema().fields().iter().zip(key_values) {
        ordered.push(value.ok_or_else(|| RqlBindError::MissingKeyField(field.name().to_owned()))?);
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

    Ok(document_query)
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
