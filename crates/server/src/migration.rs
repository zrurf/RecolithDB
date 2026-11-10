//! Declarative record migration for compatible and incompatible schema changes.

use std::collections::BTreeMap;

use recolith_common::FieldId;
use recolith_schema::{RecordSchema, Type};
use recolith_storage::{RecordData, Value};
use thiserror::Error;

/// Source of a migrated field value.
#[derive(Clone, Debug)]
pub enum MigrationSource {
    /// Copies a field from the old record.
    Field(FieldId),
    /// Uses a constant value.
    Default(Value),
}

/// Supported field conversions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationConversion {
    /// No conversion; source and target types must match.
    None,
    /// Converts `i32` to `i64`.
    I32ToI64,
    /// Converts `u32` to `u64`.
    U32ToU64,
    /// Converts `u32` to `i64`.
    U32ToI64,
}

/// One target field mapping.
#[derive(Clone, Debug)]
pub struct FieldMapping {
    /// Target field in the new schema.
    pub to: FieldId,
    /// Value source.
    pub source: MigrationSource,
    /// Conversion applied to the source value.
    pub conversion: MigrationConversion,
}

/// Declarative migration applied to every record in a bucket.
#[derive(Clone, Debug, Default)]
pub struct SchemaMigration {
    /// Field mappings. Fields without a mapping are copied by ID when present.
    pub mappings: Vec<FieldMapping>,
}

/// Record migration failure.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// A mapping targets a field that is not in the new schema.
    #[error("field {0} is not in the target schema")]
    UnknownTarget(FieldId),
    /// A target field is mapped more than once.
    #[error("field {0} is mapped more than once")]
    DuplicateTarget(FieldId),
    /// A mapped source field is missing from the old record.
    #[error("source field {0} is missing")]
    MissingSource(FieldId),
    /// A required target field has no value.
    #[error("required field {0} has no value")]
    MissingRequired(FieldId),
    /// A value does not match the target field type.
    #[error("field {0} expects {1}")]
    TypeMismatch(FieldId, &'static str),
    /// A conversion cannot be applied to the source value.
    #[error("conversion {0:?} cannot be applied to field {1}")]
    InvalidConversion(MigrationConversion, FieldId),
}

/// Applies a migration to one record.
///
/// # Errors
///
/// Returns [`MigrationError`] for invalid mappings, missing required fields, or
/// type mismatches.
pub fn migrate_record(
    old: &RecordData,
    new_schema: &RecordSchema,
    migration: &SchemaMigration,
) -> Result<RecordData, MigrationError> {
    let mut mappings = BTreeMap::new();
    for mapping in &migration.mappings {
        if new_schema.field(mapping.to).is_none() {
            return Err(MigrationError::UnknownTarget(mapping.to));
        }
        if mappings.insert(mapping.to, mapping).is_some() {
            return Err(MigrationError::DuplicateTarget(mapping.to));
        }
    }

    let mut output = RecordData::new();
    for field in new_schema.fields() {
        let id = field.id();
        let value = if let Some(mapping) = mappings.get(&id) {
            let value = match &mapping.source {
                MigrationSource::Field(from) => old
                    .get(*from)
                    .ok_or(MigrationError::MissingSource(*from))?
                    .clone(),
                MigrationSource::Default(value) => {
                    old.get(id).cloned().unwrap_or_else(|| value.clone())
                }
            };
            convert(value, mapping.conversion, id)?
        } else if let Some(value) = old.get(id) {
            value.clone()
        } else if field.optional() {
            continue;
        } else {
            return Err(MigrationError::MissingRequired(id));
        };
        ensure_type(&value, field.ty(), id)?;
        output.insert(id, value);
    }

    Ok(output)
}

fn convert(
    value: Value,
    conversion: MigrationConversion,
    field: FieldId,
) -> Result<Value, MigrationError> {
    match conversion {
        MigrationConversion::None => Ok(value),
        MigrationConversion::I32ToI64 => match value {
            Value::I32(value) => Ok(Value::I64(i64::from(value))),
            _ => Err(MigrationError::InvalidConversion(conversion, field)),
        },
        MigrationConversion::U32ToU64 => match value {
            Value::U32(value) => Ok(Value::U64(u64::from(value))),
            _ => Err(MigrationError::InvalidConversion(conversion, field)),
        },
        MigrationConversion::U32ToI64 => match value {
            Value::U32(value) => Ok(Value::I64(i64::from(value))),
            _ => Err(MigrationError::InvalidConversion(conversion, field)),
        },
    }
}

fn ensure_type(value: &Value, ty: &Type, field: FieldId) -> Result<(), MigrationError> {
    let matches = matches!(
        (value, ty),
        (Value::Bool(_), Type::Bool)
            | (Value::I32(_), Type::I32)
            | (Value::I64(_), Type::I64)
            | (Value::U32(_), Type::U32)
            | (Value::U64(_), Type::U64)
            | (Value::F32(_), Type::F32)
            | (Value::F64(_), Type::F64)
            | (Value::String(_), Type::String)
            | (Value::Bytes(_), Type::Bytes)
            | (Value::Timestamp(_), Type::Timestamp)
            | (Value::Duration(_), Type::Duration)
            | (Value::Enum(_), Type::Enum(_))
            | (Value::List(_), Type::List(_))
            | (Value::Map(_), Type::Map { .. })
            | (Value::Record(_), Type::Record(_))
    );
    if matches {
        Ok(())
    } else {
        Err(MigrationError::TypeMismatch(field, type_name(ty)))
    }
}

const fn type_name(ty: &Type) -> &'static str {
    match ty {
        Type::Bool => "bool",
        Type::I32 => "i32",
        Type::I64 => "i64",
        Type::U32 => "u32",
        Type::U64 => "u64",
        Type::F32 => "f32",
        Type::F64 => "f64",
        Type::String => "string",
        Type::Bytes => "bytes",
        Type::Timestamp => "timestamp",
        Type::Duration => "duration",
        Type::Enum(_) => "enum",
        Type::List(_) => "list",
        Type::Map { .. } => "map",
        Type::Record(_) => "record",
    }
}
