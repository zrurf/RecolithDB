use std::collections::BTreeSet;

use recolith_common::FieldId;

use crate::error::{Result, SchemaError};
use crate::record::RecordSchema;
use crate::types::Type;

/// Validates a record schema and all nested types.
///
/// # Errors
///
/// Returns [`SchemaError`] when names, IDs, types, or enum variants are invalid.
pub fn validate_record(record: &RecordSchema) -> Result<()> {
    if record.name().is_empty() {
        return Err(SchemaError::EmptyName("record"));
    }
    if record.fields().is_empty() {
        return Err(SchemaError::EmptyRecord(record.name().to_owned()));
    }

    let mut field_ids = BTreeSet::new();
    let mut field_names = BTreeSet::new();

    for field in record.fields() {
        let id: FieldId = field.id();
        if id.get() == 0 {
            return Err(SchemaError::InvalidFieldId(id));
        }
        if !field_ids.insert(id) {
            return Err(SchemaError::DuplicateFieldId(id));
        }
        if field.name().is_empty() {
            return Err(SchemaError::EmptyName("field"));
        }
        if !field_names.insert(field.name().to_owned()) {
            return Err(SchemaError::DuplicateFieldName(field.name().to_owned()));
        }

        validate_type(field.ty())?;
    }

    Ok(())
}

fn validate_type(ty: &Type) -> Result<()> {
    match ty {
        Type::Bool
        | Type::I32
        | Type::I64
        | Type::U32
        | Type::U64
        | Type::F32
        | Type::F64
        | Type::String
        | Type::Bytes
        | Type::Timestamp
        | Type::Duration => Ok(()),
        Type::Enum(schema) => {
            if schema.name().is_empty() {
                return Err(SchemaError::EmptyName("enum"));
            }
            if schema.variants().is_empty() {
                return Err(SchemaError::EmptyEnum(schema.name().to_owned()));
            }

            let mut variants = BTreeSet::new();
            for variant in schema.variants() {
                if variant.is_empty() {
                    return Err(SchemaError::EmptyName("enum variant"));
                }
                if !variants.insert(variant.clone()) {
                    return Err(SchemaError::DuplicateEnumVariant(variant.clone()));
                }
            }

            Ok(())
        }
        Type::List(inner) => validate_type(inner),
        Type::Map(key, value) => {
            if !key.is_valid_map_key() {
                return Err(SchemaError::InvalidType(format!(
                    "map key must be scalar, got {key}"
                )));
            }
            validate_type(key)?;
            validate_type(value)
        }
        Type::Record(record) => validate_record(record),
    }
}
