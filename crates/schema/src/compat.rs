use std::collections::BTreeMap;

use crate::error::Result;
use crate::field::Field;
use crate::record::RecordSchema;
use crate::types::Type;
use crate::validate::validate_record;

/// Schema change classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compatibility {
    /// Existing records remain readable and writable without a rewrite.
    Compatible,
    /// A new layout and data migration are required.
    RequiresMigration,
}

/// Compares two versions of a record schema.
///
/// # Errors
///
/// Returns [`crate::SchemaError`] if either schema is invalid.
pub fn check_compatibility(old: &RecordSchema, new: &RecordSchema) -> Result<Compatibility> {
    validate_record(old)?;
    validate_record(new)?;

    if old.id() != new.id() {
        return Ok(Compatibility::RequiresMigration);
    }

    let new_fields: BTreeMap<_, _> = new
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect();

    for old_field in old.fields() {
        let Some(new_field) = new_fields.get(&old_field.id()) else {
            return Ok(Compatibility::RequiresMigration);
        };

        if !types_compatible(old_field.ty(), new_field.ty()) {
            return Ok(Compatibility::RequiresMigration);
        }
        if old_field.optional() && !new_field.optional() {
            return Ok(Compatibility::RequiresMigration);
        }
    }

    let old_fields: BTreeMap<_, _> = old
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect();

    for new_field in new.fields() {
        if !old_fields.contains_key(&new_field.id()) && !new_field.optional() {
            return Ok(Compatibility::RequiresMigration);
        }
    }

    Ok(Compatibility::Compatible)
}

fn types_compatible(old: &Type, new: &Type) -> bool {
    if old == new {
        return true;
    }

    match (old, new) {
        (Type::Enum(old_enum), Type::Enum(new_enum)) => {
            old_enum.name() == new_enum.name()
                && new_enum.variants().starts_with(old_enum.variants())
        }
        (Type::List(old_inner), Type::List(new_inner)) => types_compatible(old_inner, new_inner),
        (Type::Map(old_key, old_value), Type::Map(new_key, new_value)) => {
            old_key == new_key && types_compatible(old_value, new_value)
        }
        (Type::Record(old_record), Type::Record(new_record)) => {
            records_compatible(old_record, new_record)
        }
        _ => false,
    }
}

fn records_compatible(old: &RecordSchema, new: &RecordSchema) -> bool {
    if old.fields().len() > new.fields().len() {
        return false;
    }

    let new_fields: BTreeMap<_, _> = new
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect();
    let old_fields: BTreeMap<_, _> = old
        .fields()
        .iter()
        .map(|field| (field.id(), field))
        .collect();

    old.fields().iter().all(|field| {
        new_fields
            .get(&field.id())
            .is_some_and(|new_field| field_compatible(field, new_field))
    }) && new
        .fields()
        .iter()
        .filter(|field| !old_fields.contains_key(&field.id()))
        .all(|field| field.optional())
}

fn field_compatible(old: &Field, new: &Field) -> bool {
    types_compatible(old.ty(), new.ty()) && !(old.optional() && !new.optional())
}
