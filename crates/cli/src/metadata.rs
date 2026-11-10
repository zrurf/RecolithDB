use std::collections::BTreeMap;

use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_protocol::messages::{
    CollectionMetadata, DescribeCollectionRequest, FieldValue, FragmentCardinality,
    FragmentMetadata, FragmentPayload, KeyFieldType, SchemaField, SchemaScalarType, Value,
    describe_collection_request, schema_type, value,
};

use crate::command::parse_typed_value;
use crate::error::CliError;
use crate::hex;

pub(crate) fn describe_request(
    collection: &str,
    keyspace: Option<&str>,
) -> DescribeCollectionRequest {
    let target = collection.parse::<u64>().map_or_else(
        |_| describe_collection_request::Target::CollectionName(collection.to_owned()),
        describe_collection_request::Target::CollectionId,
    );

    DescribeCollectionRequest {
        meta: None,
        keyspace: keyspace.unwrap_or_default().to_owned(),
        target: Some(target),
    }
}

pub(crate) fn collection(
    mut response: recolith_protocol::messages::DescribeCollectionResponse,
) -> Result<CollectionMetadata, CliError> {
    response.collection.take().ok_or_else(|| {
        CliError::InvalidMetadata("server returned no collection metadata".to_owned())
    })
}

pub(crate) fn document_key(
    keys: &[String],
    key_hex: Option<&str>,
    metadata: &CollectionMetadata,
) -> Result<Vec<u8>, CliError> {
    if let Some(input) = key_hex {
        return key_bytes(input);
    }

    let mut supplied = BTreeMap::new();
    for specification in keys {
        let (name, value) = specification
            .split_once('=')
            .ok_or_else(|| invalid_key_specification(specification))?;
        let field = metadata
            .key_fields
            .iter()
            .find(|field| field.name == name)
            .ok_or_else(|| {
                CliError::InvalidMetadata(format!("key field `{name}` was not found"))
            })?;
        let value = key_value(field.r#type, value)?;
        if supplied.insert(name.to_owned(), value).is_some() {
            return Err(CliError::InvalidMetadata(format!(
                "duplicate key field `{name}`"
            )));
        }
    }

    if supplied.len() != metadata.key_fields.len() {
        let expected = metadata
            .key_fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(CliError::InvalidMetadata(format!(
            "document key requires: {expected}"
        )));
    }

    let schema = DocumentKeySchema::new(
        metadata
            .key_fields
            .iter()
            .map(|field| Ok(KeyField::new(field.name.clone(), key_type(field.r#type)?)))
            .collect::<Result<Vec<_>, CliError>>()?,
    );
    let values = metadata
        .key_fields
        .iter()
        .map(|field| {
            supplied.remove(&field.name).ok_or_else(|| {
                CliError::InvalidMetadata(format!("missing key field `{}`", field.name))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    schema
        .encode(&values)
        .map_err(|error| CliError::InvalidMetadata(error.to_string()))
}

pub(crate) fn fragments(
    fields: &[String],
    subkeys: &[String],
    metadata: &CollectionMetadata,
) -> Result<Vec<FragmentPayload>, CliError> {
    let mut grouped: BTreeMap<u32, Vec<FieldValue>> = BTreeMap::new();
    let mut seen = BTreeMap::<u32, Vec<u32>>::new();
    for specification in fields {
        let (fragment, field, kind) = resolve_field(specification, metadata)?;
        if seen.entry(fragment.id).or_default().contains(&field.id) {
            return Err(CliError::InvalidMetadata(format!(
                "duplicate field `{}` in fragment `{}`",
                field.name, fragment.name
            )));
        }
        seen.entry(fragment.id).or_default().push(field.id);
        grouped.entry(fragment.id).or_default().push(FieldValue {
            field_id: field.id,
            value: Some(Value { kind: Some(kind) }),
        });
    }

    let mut subkeys_by_fragment = BTreeMap::new();
    for specification in subkeys {
        let (target, raw) = specification
            .split_once('=')
            .ok_or_else(|| CliError::InvalidField(specification.to_owned()))?;
        let fragment = resolve_fragment(metadata, target)?;
        if fragment.cardinality != FragmentCardinality::Many as i32 {
            return Err(CliError::InvalidMetadata(format!(
                "fragment `{}` does not accept a subkey",
                fragment.name
            )));
        }
        let subkey = hex::decode(raw).ok_or_else(|| CliError::InvalidKey(raw.to_owned()))?;
        if subkey.is_empty() {
            return Err(CliError::InvalidMetadata(format!(
                "subkey for fragment `{}` must not be empty",
                fragment.name
            )));
        }
        if subkeys_by_fragment.insert(fragment.id, subkey).is_some() {
            return Err(CliError::InvalidMetadata(format!(
                "duplicate subkey for fragment `{}`",
                fragment.name
            )));
        }
    }

    let mut payloads = Vec::with_capacity(grouped.len());
    for (fragment_id, fields) in grouped {
        let fragment = metadata
            .fragments
            .iter()
            .find(|fragment| fragment.id == fragment_id)
            .ok_or_else(|| CliError::InvalidMetadata("fragment metadata is missing".to_owned()))?;
        let subkey = match FragmentCardinality::try_from(fragment.cardinality) {
            Ok(FragmentCardinality::Many) => {
                subkeys_by_fragment.remove(&fragment_id).ok_or_else(|| {
                    CliError::InvalidMetadata(format!(
                        "fragment `{}` requires a subkey",
                        fragment.name
                    ))
                })?
            }
            Ok(FragmentCardinality::One) => Vec::new(),
            Ok(FragmentCardinality::Unspecified) | Err(_) => {
                return Err(CliError::InvalidMetadata(format!(
                    "fragment `{}` has an unknown cardinality",
                    fragment.name
                )));
            }
        };
        payloads.push(FragmentPayload {
            fragment_id,
            subkey,
            fields,
            if_match: None,
        });
    }
    if let Some(fragment_id) = subkeys_by_fragment.keys().next() {
        return Err(CliError::InvalidMetadata(format!(
            "subkey for fragment {fragment_id} has no fields"
        )));
    }

    Ok(payloads)
}

pub(crate) fn fragment_ids(
    fragments: &[String],
    metadata: &CollectionMetadata,
) -> Result<Vec<u32>, CliError> {
    fragments
        .iter()
        .map(|target| resolve_fragment(metadata, target).map(|fragment| fragment.id))
        .collect()
}

fn resolve_field<'a>(
    specification: &str,
    metadata: &'a CollectionMetadata,
) -> Result<(&'a FragmentMetadata, &'a SchemaField, value::Kind), CliError> {
    let (target, specification_value) = specification
        .split_once('=')
        .ok_or_else(|| CliError::InvalidField(specification.to_owned()))?;
    let (fragment, field) = if let Some((fragment, field)) = target.split_once('.') {
        let fragment = metadata
            .fragments
            .iter()
            .find(|candidate| candidate.name == fragment)
            .ok_or_else(|| {
                CliError::InvalidMetadata(format!("fragment `{fragment}` was not found"))
            })?;
        let field = fragment
            .fields
            .iter()
            .find(|candidate| candidate.name == field)
            .ok_or_else(|| {
                CliError::InvalidMetadata(format!(
                    "field `{field}` was not found in fragment `{}`",
                    fragment.name
                ))
            })?;
        (fragment, field)
    } else if let Some((fragment, field)) = target.split_once(':') {
        let fragment = resolve_fragment(metadata, fragment)?;
        let field_id: u32 = field
            .parse()
            .map_err(|_| CliError::InvalidField(specification.to_owned()))?;
        let field = fragment
            .fields
            .iter()
            .find(|candidate| candidate.id == field_id)
            .ok_or_else(|| {
                CliError::InvalidMetadata(format!(
                    "field {field_id} was not found in fragment `{}`",
                    fragment.name
                ))
            })?;
        (fragment, field)
    } else {
        return Err(CliError::InvalidField(specification.to_owned()));
    };

    let kind = parse_typed_value(specification_value)?;
    check_scalar(&kind, field, fragment)?;

    Ok((fragment, field, kind))
}

fn check_scalar(
    kind: &value::Kind,
    field: &SchemaField,
    fragment: &FragmentMetadata,
) -> Result<(), CliError> {
    let Some(schema_type::Kind::Scalar(scalar)) = field
        .r#type
        .as_ref()
        .and_then(|schema_type| schema_type.kind.as_ref())
    else {
        return Err(CliError::InvalidMetadata(format!(
            "field `{}.{}` uses a type that the CLI cannot write",
            fragment.name, field.name
        )));
    };
    let Ok(scalar) = SchemaScalarType::try_from(*scalar) else {
        return Err(CliError::InvalidMetadata(format!(
            "field `{}.{}` has an unknown type",
            fragment.name, field.name
        )));
    };
    if !scalar_matches(scalar, kind) {
        return Err(CliError::InvalidMetadata(format!(
            "field `{}.{}` expects {}",
            fragment.name,
            field.name,
            scalar_name(scalar)
        )));
    }

    Ok(())
}

const fn scalar_matches(scalar: SchemaScalarType, kind: &value::Kind) -> bool {
    matches!(
        (scalar, kind),
        (SchemaScalarType::Bool, value::Kind::BoolValue(_))
            | (SchemaScalarType::I32, value::Kind::I32Value(_))
            | (SchemaScalarType::I64, value::Kind::I64Value(_))
            | (SchemaScalarType::U32, value::Kind::U32Value(_))
            | (SchemaScalarType::U64, value::Kind::U64Value(_))
            | (SchemaScalarType::F32, value::Kind::F32Value(_))
            | (SchemaScalarType::F64, value::Kind::F64Value(_))
            | (SchemaScalarType::String, value::Kind::StringValue(_))
            | (SchemaScalarType::Bytes, value::Kind::BytesValue(_))
            | (SchemaScalarType::Timestamp, value::Kind::TimestampMicros(_))
            | (SchemaScalarType::Duration, value::Kind::DurationMicros(_))
    )
}

const fn scalar_name(scalar: SchemaScalarType) -> &'static str {
    match scalar {
        SchemaScalarType::Bool => "bool",
        SchemaScalarType::I32 => "i32",
        SchemaScalarType::I64 => "i64",
        SchemaScalarType::U32 => "u32",
        SchemaScalarType::U64 => "u64",
        SchemaScalarType::F32 => "f32",
        SchemaScalarType::F64 => "f64",
        SchemaScalarType::String => "string",
        SchemaScalarType::Bytes => "bytes",
        SchemaScalarType::Timestamp => "timestamp",
        SchemaScalarType::Duration => "duration",
        SchemaScalarType::Unspecified => "unspecified",
    }
}

fn resolve_fragment<'a>(
    metadata: &'a CollectionMetadata,
    target: &str,
) -> Result<&'a FragmentMetadata, CliError> {
    metadata
        .fragments
        .iter()
        .find(|fragment| fragment.name == target)
        .or_else(|| {
            target
                .parse::<u32>()
                .ok()
                .and_then(|id| metadata.fragments.iter().find(|fragment| fragment.id == id))
        })
        .ok_or_else(|| CliError::InvalidMetadata(format!("fragment `{target}` was not found")))
}

fn key_value(field_type: i32, specification: &str) -> Result<KeyValue, CliError> {
    let (declared, raw) = specification
        .split_once(':')
        .ok_or_else(|| invalid_key_specification(specification))?;
    let declared = declared.trim().to_ascii_lowercase();
    let Ok(field_type) = KeyFieldType::try_from(field_type) else {
        return Err(CliError::InvalidMetadata(
            "key field has an unknown type".to_owned(),
        ));
    };

    match field_type {
        KeyFieldType::U64 => {
            require_type(&declared, "u64")?;
            raw.parse()
                .map(KeyValue::U64)
                .map_err(|_| CliError::InvalidValue {
                    kind: "u64",
                    value: raw.to_owned(),
                })
        }
        KeyFieldType::I64 => {
            require_type(&declared, "i64")?;
            raw.parse()
                .map(KeyValue::I64)
                .map_err(|_| CliError::InvalidValue {
                    kind: "i64",
                    value: raw.to_owned(),
                })
        }
        KeyFieldType::String => {
            if declared != "string" && declared != "str" {
                return Err(CliError::InvalidValue {
                    kind: "string",
                    value: declared,
                });
            }
            Ok(KeyValue::String(raw.to_owned()))
        }
        KeyFieldType::Bytes => {
            if declared != "bytes" && declared != "hex" {
                return Err(CliError::InvalidValue {
                    kind: "bytes",
                    value: declared,
                });
            }
            hex::decode(raw)
                .map(KeyValue::Bytes)
                .ok_or_else(|| CliError::InvalidValue {
                    kind: "bytes",
                    value: raw.to_owned(),
                })
        }
        KeyFieldType::Uuid => {
            require_type(&declared, "uuid")?;
            let bytes = hex::decode(raw).ok_or_else(|| CliError::InvalidValue {
                kind: "uuid",
                value: raw.to_owned(),
            })?;
            let bytes: [u8; 16] = bytes.try_into().map_err(|_| CliError::InvalidValue {
                kind: "uuid",
                value: raw.to_owned(),
            })?;
            Ok(KeyValue::Uuid(bytes))
        }
        KeyFieldType::Unspecified => Err(CliError::InvalidMetadata(
            "key field has an unspecified type".to_owned(),
        )),
    }
}

fn key_type(field_type: i32) -> Result<KeyType, CliError> {
    match KeyFieldType::try_from(field_type) {
        Ok(KeyFieldType::U64) => Ok(KeyType::U64),
        Ok(KeyFieldType::I64) => Ok(KeyType::I64),
        Ok(KeyFieldType::String) => Ok(KeyType::String),
        Ok(KeyFieldType::Bytes) => Ok(KeyType::Bytes),
        Ok(KeyFieldType::Uuid) => Ok(KeyType::Uuid),
        Ok(KeyFieldType::Unspecified) | Err(_) => Err(CliError::InvalidMetadata(
            "key field has an unknown type".to_owned(),
        )),
    }
}

fn require_type(declared: &str, expected: &'static str) -> Result<(), CliError> {
    if declared == expected {
        return Ok(());
    }

    Err(CliError::InvalidMetadata(format!(
        "key field expects type `{expected}`, got `{declared}`"
    )))
}

fn key_bytes(input: &str) -> Result<Vec<u8>, CliError> {
    let key = hex::decode(input).ok_or_else(|| CliError::InvalidKey(input.to_owned()))?;
    if key.is_empty() {
        return Err(CliError::InvalidKey(input.to_owned()));
    }

    Ok(key)
}

fn invalid_key_specification(specification: &str) -> CliError {
    CliError::InvalidMetadata(format!(
        "invalid key `{specification}`; expected name=type:value"
    ))
}
