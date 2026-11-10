use std::fs;

use recolith_catalog::CatalogDefinition;
use recolith_model::{
    BucketPolicy as ModelBucketPolicy, FragmentCardinality as ModelCardinality,
    IndexKind as ModelIndexKind, KeyType as ModelKeyType,
};
use recolith_protocol::messages::{
    BucketDefinition, BucketPolicy, CatalogAlterRequest, CatalogApplyRequest, CatalogDropRequest,
    CatalogIndexTarget, CollectionDefinition, FieldConversion, FieldMapping, FragmentCardinality,
    FragmentDefinition, IndexDefinition, IndexKind, KeyFieldDefinition, KeyFieldType,
    KeyspaceDefinition, RedefineIndex, RequestMeta, SchemaDefinition, SchemaMigration,
    SetBucketSchema, Value, catalog_alter_request, catalog_drop_request, field_mapping,
};

use crate::args::CatalogCommand;
use crate::error::CliError;

pub(crate) enum CatalogRequest {
    Apply(CatalogApplyRequest),
    Drop(CatalogDropRequest),
    Alter(CatalogAlterRequest),
}

pub(crate) fn request(command: CatalogCommand) -> Result<CatalogRequest, CliError> {
    let mut request = CatalogApplyRequest::default();
    match command {
        CatalogCommand::AlterBucketSchema {
            bucket_id,
            schema_version,
            mappings,
            conversions,
            defaults,
            catalog_generation,
        } => {
            return Ok(CatalogRequest::Alter(CatalogAlterRequest {
                meta: meta(catalog_generation),
                change: Some(catalog_alter_request::Change::BucketSchema(
                    SetBucketSchema {
                        bucket_id,
                        schema_version,
                        migration: migration(&mappings, &conversions, &defaults)?,
                    },
                )),
            }));
        }
        CatalogCommand::AlterIndex {
            bucket_id,
            index_id,
            name,
            kind,
            fields,
            catalog_generation,
        } => {
            return Ok(CatalogRequest::Alter(CatalogAlterRequest {
                meta: meta(catalog_generation),
                change: Some(catalog_alter_request::Change::Index(RedefineIndex {
                    bucket_id,
                    index_id,
                    name,
                    kind: index_kind(&kind)? as i32,
                    fields,
                })),
            }));
        }
        CatalogCommand::Apply {
            file,
            catalog_generation,
        } => {
            let definition = CatalogDefinition::load(&file)
                .map_err(|error| CliError::InvalidCatalog(error.to_string()))?;
            return Ok(CatalogRequest::Apply(definition_request(
                definition,
                catalog_generation,
            )));
        }
        CatalogCommand::DropIndex {
            bucket_id,
            index_id,
            purge,
            catalog_generation,
        } => {
            return Ok(CatalogRequest::Drop(CatalogDropRequest {
                meta: meta(catalog_generation),
                purge,
                target: Some(catalog_drop_request::Target::Index(CatalogIndexTarget {
                    bucket_id,
                    index_id,
                })),
            }));
        }
        CatalogCommand::DropCollection {
            id,
            purge,
            catalog_generation,
        } => {
            return Ok(CatalogRequest::Drop(CatalogDropRequest {
                meta: meta(catalog_generation),
                purge,
                target: Some(catalog_drop_request::Target::CollectionId(id)),
            }));
        }
        CatalogCommand::DropBucket {
            id,
            purge,
            catalog_generation,
        } => {
            return Ok(CatalogRequest::Drop(CatalogDropRequest {
                meta: meta(catalog_generation),
                purge,
                target: Some(catalog_drop_request::Target::BucketId(id)),
            }));
        }
        CatalogCommand::DropKeyspace {
            id,
            purge,
            catalog_generation,
        } => {
            return Ok(CatalogRequest::Drop(CatalogDropRequest {
                meta: meta(catalog_generation),
                purge,
                target: Some(catalog_drop_request::Target::KeyspaceId(id)),
            }));
        }
        CatalogCommand::CreateKeyspace {
            id,
            name,
            catalog_generation,
        } => {
            request.keyspaces.push(KeyspaceDefinition { id, name });
            request.meta = meta(catalog_generation);
        }
        CatalogCommand::RegisterSchema {
            id,
            version,
            idl_file,
            catalog_generation,
        } => {
            let idl = fs::read_to_string(&idl_file)?;
            request.schemas.push(SchemaDefinition { id, version, idl });
            request.meta = meta(catalog_generation);
        }
        CatalogCommand::CreateBucket {
            id,
            keyspace_id,
            name,
            partitions,
            schema_id,
            schema_version,
            policy,
            catalog_generation,
        } => {
            request.buckets.push(BucketDefinition {
                id,
                keyspace_id,
                name,
                partitions,
                policy: parse_policy(&policy)? as i32,
                schema_id,
                schema_version,
            });
            request.meta = meta(catalog_generation);
        }
        CatalogCommand::CreateCollection {
            id,
            keyspace_id,
            name,
            affinity_group,
            key_fields,
            fragments,
            catalog_generation,
        } => {
            let affinity_group = affinity_group.unwrap_or_else(|| name.clone());
            request.collections.push(CollectionDefinition {
                id,
                keyspace_id,
                name,
                affinity_group,
                key_fields: key_fields
                    .iter()
                    .map(|field| key_field(field))
                    .collect::<Result<Vec<_>, _>>()?,
                fragments: fragments
                    .iter()
                    .map(|fragment| parse_fragment(fragment))
                    .collect::<Result<Vec<_>, _>>()?,
            });
            request.meta = meta(catalog_generation);
        }
        CatalogCommand::CreateIndex {
            bucket_id,
            id,
            name,
            kind,
            fields,
            catalog_generation,
        } => {
            request.indexes.push(IndexDefinition {
                id,
                bucket_id,
                name,
                kind: index_kind(&kind)? as i32,
                fields,
            });
            request.meta = meta(catalog_generation);
        }
    }

    Ok(CatalogRequest::Apply(request))
}

fn definition_request(definition: CatalogDefinition, generation: u64) -> CatalogApplyRequest {
    let mut request = CatalogApplyRequest {
        meta: meta(generation),
        ..Default::default()
    };
    for keyspace in definition.keyspaces {
        request.keyspaces.push(KeyspaceDefinition {
            id: keyspace.id().get(),
            name: keyspace.name().to_owned(),
        });
    }
    for schema in definition.schemas {
        request.schemas.push(SchemaDefinition {
            id: schema.id,
            version: schema.version,
            idl: schema.idl,
        });
    }
    for (keyspace_id, bucket) in definition.buckets {
        request.buckets.push(BucketDefinition {
            id: bucket.id().get(),
            keyspace_id: keyspace_id.get(),
            name: bucket.name().to_owned(),
            partitions: bucket.partition_count(),
            policy: model_policy(bucket.policy()) as i32,
            schema_id: bucket.schema_id().get(),
            schema_version: bucket.schema_version().get(),
        });
    }
    for collection in definition.collections {
        request.collections.push(CollectionDefinition {
            id: collection.id().get(),
            keyspace_id: collection.keyspace_id().get(),
            name: collection.name().to_owned(),
            affinity_group: collection.affinity_group().to_owned(),
            key_fields: collection
                .key_schema()
                .fields()
                .iter()
                .map(|field| KeyFieldDefinition {
                    name: field.name().to_owned(),
                    r#type: model_key_type(field.ty()) as i32,
                })
                .collect(),
            fragments: collection
                .fragments()
                .iter()
                .map(|fragment| FragmentDefinition {
                    id: fragment.id().get(),
                    name: fragment.name().to_owned(),
                    bucket_id: fragment.bucket_id().get(),
                    cardinality: model_cardinality(fragment.cardinality()) as i32,
                })
                .collect(),
        });
    }
    for (bucket_id, index) in definition.indexes {
        request.indexes.push(IndexDefinition {
            id: index.id().get(),
            bucket_id: bucket_id.get(),
            name: index.name().to_owned(),
            kind: model_index_kind(index.kind()) as i32,
            fields: index.fields().iter().map(|field| field.get()).collect(),
        });
    }

    request
}

fn model_policy(policy: ModelBucketPolicy) -> BucketPolicy {
    match policy {
        ModelBucketPolicy::Strict => BucketPolicy::Strict,
        ModelBucketPolicy::Managed => BucketPolicy::Managed,
        ModelBucketPolicy::Raw => BucketPolicy::Raw,
    }
}

fn model_key_type(ty: ModelKeyType) -> KeyFieldType {
    match ty {
        ModelKeyType::U64 => KeyFieldType::U64,
        ModelKeyType::I64 => KeyFieldType::I64,
        ModelKeyType::String => KeyFieldType::String,
        ModelKeyType::Bytes => KeyFieldType::Bytes,
        ModelKeyType::Uuid => KeyFieldType::Uuid,
    }
}

fn model_cardinality(cardinality: ModelCardinality) -> FragmentCardinality {
    match cardinality {
        ModelCardinality::One => FragmentCardinality::One,
        ModelCardinality::Many => FragmentCardinality::Many,
    }
}

fn model_index_kind(kind: ModelIndexKind) -> IndexKind {
    match kind {
        ModelIndexKind::Equality => IndexKind::Equality,
        ModelIndexKind::Range => IndexKind::Range,
        ModelIndexKind::Composite => IndexKind::Composite,
        ModelIndexKind::Unique => IndexKind::Unique,
    }
}

fn migration(
    mappings: &[String],
    conversions: &[String],
    defaults: &[String],
) -> Result<Option<SchemaMigration>, CliError> {
    let mut output = Vec::new();
    for mapping in mappings {
        let (to, from) = mapping
            .split_once('=')
            .ok_or_else(|| invalid_migration(mapping, "to=from"))?;
        output.push(FieldMapping {
            to_field: parse_u32(to, mapping)?,
            source: Some(field_mapping::Source::FromField(parse_u32(from, mapping)?)),
            conversion: FieldConversion::None as i32,
        });
    }
    for conversion in conversions {
        let (to, rest) = conversion
            .split_once('=')
            .ok_or_else(|| invalid_migration(conversion, "to=from:conversion"))?;
        let (from, kind) = rest
            .split_once(':')
            .ok_or_else(|| invalid_migration(conversion, "to=from:conversion"))?;
        output.push(FieldMapping {
            to_field: parse_u32(to, conversion)?,
            source: Some(field_mapping::Source::FromField(parse_u32(
                from, conversion,
            )?)),
            conversion: conversion_kind(kind)? as i32,
        });
    }
    for default in defaults {
        let (to, specification) = default
            .split_once('=')
            .ok_or_else(|| invalid_migration(default, "to=type:value"))?;
        let kind = crate::command::parse_typed_value(specification)?;
        output.push(FieldMapping {
            to_field: parse_u32(to, default)?,
            source: Some(field_mapping::Source::DefaultValue(Value {
                kind: Some(kind),
            })),
            conversion: FieldConversion::None as i32,
        });
    }

    Ok((!output.is_empty()).then_some(SchemaMigration { mappings: output }))
}

fn conversion_kind(value: &str) -> Result<FieldConversion, CliError> {
    match value {
        "none" => Ok(FieldConversion::None),
        "i32-i64" => Ok(FieldConversion::I32ToI64),
        "u32-u64" => Ok(FieldConversion::U32ToU64),
        "u32-i64" => Ok(FieldConversion::U32ToI64),
        _ => Err(CliError::InvalidCatalog(format!(
            "unknown conversion `{value}`; expected none, i32-i64, u32-u64, or u32-i64"
        ))),
    }
}

fn parse_u32(value: &str, original: &str) -> Result<u32, CliError> {
    value
        .parse()
        .map_err(|_| CliError::InvalidCatalog(format!("invalid migration `{original}`")))
}

fn invalid_migration(value: &str, expected: &str) -> CliError {
    CliError::InvalidCatalog(format!("invalid migration `{value}`; expected {expected}"))
}

fn meta(generation: u64) -> Option<RequestMeta> {
    (generation != 0).then(|| RequestMeta {
        catalog_generation: generation,
        ..Default::default()
    })
}

fn parse_policy(value: &str) -> Result<BucketPolicy, CliError> {
    match value {
        "strict" => Ok(BucketPolicy::Strict),
        "managed" => Ok(BucketPolicy::Managed),
        "raw" => Ok(BucketPolicy::Raw),
        _ => Err(CliError::InvalidCatalog(format!(
            "unknown bucket policy `{value}`; expected strict, managed, or raw"
        ))),
    }
}

fn index_kind(value: &str) -> Result<IndexKind, CliError> {
    match value {
        "equality" => Ok(IndexKind::Equality),
        "range" => Ok(IndexKind::Range),
        "composite" => Ok(IndexKind::Composite),
        "unique" => Ok(IndexKind::Unique),
        _ => Err(CliError::InvalidCatalog(format!(
            "unknown index kind `{value}`; expected equality, range, composite, or unique"
        ))),
    }
}

fn key_field(value: &str) -> Result<KeyFieldDefinition, CliError> {
    let (name, ty) = value.split_once(':').ok_or_else(|| {
        CliError::InvalidCatalog(format!("invalid key field `{value}`; expected name:type"))
    })?;
    if name.is_empty() {
        return Err(CliError::InvalidCatalog(format!(
            "invalid key field `{value}`; name is empty"
        )));
    }
    let ty = match ty {
        "u64" => KeyFieldType::U64,
        "i64" => KeyFieldType::I64,
        "string" => KeyFieldType::String,
        "bytes" => KeyFieldType::Bytes,
        "uuid" => KeyFieldType::Uuid,
        _ => {
            return Err(CliError::InvalidCatalog(format!(
                "invalid key field type `{ty}`"
            )));
        }
    };

    Ok(KeyFieldDefinition {
        name: name.to_owned(),
        r#type: ty as i32,
    })
}

fn parse_fragment(value: &str) -> Result<FragmentDefinition, CliError> {
    let mut parts = value.split(':');
    let id = parts
        .next()
        .ok_or_else(|| invalid_fragment(value))?
        .parse()
        .map_err(|_| invalid_fragment(value))?;
    let name = parts.next().ok_or_else(|| invalid_fragment(value))?;
    let bucket_id = parts
        .next()
        .ok_or_else(|| invalid_fragment(value))?
        .parse()
        .map_err(|_| invalid_fragment(value))?;
    let cardinality = match parts.next() {
        None | Some("one") => FragmentCardinality::One,
        Some("many") => FragmentCardinality::Many,
        Some(_) => return Err(invalid_fragment(value)),
    };
    if name.is_empty() || parts.next().is_some() {
        return Err(invalid_fragment(value));
    }

    Ok(FragmentDefinition {
        id,
        name: name.to_owned(),
        bucket_id,
        cardinality: cardinality as i32,
    })
}

fn invalid_fragment(value: &str) -> CliError {
    CliError::InvalidCatalog(format!(
        "invalid fragment `{value}`; expected id:name:bucket_id[:one|many]"
    ))
}
