use std::fs;

use recolith_protocol::messages::{
    BucketDefinition, BucketPolicy, CatalogApplyRequest, CollectionDefinition, FragmentCardinality,
    FragmentDefinition, IndexDefinition, IndexKind, KeyFieldDefinition, KeyFieldType,
    KeyspaceDefinition, RequestMeta, SchemaDefinition,
};

use crate::args::CatalogCommand;
use crate::error::CliError;

pub(crate) fn request(command: CatalogCommand) -> Result<CatalogApplyRequest, CliError> {
    let mut request = CatalogApplyRequest::default();
    match command {
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

    Ok(request)
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
