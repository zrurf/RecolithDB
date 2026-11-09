use std::fs;
use std::path::{Path, PathBuf};

use recolith_catalog::{Catalog, CatalogError};
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{SchemaError, parse_record};
use serde::Deserialize;
use thiserror::Error;

/// Catalog metadata used when the persistent catalog is empty.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapFile {
    #[serde(default)]
    keyspaces: Vec<KeyspaceConfig>,
    #[serde(default)]
    schemas: Vec<SchemaConfig>,
    #[serde(default)]
    buckets: Vec<BucketConfig>,
    #[serde(default)]
    collections: Vec<CollectionConfig>,
    #[serde(default)]
    indexes: Vec<IndexConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyspaceConfig {
    id: u32,
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaConfig {
    id: u64,
    version: u32,
    idl: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BucketConfig {
    id: u64,
    keyspace_id: u32,
    name: String,
    partitions: u32,
    #[serde(default)]
    policy: PolicyConfig,
    schema_id: u64,
    schema_version: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectionConfig {
    id: u64,
    keyspace_id: u32,
    name: String,
    affinity_group: Option<String>,
    key: Vec<KeyFieldConfig>,
    fragments: Vec<FragmentConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFieldConfig {
    name: String,
    #[serde(rename = "type")]
    ty: KeyTypeConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FragmentConfig {
    id: u32,
    name: String,
    bucket_id: u64,
    #[serde(default)]
    cardinality: CardinalityConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexConfig {
    bucket_id: u64,
    id: u64,
    name: String,
    #[serde(default)]
    kind: IndexKindConfig,
    fields: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PolicyConfig {
    #[default]
    Strict,
    Managed,
    Raw,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CardinalityConfig {
    #[default]
    One,
    Many,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum IndexKindConfig {
    #[default]
    Equality,
    Range,
    Composite,
    Unique,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum KeyTypeConfig {
    U64,
    I64,
    String,
    Bytes,
    Uuid,
}

/// Catalog bootstrap loading failure.
#[derive(Debug, Error)]
pub enum BootstrapError {
    /// The bootstrap file could not be read.
    #[error("failed to read catalog bootstrap {path}: {source}")]
    Read {
        /// Bootstrap path.
        path: PathBuf,
        /// I/O failure.
        source: std::io::Error,
    },
    /// The bootstrap file is not valid TOML.
    #[error("invalid catalog bootstrap: {0}")]
    Parse(#[from] toml::de::Error),
    /// A schema IDL is invalid.
    #[error("schema {schema_id} version {version}: {source}")]
    Schema {
        /// Schema identifier.
        schema_id: u64,
        /// Schema version.
        version: u32,
        /// Schema parser or validation failure.
        source: SchemaError,
    },
    /// Catalog metadata is inconsistent.
    #[error("invalid catalog metadata: {0}")]
    Catalog(#[from] CatalogError),
    /// The bootstrap file defines no metadata.
    #[error("catalog bootstrap defines no metadata")]
    Empty,
}

/// Loads a catalog bootstrap file.
///
/// # Errors
///
/// Returns [`BootstrapError`] for I/O, TOML, schema, or catalog validation failures.
pub fn load_bootstrap(path: impl AsRef<Path>) -> Result<Catalog, BootstrapError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|source| BootstrapError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    parse_bootstrap(&contents)
}

/// Builds a catalog from bootstrap TOML.
///
/// # Errors
///
/// Returns [`BootstrapError`] for TOML, schema, or catalog validation failures.
pub fn parse_bootstrap(contents: &str) -> Result<Catalog, BootstrapError> {
    let file: BootstrapFile = toml::from_str(contents)?;
    build(file)
}

fn build(file: BootstrapFile) -> Result<Catalog, BootstrapError> {
    if file.keyspaces.is_empty()
        && file.schemas.is_empty()
        && file.buckets.is_empty()
        && file.collections.is_empty()
        && file.indexes.is_empty()
    {
        return Err(BootstrapError::Empty);
    }

    let catalog = Catalog::new();
    let mut schemas = file.schemas;
    schemas.sort_by_key(|schema| (schema.id, schema.version));
    for schema in schemas {
        let record = parse_record(
            &schema.idl,
            SchemaId::new(schema.id),
            SchemaVersion::new(schema.version),
        )
        .map_err(|source| BootstrapError::Schema {
            schema_id: schema.id,
            version: schema.version,
            source,
        })?;
        catalog.register_schema(record)?;
    }

    for keyspace in file.keyspaces {
        catalog.create_keyspace(Keyspace::new(KeyspaceId::new(keyspace.id), keyspace.name))?;
    }

    for bucket in file.buckets {
        catalog.create_bucket(
            KeyspaceId::new(bucket.keyspace_id),
            Bucket::new(
                BucketId::new(bucket.id),
                bucket.name,
                bucket.partitions,
                bucket.policy.into(),
                SchemaId::new(bucket.schema_id),
                SchemaVersion::new(bucket.schema_version),
            ),
        )?;
    }

    for collection in file.collections {
        let key_schema = DocumentKeySchema::new(
            collection
                .key
                .into_iter()
                .map(|field| KeyField::new(field.name, field.ty.into()))
                .collect(),
        );
        let fragments = collection
            .fragments
            .into_iter()
            .map(|fragment| {
                Fragment::new(
                    FragmentId::new(fragment.id),
                    fragment.name,
                    BucketId::new(fragment.bucket_id),
                    fragment.cardinality.into(),
                )
            })
            .collect();
        let affinity_group = collection
            .affinity_group
            .unwrap_or_else(|| collection.name.clone());
        catalog.create_collection(Collection::new(
            CollectionId::new(collection.id),
            KeyspaceId::new(collection.keyspace_id),
            collection.name,
            affinity_group,
            key_schema,
            fragments,
        ))?;
    }

    for index in file.indexes {
        catalog.create_index(
            BucketId::new(index.bucket_id),
            IndexDefinition::new(
                IndexId::new(index.id),
                index.name,
                index.kind.into(),
                index.fields.into_iter().map(FieldId::new).collect(),
            ),
        )?;
    }

    Ok(catalog)
}

impl From<PolicyConfig> for BucketPolicy {
    fn from(policy: PolicyConfig) -> Self {
        match policy {
            PolicyConfig::Strict => Self::Strict,
            PolicyConfig::Managed => Self::Managed,
            PolicyConfig::Raw => Self::Raw,
        }
    }
}

impl From<CardinalityConfig> for FragmentCardinality {
    fn from(cardinality: CardinalityConfig) -> Self {
        match cardinality {
            CardinalityConfig::One => Self::One,
            CardinalityConfig::Many => Self::Many,
        }
    }
}

impl From<IndexKindConfig> for IndexKind {
    fn from(kind: IndexKindConfig) -> Self {
        match kind {
            IndexKindConfig::Equality => Self::Equality,
            IndexKindConfig::Range => Self::Range,
            IndexKindConfig::Composite => Self::Composite,
            IndexKindConfig::Unique => Self::Unique,
        }
    }
}

impl From<KeyTypeConfig> for KeyType {
    fn from(ty: KeyTypeConfig) -> Self {
        match ty {
            KeyTypeConfig::U64 => Self::U64,
            KeyTypeConfig::I64 => Self::I64,
            KeyTypeConfig::String => Self::String,
            KeyTypeConfig::Bytes => Self::Bytes,
            KeyTypeConfig::Uuid => Self::Uuid,
        }
    }
}
