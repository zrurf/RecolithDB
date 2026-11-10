//! TOML catalog definitions shared by server bootstrap and CLI apply.

use std::fs;
use std::path::{Path, PathBuf};

use recolith_common::{BucketId, KeyspaceId, SchemaId, SchemaVersion};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{SchemaError, parse_record};
use serde::Deserialize;
use thiserror::Error;

use crate::apply::CatalogChanges;
use crate::catalog::Catalog;
use crate::error::CatalogError;

/// Parsed catalog definition file.
#[derive(Clone, Debug, Default)]
pub struct CatalogDefinition {
    /// Keyspace definitions.
    pub keyspaces: Vec<Keyspace>,
    /// Schema definitions with their source IDL.
    pub schemas: Vec<SchemaDefinition>,
    /// Bucket definitions paired with their owning keyspace.
    pub buckets: Vec<(KeyspaceId, Bucket)>,
    /// Collection definitions.
    pub collections: Vec<Collection>,
    /// Index definitions paired with their bucket.
    pub indexes: Vec<(BucketId, IndexDefinition)>,
}

/// Schema definition carrying source IDL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaDefinition {
    /// Schema identifier.
    pub id: u64,
    /// Schema version.
    pub version: u32,
    /// Record IDL text.
    pub idl: String,
}

/// Catalog definition loading or validation failure.
#[derive(Debug, Error)]
pub enum DefinitionError {
    /// The definition file could not be read.
    #[error("failed to read catalog definition {path}: {source}")]
    Read {
        /// Definition path.
        path: PathBuf,
        /// I/O failure.
        source: std::io::Error,
    },
    /// The definition file is not valid TOML.
    #[error("invalid catalog definition: {0}")]
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
    /// The definition contains no metadata.
    #[error("catalog definition defines no metadata")]
    Empty,
}

impl CatalogDefinition {
    /// Loads a catalog definition file.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError`] for I/O or TOML failures.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, DefinitionError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| DefinitionError::Read {
            path: path.to_path_buf(),
            source,
        })?;

        Self::parse(&contents)
    }

    /// Parses a catalog definition document.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError`] for TOML or empty definitions.
    pub fn parse(contents: &str) -> Result<Self, DefinitionError> {
        let file: DefinitionFile = toml::from_str(contents)?;
        if file.keyspaces.is_empty()
            && file.schemas.is_empty()
            && file.buckets.is_empty()
            && file.collections.is_empty()
            && file.indexes.is_empty()
        {
            return Err(DefinitionError::Empty);
        }

        let keyspaces = file
            .keyspaces
            .into_iter()
            .map(|keyspace| Keyspace::new(KeyspaceId::new(keyspace.id), keyspace.name))
            .collect();
        let schemas = file
            .schemas
            .into_iter()
            .map(|schema| SchemaDefinition {
                id: schema.id,
                version: schema.version,
                idl: schema.idl,
            })
            .collect();
        let buckets = file
            .buckets
            .into_iter()
            .map(|bucket| {
                let keyspace_id = KeyspaceId::new(bucket.keyspace_id);
                (
                    keyspace_id,
                    Bucket::new(
                        BucketId::new(bucket.id),
                        bucket.name,
                        bucket.partitions,
                        bucket.policy.into(),
                        SchemaId::new(bucket.schema_id),
                        SchemaVersion::new(bucket.schema_version),
                    ),
                )
            })
            .collect();
        let collections = file
            .collections
            .into_iter()
            .map(|collection| {
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
                            recolith_common::FragmentId::new(fragment.id),
                            fragment.name,
                            BucketId::new(fragment.bucket_id),
                            fragment.cardinality.into(),
                        )
                    })
                    .collect();
                let affinity_group = collection
                    .affinity_group
                    .unwrap_or_else(|| collection.name.clone());
                Collection::new(
                    recolith_common::CollectionId::new(collection.id),
                    KeyspaceId::new(collection.keyspace_id),
                    collection.name,
                    affinity_group,
                    key_schema,
                    fragments,
                )
            })
            .collect();
        let indexes = file
            .indexes
            .into_iter()
            .map(|index| {
                (
                    BucketId::new(index.bucket_id),
                    IndexDefinition::new(
                        recolith_common::IndexId::new(index.id),
                        index.name,
                        index.kind.into(),
                        index
                            .fields
                            .into_iter()
                            .map(recolith_common::FieldId::new)
                            .collect(),
                    ),
                )
            })
            .collect();

        Ok(Self {
            keyspaces,
            schemas,
            buckets,
            collections,
            indexes,
        })
    }

    /// Parses schema IDL and converts the definition into catalog changes.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError`] for invalid schema IDL.
    pub fn into_changes(self) -> Result<CatalogChanges, DefinitionError> {
        let schemas = self
            .schemas
            .into_iter()
            .map(|schema| {
                parse_record(
                    &schema.idl,
                    SchemaId::new(schema.id),
                    SchemaVersion::new(schema.version),
                )
                .map_err(|source| DefinitionError::Schema {
                    schema_id: schema.id,
                    version: schema.version,
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(CatalogChanges {
            schemas,
            keyspaces: self.keyspaces,
            buckets: self.buckets,
            collections: self.collections,
            indexes: self.indexes,
        })
    }

    /// Builds a complete catalog from the definition.
    ///
    /// # Errors
    ///
    /// Returns [`DefinitionError`] for invalid schema IDL or inconsistent
    /// catalog references.
    pub fn into_catalog(self) -> Result<Catalog, DefinitionError> {
        let changes = self.into_changes()?;
        Catalog::new()
            .apply_changes(&changes)?
            .ok_or(DefinitionError::Empty)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DefinitionFile {
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
