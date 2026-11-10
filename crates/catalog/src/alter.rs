//! Catalog object alteration.

use recolith_common::{BucketId, CatalogGeneration, IndexId, SchemaVersion};
use recolith_model::{Bucket, IndexDefinition};

use crate::catalog::{Catalog, CatalogSnapshot, validate_index};
use crate::error::{CatalogError, Result};

/// One catalog object to alter.
#[derive(Clone, Debug)]
pub enum CatalogAlter {
    /// Switches a bucket's current schema version.
    BucketSchema {
        /// Bucket to update.
        bucket: BucketId,
        /// New schema version for the bucket's schema ID.
        version: SchemaVersion,
    },
    /// Replaces an index definition on a bucket.
    Index {
        /// Owning bucket.
        bucket: BucketId,
        /// Index to redefine.
        index: IndexId,
        /// New definition.
        definition: IndexDefinition,
    },
}

impl Catalog {
    /// Alters one catalog object and returns the updated catalog.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for missing targets, unknown schema versions,
    /// duplicate names, or indexes that are invalid for the new schema.
    pub fn alter_object(&self, alter: CatalogAlter) -> Result<Catalog> {
        let mut snapshot = self.snapshot()?;
        match alter {
            CatalogAlter::BucketSchema { bucket, version } => {
                switch_bucket_schema(&mut snapshot, bucket, version)?
            }
            CatalogAlter::Index {
                bucket,
                index,
                definition,
            } => redefine_index(&mut snapshot, bucket, index, definition)?,
        }
        snapshot.generation = CatalogGeneration::new(snapshot.generation.get().saturating_add(1));

        Self::restore(snapshot)
    }
}

fn switch_bucket_schema(
    snapshot: &mut CatalogSnapshot,
    bucket_id: BucketId,
    version: SchemaVersion,
) -> Result<()> {
    let bucket = snapshot
        .buckets
        .get(&bucket_id)
        .ok_or(CatalogError::NotFound("bucket"))?
        .clone();
    let schema = snapshot
        .schemas
        .schemas
        .get(&(bucket.schema_id(), version))
        .ok_or(CatalogError::NotFound("schema"))?;
    if let Some(indexes) = snapshot.indexes.get(&bucket_id) {
        for index in indexes.values() {
            validate_index(schema, index)?;
        }
    }
    snapshot.buckets.insert(
        bucket_id,
        Bucket::new(
            bucket.id(),
            bucket.name().to_owned(),
            bucket.partition_count(),
            bucket.policy(),
            bucket.schema_id(),
            version,
        ),
    );

    Ok(())
}

fn redefine_index(
    snapshot: &mut CatalogSnapshot,
    bucket_id: BucketId,
    index_id: IndexId,
    definition: IndexDefinition,
) -> Result<()> {
    let bucket = snapshot
        .buckets
        .get(&bucket_id)
        .ok_or(CatalogError::NotFound("bucket"))?;
    let indexes = snapshot
        .indexes
        .get_mut(&bucket_id)
        .ok_or(CatalogError::NotFound("index"))?;
    if !indexes.contains_key(&index_id) {
        return Err(CatalogError::NotFound("index"));
    }
    if indexes
        .values()
        .any(|index| index.id() != index_id && index.name() == definition.name())
    {
        return Err(CatalogError::Duplicate("index name"));
    }
    let schema = snapshot
        .schemas
        .schemas
        .get(&(bucket.schema_id(), bucket.schema_version()))
        .ok_or(CatalogError::NotFound("schema"))?;
    validate_index(schema, &definition)?;
    indexes.insert(index_id, definition);

    Ok(())
}
