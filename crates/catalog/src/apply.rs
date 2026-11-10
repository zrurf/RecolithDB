//! Additive catalog definition application.

use recolith_common::{BucketId, KeyspaceId};
use recolith_model::{Bucket, Collection, IndexDefinition, Keyspace};
use recolith_schema::RecordSchema;

use crate::catalog::Catalog;
use crate::error::{CatalogError, Result};

/// Additive catalog definitions applied as one validated batch.
///
/// Existing objects with identical definitions are accepted. The same
/// identifier with a different definition is rejected.
#[derive(Clone, Debug, Default)]
pub struct CatalogChanges {
    /// Schema versions to register.
    pub schemas: Vec<RecordSchema>,
    /// Keyspaces to create.
    pub keyspaces: Vec<Keyspace>,
    /// Buckets to create, paired with their owning keyspace.
    pub buckets: Vec<(KeyspaceId, Bucket)>,
    /// Collections to create.
    pub collections: Vec<Collection>,
    /// Indexes to create, paired with their bucket.
    pub indexes: Vec<(BucketId, IndexDefinition)>,
}

impl Catalog {
    /// Applies additive definitions and returns the updated catalog.
    ///
    /// Returns `None` when every definition already exists identically, so
    /// callers can skip persistence and generation changes.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for conflicting definitions, missing references,
    /// invalid schemas, or invalid indexes.
    pub fn apply_changes(&self, changes: &CatalogChanges) -> Result<Option<Catalog>> {
        let snapshot = self.snapshot()?;
        let candidate = Self::restore(snapshot)?;
        let mut changed = false;
        let mut schemas = changes.schemas.clone();
        schemas.sort_by_key(|schema| (schema.id(), schema.version()));

        for schema in &schemas {
            let current = candidate.snapshot()?;
            if let Some(existing) = current
                .schemas
                .schemas
                .get(&(schema.id(), schema.version()))
            {
                if existing == schema {
                    continue;
                }
                return Err(conflict(format!(
                    "schema {} version {}",
                    schema.id(),
                    schema.version()
                )));
            }
            candidate.register_schema(schema.clone())?;
            changed = true;
        }

        for keyspace in &changes.keyspaces {
            let current = candidate.snapshot()?;
            if let Some(existing) = current.keyspaces.get(&keyspace.id()) {
                if existing == keyspace {
                    continue;
                }
                return Err(conflict(format!("keyspace {}", keyspace.id())));
            }
            candidate.create_keyspace(keyspace.clone())?;
            changed = true;
        }

        for (keyspace_id, bucket) in &changes.buckets {
            let current = candidate.snapshot()?;
            if let Some(existing) = current.buckets.get(&bucket.id()) {
                if existing == bucket
                    && current.bucket_keyspaces.get(&bucket.id()) == Some(keyspace_id)
                {
                    continue;
                }
                return Err(conflict(format!("bucket {}", bucket.id())));
            }
            candidate.create_bucket(*keyspace_id, bucket.clone())?;
            changed = true;
        }

        for collection in &changes.collections {
            let current = candidate.snapshot()?;
            if let Some(existing) = current.collections.get(&collection.id()) {
                if existing == collection {
                    continue;
                }
                return Err(conflict(format!("collection {}", collection.id())));
            }
            candidate.create_collection(collection.clone())?;
            changed = true;
        }

        for (bucket_id, index) in &changes.indexes {
            let current = candidate.snapshot()?;
            if let Some(existing) = current
                .indexes
                .get(bucket_id)
                .and_then(|indexes| indexes.get(&index.id()))
            {
                if existing == index {
                    continue;
                }
                return Err(conflict(format!("index {}", index.id())));
            }
            candidate.create_index(*bucket_id, index.clone())?;
            changed = true;
        }

        Ok(changed.then_some(candidate))
    }
}

fn conflict(object: String) -> CatalogError {
    CatalogError::Conflict(format!(
        "{object} already exists with a different definition"
    ))
}
