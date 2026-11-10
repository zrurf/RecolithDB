//! Catalog object removal.

use recolith_common::{BucketId, CatalogGeneration, CollectionId, IndexId, KeyspaceId};

use crate::catalog::{Catalog, CatalogSnapshot};
use crate::error::{CatalogError, Result};

/// One catalog object to remove.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogDrop {
    /// Removes an index from a bucket.
    Index {
        /// Owning bucket.
        bucket: BucketId,
        /// Index to remove.
        index: IndexId,
    },
    /// Removes a collection.
    Collection(CollectionId),
    /// Removes an unreferenced bucket.
    Bucket(BucketId),
    /// Removes a keyspace and its collections, buckets, indexes, and placements.
    Keyspace(KeyspaceId),
}

impl Catalog {
    /// Removes one catalog object and returns the updated catalog.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for missing targets or objects that are still
    /// referenced by another catalog object.
    pub fn drop_object(&self, target: CatalogDrop) -> Result<Catalog> {
        let mut snapshot = self.snapshot()?;
        match target {
            CatalogDrop::Index { bucket, index } => drop_index(&mut snapshot, bucket, index)?,
            CatalogDrop::Collection(id) => drop_collection(&mut snapshot, id)?,
            CatalogDrop::Bucket(id) => drop_bucket(&mut snapshot, id)?,
            CatalogDrop::Keyspace(id) => drop_keyspace(&mut snapshot, id)?,
        }
        snapshot.generation = CatalogGeneration::new(snapshot.generation.get().saturating_add(1));

        Self::restore(snapshot)
    }
}

fn drop_index(snapshot: &mut CatalogSnapshot, bucket: BucketId, index: IndexId) -> Result<()> {
    let indexes = snapshot
        .indexes
        .get_mut(&bucket)
        .ok_or(CatalogError::NotFound("bucket"))?;
    if indexes.remove(&index).is_none() {
        return Err(CatalogError::NotFound("index"));
    }
    if indexes.is_empty() {
        snapshot.indexes.remove(&bucket);
    }

    Ok(())
}

fn drop_collection(snapshot: &mut CatalogSnapshot, id: CollectionId) -> Result<()> {
    let collection = snapshot
        .collections
        .get(&id)
        .ok_or(CatalogError::NotFound("collection"))?;
    for fragment in collection.fragments() {
        if let Some(other) =
            referencing_collection_excluding(snapshot, fragment.bucket_id(), Some(id))
        {
            return Err(CatalogError::Referenced(format!(
                "bucket {} is shared with collection {other}",
                fragment.bucket_id()
            )));
        }
    }
    snapshot.collections.remove(&id);

    Ok(())
}

fn drop_bucket(snapshot: &mut CatalogSnapshot, id: BucketId) -> Result<()> {
    if !snapshot.buckets.contains_key(&id) {
        return Err(CatalogError::NotFound("bucket"));
    }
    if let Some(collection) = referencing_collection(snapshot, id) {
        return Err(CatalogError::Referenced(format!(
            "bucket {id} is referenced by collection {collection}"
        )));
    }
    snapshot.buckets.remove(&id);
    snapshot.bucket_keyspaces.remove(&id);
    snapshot.indexes.remove(&id);
    snapshot.placements.remove(&id);

    Ok(())
}

fn drop_keyspace(snapshot: &mut CatalogSnapshot, id: KeyspaceId) -> Result<()> {
    if !snapshot.keyspaces.contains_key(&id) {
        return Err(CatalogError::NotFound("keyspace"));
    }

    let collections: Vec<CollectionId> = snapshot
        .collections
        .iter()
        .filter(|(_, collection)| collection.keyspace_id() == id)
        .map(|(collection_id, _)| *collection_id)
        .collect();
    let buckets: Vec<BucketId> = snapshot
        .bucket_keyspaces
        .iter()
        .filter(|(_, keyspace_id)| **keyspace_id == id)
        .map(|(bucket_id, _)| *bucket_id)
        .collect();

    for collection_id in &collections {
        let collection = &snapshot.collections[collection_id];
        for fragment in collection.fragments() {
            if snapshot.bucket_keyspaces.get(&fragment.bucket_id()) != Some(&id) {
                return Err(CatalogError::Referenced(format!(
                    "collection {collection_id} references bucket {} outside keyspace {id}",
                    fragment.bucket_id()
                )));
            }
        }
    }
    for (collection_id, collection) in &snapshot.collections {
        if collection.keyspace_id() == id {
            continue;
        }
        for fragment in collection.fragments() {
            if buckets.contains(&fragment.bucket_id()) {
                return Err(CatalogError::Referenced(format!(
                    "bucket {} is referenced by collection {collection_id} outside keyspace {id}",
                    fragment.bucket_id()
                )));
            }
        }
    }

    for collection_id in collections {
        snapshot.collections.remove(&collection_id);
    }
    for bucket_id in buckets {
        snapshot.buckets.remove(&bucket_id);
        snapshot.bucket_keyspaces.remove(&bucket_id);
        snapshot.indexes.remove(&bucket_id);
        snapshot.placements.remove(&bucket_id);
    }
    snapshot.keyspaces.remove(&id);

    Ok(())
}

fn referencing_collection(snapshot: &CatalogSnapshot, bucket: BucketId) -> Option<CollectionId> {
    referencing_collection_excluding(snapshot, bucket, None)
}

fn referencing_collection_excluding(
    snapshot: &CatalogSnapshot,
    bucket: BucketId,
    excluded: Option<CollectionId>,
) -> Option<CollectionId> {
    snapshot
        .collections
        .iter()
        .filter(|(collection_id, _)| Some(**collection_id) != excluded)
        .find(|(_, collection)| {
            collection
                .fragments()
                .iter()
                .any(|fragment| fragment.bucket_id() == bucket)
        })
        .map(|(collection_id, _)| *collection_id)
}
