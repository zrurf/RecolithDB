use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use recolith_common::{
    BucketId, CatalogGeneration, CollectionId, FieldId, IndexId, KeyspaceId, SchemaId,
    SchemaVersion,
};
use recolith_model::{Bucket, Collection, IndexDefinition, IndexKind, Keyspace};
use recolith_schema::{RecordSchema, Type};

use crate::error::{CatalogError, Result};
use crate::placement::BucketPlacement;
use crate::schema_registry::{SchemaRegistry, SchemaSnapshot};

/// Immutable catalog snapshot.
#[derive(Clone, Debug)]
pub struct CatalogSnapshot {
    /// Catalog generation at snapshot time.
    pub generation: CatalogGeneration,
    /// Keyspaces.
    pub keyspaces: BTreeMap<KeyspaceId, Keyspace>,
    /// Buckets.
    pub buckets: BTreeMap<BucketId, Bucket>,
    /// Keyspace ownership for each bucket.
    pub bucket_keyspaces: BTreeMap<BucketId, KeyspaceId>,
    /// Collections.
    pub collections: BTreeMap<CollectionId, Collection>,
    /// Index definitions grouped by bucket.
    pub indexes: BTreeMap<BucketId, BTreeMap<IndexId, IndexDefinition>>,
    /// Bucket placements.
    pub placements: BTreeMap<BucketId, BucketPlacement>,
    /// Schema snapshot.
    pub schemas: SchemaSnapshot,
}

impl CatalogSnapshot {
    /// Returns the first index on `bucket` whose leading field is `field`.
    #[must_use]
    pub fn index_for_field(&self, bucket: BucketId, field: FieldId) -> Option<&IndexDefinition> {
        self.indexes
            .get(&bucket)?
            .values()
            .find(|index| index.leading_field() == Some(field))
    }

    /// Returns a collection by name.
    #[must_use]
    pub fn collection_by_name(&self, name: &str) -> Option<&Collection> {
        self.collections
            .values()
            .find(|collection| collection.name() == name)
    }
}

#[derive(Debug, Default)]
struct CatalogState {
    keyspaces: BTreeMap<KeyspaceId, Keyspace>,
    buckets: BTreeMap<BucketId, Bucket>,
    bucket_keyspaces: BTreeMap<BucketId, KeyspaceId>,
    collections: BTreeMap<CollectionId, Collection>,
    indexes: BTreeMap<BucketId, BTreeMap<IndexId, IndexDefinition>>,
    placements: BTreeMap<BucketId, BucketPlacement>,
}

/// Local catalog with generation tracking.
#[derive(Debug)]
pub struct Catalog {
    state: RwLock<CatalogState>,
    schema_registry: SchemaRegistry,
    generation: AtomicU64,
}

impl Catalog {
    /// Creates an empty catalog at generation zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(CatalogState::default()),
            schema_registry: SchemaRegistry::new(),
            generation: AtomicU64::new(0),
        }
    }

    /// Returns the current catalog generation.
    #[must_use]
    pub fn generation(&self) -> CatalogGeneration {
        CatalogGeneration::new(self.generation.load(Ordering::SeqCst))
    }

    /// Creates a keyspace.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Duplicate`] if the keyspace already exists.
    pub fn create_keyspace(&self, keyspace: Keyspace) -> Result<()> {
        let mut state = self.write()?;
        if state.keyspaces.contains_key(&keyspace.id()) {
            return Err(CatalogError::Duplicate("keyspace"));
        }
        if state
            .keyspaces
            .values()
            .any(|existing| existing.name() == keyspace.name())
        {
            return Err(CatalogError::Duplicate("keyspace name"));
        }
        state.keyspaces.insert(keyspace.id(), keyspace);
        drop(state);
        self.bump();

        Ok(())
    }

    /// Creates a bucket in a keyspace.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for a missing keyspace, a missing schema, or a duplicate
    /// bucket ID.
    pub fn create_bucket(&self, keyspace_id: KeyspaceId, bucket: Bucket) -> Result<()> {
        let mut state = self.write()?;
        if !state.keyspaces.contains_key(&keyspace_id) {
            return Err(CatalogError::NotFound("keyspace"));
        }
        if state.buckets.contains_key(&bucket.id()) {
            return Err(CatalogError::Duplicate("bucket"));
        }
        if state.buckets.values().any(|existing| {
            existing.name() == bucket.name()
                && state.bucket_keyspaces.get(&existing.id()) == Some(&keyspace_id)
        }) {
            return Err(CatalogError::Duplicate("bucket name"));
        }
        if self
            .schema_registry
            .get(bucket.schema_id(), bucket.schema_version())?
            .is_none()
        {
            return Err(CatalogError::NotFound("schema"));
        }

        state.bucket_keyspaces.insert(bucket.id(), keyspace_id);
        state.buckets.insert(bucket.id(), bucket);
        drop(state);
        self.bump();

        Ok(())
    }

    /// Creates a collection and verifies its fragment bucket references.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for missing keyspaces/buckets or duplicate collections.
    pub fn create_collection(&self, collection: Collection) -> Result<()> {
        let mut state = self.write()?;
        if !state.keyspaces.contains_key(&collection.keyspace_id()) {
            return Err(CatalogError::NotFound("keyspace"));
        }
        if state.collections.contains_key(&collection.id()) {
            return Err(CatalogError::Duplicate("collection"));
        }
        if state.collections.values().any(|existing| {
            existing.name() == collection.name()
                && existing.keyspace_id() == collection.keyspace_id()
        }) {
            return Err(CatalogError::Duplicate("collection name"));
        }
        for fragment in collection.fragments() {
            if !state.buckets.contains_key(&fragment.bucket_id()) {
                return Err(CatalogError::InvalidReference(format!(
                    "fragment {} references missing bucket {}",
                    fragment.id(),
                    fragment.bucket_id()
                )));
            }
        }

        state.collections.insert(collection.id(), collection);
        drop(state);
        self.bump();

        Ok(())
    }

    /// Registers an index on a bucket.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for a missing bucket or schema, a duplicate index ID,
    /// unknown fields, non-indexable field types, or an invalid field count.
    pub fn create_index(&self, bucket_id: BucketId, index: IndexDefinition) -> Result<()> {
        let mut state = self.write()?;
        let bucket = state
            .buckets
            .get(&bucket_id)
            .ok_or(CatalogError::NotFound("bucket"))?
            .clone();
        if state
            .indexes
            .get(&bucket_id)
            .is_some_and(|indexes| indexes.contains_key(&index.id()))
        {
            return Err(CatalogError::Duplicate("index"));
        }
        if state.indexes.get(&bucket_id).is_some_and(|indexes| {
            indexes
                .values()
                .any(|existing| existing.name() == index.name())
        }) {
            return Err(CatalogError::Duplicate("index name"));
        }

        let schema = self
            .schema_registry
            .get(bucket.schema_id(), bucket.schema_version())?
            .ok_or(CatalogError::NotFound("schema"))?;
        validate_index(&schema, &index)?;

        state
            .indexes
            .entry(bucket_id)
            .or_default()
            .insert(index.id(), index);
        drop(state);
        self.bump();

        Ok(())
    }

    /// Assigns a bucket to a backend.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for missing buckets or mismatched partition counts.
    pub fn place_bucket(&self, placement: BucketPlacement) -> Result<()> {
        let mut state = self.write()?;
        let bucket = state
            .buckets
            .get(&placement.bucket_id())
            .ok_or(CatalogError::NotFound("bucket"))?;

        if bucket.partition_count() != placement.partition_count() {
            return Err(CatalogError::InvalidPlacement(format!(
                "bucket {} has {} partitions, placement has {}",
                bucket.id(),
                bucket.partition_count(),
                placement.partition_count()
            )));
        }
        if placement.affinity_group().is_empty() {
            return Err(CatalogError::InvalidPlacement(
                "affinity group must not be empty".to_owned(),
            ));
        }

        state.placements.insert(placement.bucket_id(), placement);
        drop(state);
        self.bump();

        Ok(())
    }

    /// Registers a schema version and advances the catalog generation.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for invalid, conflicting, or migration-requiring schemas.
    pub fn register_schema(&self, schema: RecordSchema) -> Result<SchemaVersion> {
        let version = self.schema_registry.register(schema)?;
        self.bump();

        Ok(version)
    }

    /// Returns a specific schema version.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::LockPoisoned`] if the schema registry is poisoned.
    pub fn schema(&self, id: SchemaId, version: SchemaVersion) -> Result<Option<RecordSchema>> {
        self.schema_registry.get(id, version)
    }

    /// Returns the latest schema for `id`.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::LockPoisoned`] if the schema registry is poisoned.
    pub fn latest_schema(&self, id: SchemaId) -> Result<Option<RecordSchema>> {
        self.schema_registry.latest(id)
    }

    /// Returns the placement for a bucket.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::LockPoisoned`] if the catalog lock is poisoned.
    pub fn placement(&self, bucket: BucketId) -> Result<Option<BucketPlacement>> {
        Ok(self.read()?.placements.get(&bucket).cloned())
    }

    /// Returns an immutable catalog snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::LockPoisoned`] if an internal lock is poisoned.
    pub fn snapshot(&self) -> Result<CatalogSnapshot> {
        let state = self.read()?;
        let schemas = self.schema_registry.snapshot()?;

        Ok(CatalogSnapshot {
            generation: self.generation(),
            keyspaces: state.keyspaces.clone(),
            buckets: state.buckets.clone(),
            bucket_keyspaces: state.bucket_keyspaces.clone(),
            collections: state.collections.clone(),
            indexes: state.indexes.clone(),
            placements: state.placements.clone(),
            schemas,
        })
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, CatalogState>> {
        self.state.read().map_err(|_| CatalogError::LockPoisoned)
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, CatalogState>> {
        self.state.write().map_err(|_| CatalogError::LockPoisoned)
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Rebuilds a catalog from a complete snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] when schemas are invalid or incompatible, or
    /// when metadata references a missing object, duplicates a name, or
    /// contains an invalid index or placement.
    pub fn restore(snapshot: CatalogSnapshot) -> Result<Self> {
        let schema_registry = SchemaRegistry::new();
        for schema in snapshot.schemas.schemas.values() {
            schema_registry.register(schema.clone())?;
        }

        let mut keyspace_names = BTreeSet::new();
        for keyspace in snapshot.keyspaces.values() {
            if !keyspace_names.insert(keyspace.name().to_owned()) {
                return Err(CatalogError::Duplicate("keyspace name"));
            }
        }

        let mut bucket_names = BTreeSet::new();
        for (bucket_id, bucket) in &snapshot.buckets {
            let keyspace_id = snapshot.bucket_keyspaces.get(bucket_id).ok_or_else(|| {
                CatalogError::InvalidReference(format!("bucket {bucket_id} has no keyspace"))
            })?;
            if !snapshot.keyspaces.contains_key(keyspace_id) {
                return Err(CatalogError::InvalidReference(format!(
                    "bucket {bucket_id} references missing keyspace {keyspace_id}"
                )));
            }
            if !bucket_names.insert((*keyspace_id, bucket.name().to_owned())) {
                return Err(CatalogError::Duplicate("bucket name"));
            }
            if !snapshot
                .schemas
                .schemas
                .contains_key(&(bucket.schema_id(), bucket.schema_version()))
            {
                return Err(CatalogError::InvalidReference(format!(
                    "bucket {bucket_id} references missing schema"
                )));
            }
        }

        let mut collection_names = BTreeSet::new();
        for collection in snapshot.collections.values() {
            if !snapshot.keyspaces.contains_key(&collection.keyspace_id()) {
                return Err(CatalogError::InvalidReference(format!(
                    "collection {} references missing keyspace",
                    collection.id()
                )));
            }
            if !collection_names.insert((collection.keyspace_id(), collection.name().to_owned())) {
                return Err(CatalogError::Duplicate("collection name"));
            }
            if collection.key_schema().fields().is_empty() {
                return Err(CatalogError::InvalidReference(format!(
                    "collection {} has an empty document key schema",
                    collection.id()
                )));
            }
            for fragment in collection.fragments() {
                if !snapshot.buckets.contains_key(&fragment.bucket_id()) {
                    return Err(CatalogError::InvalidReference(format!(
                        "fragment {} references missing bucket {}",
                        fragment.id(),
                        fragment.bucket_id()
                    )));
                }
            }
        }

        let mut index_names = BTreeSet::new();
        for (bucket_id, indexes) in &snapshot.indexes {
            let bucket = snapshot.buckets.get(bucket_id).ok_or_else(|| {
                CatalogError::InvalidReference(format!(
                    "indexes reference missing bucket {bucket_id}"
                ))
            })?;
            let schema = snapshot
                .schemas
                .schemas
                .get(&(bucket.schema_id(), bucket.schema_version()))
                .ok_or(CatalogError::NotFound("schema"))?;
            for index in indexes.values() {
                if !index_names.insert((*bucket_id, index.name().to_owned())) {
                    return Err(CatalogError::Duplicate("index name"));
                }
                validate_index(schema, index)?;
            }
        }

        for placement in snapshot.placements.values() {
            let bucket = snapshot
                .buckets
                .get(&placement.bucket_id())
                .ok_or_else(|| {
                    CatalogError::InvalidReference(format!(
                        "placement references missing bucket {}",
                        placement.bucket_id()
                    ))
                })?;
            if placement.affinity_group().is_empty() {
                return Err(CatalogError::InvalidPlacement(
                    "affinity group must not be empty".to_owned(),
                ));
            }
            if bucket.partition_count() != placement.partition_count() {
                return Err(CatalogError::InvalidPlacement(format!(
                    "bucket {} has {} partitions, placement has {}",
                    bucket.id(),
                    bucket.partition_count(),
                    placement.partition_count()
                )));
            }
        }

        Ok(Self {
            state: RwLock::new(CatalogState {
                keyspaces: snapshot.keyspaces,
                buckets: snapshot.buckets,
                bucket_keyspaces: snapshot.bucket_keyspaces,
                collections: snapshot.collections,
                indexes: snapshot.indexes,
                placements: snapshot.placements,
            }),
            schema_registry,
            generation: AtomicU64::new(snapshot.generation.get()),
        })
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn validate_index(schema: &RecordSchema, index: &IndexDefinition) -> Result<()> {
    let expected_fields = if index.kind() == IndexKind::Composite {
        2
    } else {
        1
    };
    if index.fields().len() < expected_fields {
        return Err(CatalogError::InvalidIndex(format!(
            "index {} requires at least {expected_fields} fields",
            index.id()
        )));
    }
    if index.kind() != IndexKind::Composite && index.fields().len() != 1 {
        return Err(CatalogError::InvalidIndex(format!(
            "index {} of kind {:?} requires exactly one field",
            index.id(),
            index.kind()
        )));
    }

    for field_id in index.fields() {
        let field = schema.field(*field_id).ok_or_else(|| {
            CatalogError::InvalidIndex(format!(
                "index {} references missing field {field_id}",
                index.id()
            ))
        })?;
        if !is_indexable(field.ty()) {
            return Err(CatalogError::InvalidIndex(format!(
                "field {field_id} of type {} cannot be indexed",
                field.ty()
            )));
        }
    }

    Ok(())
}

fn is_indexable(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Bool
            | Type::I32
            | Type::I64
            | Type::U32
            | Type::U64
            | Type::String
            | Type::Bytes
            | Type::Timestamp
            | Type::Duration
            | Type::Enum(_)
    )
}
