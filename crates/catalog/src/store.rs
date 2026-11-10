//! Catalog persistence in a reserved system key range.

mod codec;

use std::collections::BTreeMap;

use recolith_backend::{KeyRange, ScanOptions, Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_common::{CatalogGeneration, KeyspaceId, SchemaVersion};

use crate::catalog::{Catalog, CatalogSnapshot};
use crate::error::{CatalogError, Result};

/// Reserved prefix for catalog metadata.
pub const CATALOG_PREFIX: &[u8] = b"\0recolith/catalog/";

const GENERATION: &[u8] = b"generation";
const KEYSPACE: &[u8] = b"keyspace/";
const BUCKET: &[u8] = b"bucket/";
const COLLECTION: &[u8] = b"collection/";
const INDEX: &[u8] = b"index/";
const PLACEMENT: &[u8] = b"placement/";
const SCHEMA: &[u8] = b"schema/";

/// Loads a catalog from a backend.
///
/// An empty metadata range returns an empty catalog at generation zero.
///
/// # Errors
///
/// Returns [`CatalogError`] for backend failures, malformed values, or metadata
/// that fails catalog validation.
pub fn load_catalog<B: StorageBackend>(backend: &B) -> Result<Catalog> {
    let snapshot = backend.snapshot()?;
    let entries = snapshot.scan(&KeyRange::prefix(CATALOG_PREFIX), ScanOptions::default())?;
    if entries.is_empty() {
        return Ok(Catalog::new());
    }

    let mut catalog = CatalogSnapshot {
        generation: CatalogGeneration::new(0),
        keyspaces: BTreeMap::new(),
        buckets: BTreeMap::new(),
        bucket_keyspaces: BTreeMap::new(),
        collections: BTreeMap::new(),
        indexes: BTreeMap::new(),
        placements: BTreeMap::new(),
        schemas: crate::schema_registry::SchemaSnapshot::default(),
    };
    let mut generation = None;

    for (key, value) in entries {
        let suffix = key
            .as_slice()
            .strip_prefix(CATALOG_PREFIX)
            .ok_or_else(|| invalid_metadata("catalog key without prefix"))?;
        if suffix == GENERATION {
            generation = Some(codec::decode_generation(&value)?);
        } else if let Some(rest) = suffix.strip_prefix(KEYSPACE) {
            let id = single_id(rest)?;
            let keyspace = codec::decode_keyspace(&value)?;
            if u64::from(keyspace.id().get()) != id {
                return Err(invalid_metadata("keyspace key and value disagree"));
            }
            catalog.keyspaces.insert(keyspace.id(), keyspace);
        } else if let Some(rest) = suffix.strip_prefix(BUCKET) {
            let (keyspace_id, bucket_id) = pair_id(rest)?;
            let bucket = codec::decode_bucket(&value)?;
            if bucket.id().get() != bucket_id {
                return Err(invalid_metadata("bucket key and value disagree"));
            }
            catalog.bucket_keyspaces.insert(
                bucket.id(),
                KeyspaceId::new(
                    u32::try_from(keyspace_id)
                        .map_err(|_| invalid_metadata("keyspace id exceeds u32"))?,
                ),
            );
            catalog.buckets.insert(bucket.id(), bucket);
        } else if let Some(rest) = suffix.strip_prefix(COLLECTION) {
            let id = single_id(rest)?;
            let collection = codec::decode_collection(&value)?;
            if collection.id().get() != id {
                return Err(invalid_metadata("collection key and value disagree"));
            }
            catalog.collections.insert(collection.id(), collection);
        } else if let Some(rest) = suffix.strip_prefix(INDEX) {
            let (bucket_id, index_id) = pair_id(rest)?;
            let index = codec::decode_index(&value)?;
            if index.id().get() != index_id {
                return Err(invalid_metadata("index key and value disagree"));
            }
            catalog
                .indexes
                .entry(recolith_common::BucketId::new(bucket_id))
                .or_default()
                .insert(index.id(), index);
        } else if let Some(rest) = suffix.strip_prefix(PLACEMENT) {
            let bucket_id = single_id(rest)?;
            let placement = codec::decode_placement(&value)?;
            if placement.bucket_id().get() != bucket_id {
                return Err(invalid_metadata("placement key and value disagree"));
            }
            catalog.placements.insert(placement.bucket_id(), placement);
        } else if let Some(rest) = suffix.strip_prefix(SCHEMA) {
            let (schema_id, version) = pair_id(rest)?;
            let schema = codec::decode_schema(&value)?;
            if schema.id().get() != schema_id || u64::from(schema.version().get()) != version {
                return Err(invalid_metadata("schema key and value disagree"));
            }
            catalog
                .schemas
                .schemas
                .insert((schema.id(), schema.version()), schema);
        } else {
            return Err(invalid_metadata("unknown catalog key"));
        }
    }

    catalog.generation = CatalogGeneration::new(
        generation.ok_or_else(|| invalid_metadata("missing generation record"))?,
    );
    catalog.schemas.latest = latest_versions(&catalog.schemas.schemas);
    Catalog::restore(catalog)
}

/// Prepared catalog writes for atomic staging with other backend changes.
#[derive(Debug)]
pub struct CatalogWrite {
    desired: BTreeMap<Vec<u8>, Vec<u8>>,
    existing: Vec<Vec<u8>>,
}

impl CatalogWrite {
    /// Captures the desired catalog snapshot and the currently stored keys.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] if the snapshot is inconsistent or the backend
    /// cannot be read.
    pub fn prepare<B: StorageBackend>(backend: &B, catalog: &Catalog) -> Result<Self> {
        let snapshot = catalog.snapshot()?;
        let desired = encode_snapshot(&snapshot)?;
        let existing = backend
            .snapshot()?
            .scan(&KeyRange::prefix(CATALOG_PREFIX), ScanOptions::default())?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        Ok(Self { desired, existing })
    }

    /// Stages catalog deletions and writes in a transaction.
    pub fn stage<T: Transaction>(&self, transaction: &mut T) {
        for key in &self.existing {
            if !self.desired.contains_key(key) {
                transaction.delete(key);
            }
        }
        for (key, value) in &self.desired {
            transaction.put(key, value.clone());
        }
    }
}

/// Saves the complete catalog snapshot atomically.
///
/// Existing metadata keys that are not part of the snapshot are removed.
///
/// # Errors
///
/// Returns [`CatalogError`] if the snapshot is inconsistent or backend access fails.
pub fn save_catalog<B: StorageBackend>(backend: &B, catalog: &Catalog) -> Result<()> {
    let write = CatalogWrite::prepare(backend, catalog)?;
    let mut transaction = backend.begin(TxnOptions::default())?;
    write.stage(&mut transaction);
    transaction.commit()?;

    Ok(())
}

fn encode_snapshot(snapshot: &CatalogSnapshot) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut entries = BTreeMap::new();
    entries.insert(
        key(GENERATION, &[]),
        codec::encode_generation(snapshot.generation.get()),
    );

    for keyspace in snapshot.keyspaces.values() {
        entries.insert(
            key(KEYSPACE, &[u64::from(keyspace.id().get())]),
            codec::encode_keyspace(keyspace),
        );
    }
    for (bucket_id, bucket) in &snapshot.buckets {
        let keyspace_id = snapshot.bucket_keyspaces.get(bucket_id).ok_or_else(|| {
            CatalogError::InvalidReference(format!("bucket {bucket_id} has no keyspace"))
        })?;
        entries.insert(
            key(BUCKET, &[u64::from(keyspace_id.get()), bucket_id.get()]),
            codec::encode_bucket(bucket),
        );
    }
    for collection in snapshot.collections.values() {
        entries.insert(
            key(COLLECTION, &[collection.id().get()]),
            codec::encode_collection(collection),
        );
    }
    for (bucket_id, indexes) in &snapshot.indexes {
        for index in indexes.values() {
            entries.insert(
                key(INDEX, &[bucket_id.get(), index.id().get()]),
                codec::encode_index(index),
            );
        }
    }
    for placement in snapshot.placements.values() {
        entries.insert(
            key(PLACEMENT, &[placement.bucket_id().get()]),
            codec::encode_placement(placement),
        );
    }
    for schema in snapshot.schemas.schemas.values() {
        entries.insert(
            key(
                SCHEMA,
                &[schema.id().get(), u64::from(schema.version().get())],
            ),
            codec::encode_schema(schema),
        );
    }

    Ok(entries)
}

fn latest_versions(
    schemas: &BTreeMap<(recolith_common::SchemaId, SchemaVersion), recolith_schema::RecordSchema>,
) -> BTreeMap<recolith_common::SchemaId, SchemaVersion> {
    let mut latest = BTreeMap::new();
    for (schema_id, version) in schemas.keys() {
        latest.insert(*schema_id, *version);
    }

    latest
}

fn key(tag: &[u8], ids: &[u64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(CATALOG_PREFIX.len() + tag.len() + ids.len() * 9);
    key.extend_from_slice(CATALOG_PREFIX);
    key.extend_from_slice(tag);
    for (index, id) in ids.iter().enumerate() {
        if index > 0 {
            key.push(b'/');
        }
        key.extend_from_slice(&id.to_be_bytes());
    }

    key
}

fn single_id(bytes: &[u8]) -> Result<u64> {
    if bytes.len() != 8 {
        return Err(invalid_metadata("malformed identifier suffix"));
    }

    Ok(u64::from_be_bytes(bytes.try_into().expect("eight bytes")))
}

fn pair_id(bytes: &[u8]) -> Result<(u64, u64)> {
    if bytes.len() != 17 || bytes[8] != b'/' {
        return Err(invalid_metadata("malformed identifier suffix"));
    }

    Ok((
        u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
        u64::from_be_bytes(bytes[9..].try_into().expect("eight bytes")),
    ))
}

fn invalid_metadata(message: &str) -> CatalogError {
    CatalogError::InvalidMetadata(message.to_owned())
}
