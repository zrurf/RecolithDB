use std::collections::BTreeMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use recolith_common::{SchemaId, SchemaVersion};
use recolith_schema::RecordSchema;

use crate::error::{CatalogError, Result};

/// Snapshot of all registered schemas.
#[derive(Clone, Debug, Default)]
pub struct SchemaSnapshot {
    /// All schema versions keyed by ID and version.
    pub schemas: BTreeMap<(SchemaId, SchemaVersion), RecordSchema>,
    /// Latest version for each schema ID.
    pub latest: BTreeMap<SchemaId, SchemaVersion>,
}

#[derive(Debug, Default)]
struct State {
    schemas: BTreeMap<(SchemaId, SchemaVersion), RecordSchema>,
    latest: BTreeMap<SchemaId, SchemaVersion>,
}

/// Versioned schema registry with monotonic version tracking.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    state: RwLock<State>,
}

impl SchemaRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new schema version.
    ///
    /// Migration-requiring versions are stored so they can be activated later
    /// through an explicit alter. Version numbers must still increase.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] for invalid schemas or non-monotonic versions.
    pub fn register(&self, schema: RecordSchema) -> Result<SchemaVersion> {
        schema.validate()?;

        let id = schema.id();
        let version = schema.version();
        let mut state = self.write()?;

        if let Some(latest_version) = state.latest.get(&id)
            && version.get() <= latest_version.get()
        {
            return Err(CatalogError::SchemaVersionConflict);
        }

        state.schemas.insert((id, version), schema);
        state.latest.insert(id, version);

        Ok(version)
    }

    /// Returns a specific schema version.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::LockPoisoned`] if the registry lock is poisoned.
    pub fn get(&self, id: SchemaId, version: SchemaVersion) -> Result<Option<RecordSchema>> {
        Ok(self.read()?.schemas.get(&(id, version)).cloned())
    }

    /// Returns the latest schema for `id`.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::LockPoisoned`] if the registry lock is poisoned.
    pub fn latest(&self, id: SchemaId) -> Result<Option<RecordSchema>> {
        let state = self.read()?;
        let Some(version) = state.latest.get(&id) else {
            return Ok(None);
        };

        Ok(state.schemas.get(&(id, *version)).cloned())
    }

    /// Returns an immutable snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::LockPoisoned`] if the registry lock is poisoned.
    pub fn snapshot(&self) -> Result<SchemaSnapshot> {
        let state = self.read()?;

        Ok(SchemaSnapshot {
            schemas: state.schemas.clone(),
            latest: state.latest.clone(),
        })
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, State>> {
        self.state.read().map_err(|_| CatalogError::LockPoisoned)
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, State>> {
        self.state.write().map_err(|_| CatalogError::LockPoisoned)
    }
}
