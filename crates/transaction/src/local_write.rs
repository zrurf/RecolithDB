use std::fmt;

use recolith_backend::{CommitInfo, StorageBackend, Transaction, TxnOptions};
use recolith_common::{CommitTs, IndexId};
use recolith_schema::RecordSchema;
use recolith_storage::{
    RecordData, RecordHeader, decode_record_key_head, delete_index_entry, get_record,
    put_index_entry, put_unique_index_entry, read_fields as read_record_fields, write_fields,
};

use crate::clock::PersistentCommitClock;
use crate::error::{Result, TransactionError};

/// Index entry update applied with a record write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexUpdate {
    index: IndexId,
    old_values: Option<Vec<u8>>,
    new_values: Option<Vec<u8>>,
    unique: bool,
}

impl IndexUpdate {
    /// Creates an index update. `None` new values deletes the old entry.
    #[must_use]
    pub fn new(
        index: IndexId,
        old_values: Option<Vec<u8>>,
        new_values: impl Into<Option<Vec<u8>>>,
        unique: bool,
    ) -> Self {
        Self {
            index,
            old_values,
            new_values: new_values.into(),
            unique,
        }
    }

    /// Returns the index identifier.
    #[must_use]
    pub fn index(&self) -> IndexId {
        self.index
    }

    /// Returns the previous encoded values, if any.
    #[must_use]
    pub fn old_values(&self) -> Option<&[u8]> {
        self.old_values.as_deref()
    }

    /// Returns the new encoded values, if the entry is kept.
    #[must_use]
    pub fn new_values(&self) -> Option<&[u8]> {
        self.new_values.as_deref()
    }

    /// Returns whether the index enforces uniqueness.
    #[must_use]
    pub fn unique(&self) -> bool {
        self.unique
    }
}

/// One record write in a multi-fragment local batch.
#[derive(Clone, Debug)]
pub struct FragmentWrite {
    key: Vec<u8>,
    schema: RecordSchema,
    data: RecordData,
    if_match: Option<CommitTs>,
    indexes: Vec<IndexUpdate>,
}

impl FragmentWrite {
    /// Creates a fragment write.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, schema: RecordSchema, data: RecordData) -> Self {
        Self {
            key: key.into(),
            schema,
            data,
            if_match: None,
            indexes: Vec::new(),
        }
    }

    /// Adds an optimistic version check.
    #[must_use]
    pub fn with_if_match(mut self, expected: CommitTs) -> Self {
        self.if_match = Some(expected);
        self
    }

    /// Adds one index update.
    #[must_use]
    pub fn with_index(mut self, update: IndexUpdate) -> Self {
        self.indexes.push(update);
        self
    }

    /// Returns the record key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the record schema.
    #[must_use]
    pub fn schema(&self) -> &RecordSchema {
        &self.schema
    }

    /// Returns the record data.
    #[must_use]
    pub fn data(&self) -> &RecordData {
        &self.data
    }

    /// Returns the optimistic version check.
    #[must_use]
    pub fn if_match(&self) -> Option<CommitTs> {
        self.if_match
    }

    /// Returns the index updates.
    #[must_use]
    pub fn indexes(&self) -> &[IndexUpdate] {
        &self.indexes
    }
}

/// Local write path that stamps records with a persistent commit clock.
pub struct LocalWritePath<B: StorageBackend> {
    transaction: B::Transaction,
    clock: Option<PersistentCommitClock>,
}

impl<B: StorageBackend> LocalWritePath<B> {
    /// Begins a transaction and loads the persistent commit clock.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError`] for backend failures or a malformed clock record.
    pub fn begin(backend: &B, clock: PersistentCommitClock, options: TxnOptions) -> Result<Self> {
        let mut transaction = backend.begin(options)?;
        clock.load(&mut transaction)?;

        Ok(Self {
            transaction,
            clock: Some(clock),
        })
    }

    /// Begins a transaction without attaching a commit clock.
    ///
    /// Use [`Self::write_fragments_at`] with an externally allocated timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Backend`] if the transaction cannot begin.
    pub fn begin_unclocked(backend: &B, options: TxnOptions) -> Result<Self> {
        Ok(Self {
            transaction: backend.begin(options)?,
            clock: None,
        })
    }

    /// Writes a schema-aware record and returns its commit timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError`] for clock, encoding, schema, or backend failures.
    pub fn write_record(
        &mut self,
        key: &[u8],
        schema: &RecordSchema,
        data: &RecordData,
    ) -> Result<CommitTs> {
        self.write_record_internal(key, schema, data, &[], None)
    }

    /// Writes a record only when its current version is not newer than `expected`.
    ///
    /// `expected = 0` means create-only.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::VersionConflict`] on a failed optimistic check.
    pub fn write_record_if_match(
        &mut self,
        key: &[u8],
        schema: &RecordSchema,
        data: &RecordData,
        expected: CommitTs,
    ) -> Result<CommitTs> {
        self.write_record_internal(key, schema, data, &[], Some(expected))
    }

    /// Writes a record and maintains the supplied index entries atomically.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError`] for storage/index conflicts or backend failures.
    pub fn write_record_with_indexes(
        &mut self,
        key: &[u8],
        schema: &RecordSchema,
        data: &RecordData,
        indexes: &[IndexUpdate],
    ) -> Result<CommitTs> {
        self.write_record_internal(key, schema, data, indexes, None)
    }

    /// Writes a record with optimistic version checking and index maintenance.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError`] for version conflicts, index conflicts, or backend failures.
    pub fn write_record_with_indexes_if_match(
        &mut self,
        key: &[u8],
        schema: &RecordSchema,
        data: &RecordData,
        indexes: &[IndexUpdate],
        expected: CommitTs,
    ) -> Result<CommitTs> {
        self.write_record_internal(key, schema, data, indexes, Some(expected))
    }

    /// Writes multiple fragments with one commit timestamp.
    ///
    /// All fragments share the same commit timestamp so a document write has one
    /// logical version even when it spans multiple records.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::EmptyWriteBatch`] for an empty batch, or a version,
    /// storage, index, or backend error.
    pub fn write_fragments(&mut self, fragments: &[FragmentWrite]) -> Result<CommitTs> {
        self.validate_fragments(fragments)?;
        let commit_ts = self.reserve_timestamp()?;
        self.write_fragments_inner(fragments, commit_ts)
    }

    /// Writes multiple fragments with a caller-supplied commit timestamp.
    ///
    /// The caller must allocate unique timestamps, for example with
    /// [`crate::BatchedCommitClock`].
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::EmptyWriteBatch`] for an empty batch, or a version,
    /// storage, index, or backend error.
    pub fn write_fragments_at(
        &mut self,
        fragments: &[FragmentWrite],
        commit_ts: CommitTs,
    ) -> Result<CommitTs> {
        self.validate_fragments(fragments)?;
        self.write_fragments_inner(fragments, commit_ts)
    }

    fn validate_fragments(&mut self, fragments: &[FragmentWrite]) -> Result<()> {
        if fragments.is_empty() {
            return Err(TransactionError::EmptyWriteBatch);
        }

        for fragment in fragments {
            if let Some(expected) = fragment.if_match() {
                self.check_version(fragment.key(), expected)?;
            }
        }

        Ok(())
    }

    fn write_fragments_inner(
        &mut self,
        fragments: &[FragmentWrite],
        commit_ts: CommitTs,
    ) -> Result<CommitTs> {
        for fragment in fragments {
            let header = RecordHeader::new(
                fragment.schema().id(),
                fragment.schema().version(),
                commit_ts,
            );
            write_fields(
                &mut self.transaction,
                fragment.key(),
                &header,
                fragment.schema(),
                fragment.data(),
            )?;
            self.apply_indexes(fragment.key(), fragment.indexes())?;
        }

        Ok(commit_ts)
    }

    fn reserve_timestamp(&mut self) -> Result<CommitTs> {
        let clock = self
            .clock
            .as_ref()
            .ok_or(TransactionError::MissingCommitClock)?;

        clock.reserve(&mut self.transaction)
    }

    fn write_record_internal(
        &mut self,
        key: &[u8],
        schema: &RecordSchema,
        data: &RecordData,
        indexes: &[IndexUpdate],
        if_match: Option<CommitTs>,
    ) -> Result<CommitTs> {
        if let Some(expected) = if_match {
            self.check_version(key, expected)?;
        }

        let commit_ts = self.reserve_timestamp()?;
        let header = RecordHeader::new(schema.id(), schema.version(), commit_ts);
        write_fields(&mut self.transaction, key, &header, schema, data)?;
        self.apply_indexes(key, indexes)?;

        Ok(commit_ts)
    }

    fn apply_indexes(&mut self, key: &[u8], indexes: &[IndexUpdate]) -> Result<()> {
        if indexes.is_empty() {
            return Ok(());
        }
        let head = decode_record_key_head(key)?;

        for update in indexes {
            if let Some(old_values) = update.old_values() {
                delete_index_entry(
                    &mut self.transaction,
                    head.bucket(),
                    head.partition(),
                    update.index(),
                    old_values,
                    key,
                );
            }
            let Some(new_values) = update.new_values() else {
                continue;
            };
            if update.unique() {
                put_unique_index_entry(
                    &mut self.transaction,
                    head.bucket(),
                    head.partition(),
                    update.index(),
                    new_values,
                    key,
                )?;
            } else {
                put_index_entry(
                    &mut self.transaction,
                    head.bucket(),
                    head.partition(),
                    update.index(),
                    new_values,
                    key,
                );
            }
        }

        Ok(())
    }

    fn check_version(&mut self, key: &[u8], expected: CommitTs) -> Result<()> {
        let Some(stored) = get_record(&mut self.transaction, key)? else {
            if expected.get() == 0 {
                return Ok(());
            }

            return Err(TransactionError::VersionConflict {
                expected,
                actual: None,
            });
        };
        let actual = stored.header().commit_ts();

        if actual.get() > expected.get() {
            return Err(TransactionError::VersionConflict {
                expected,
                actual: Some(actual),
            });
        }

        Ok(())
    }

    /// Reads and decodes a record.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError`] for storage, schema, or backend failures.
    pub fn read_fields(
        &mut self,
        key: &[u8],
        schema: &RecordSchema,
    ) -> Result<Option<(RecordHeader, RecordData)>> {
        Ok(read_record_fields(&mut self.transaction, key, schema)?)
    }

    /// Reads raw bytes for a system key in this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Backend`] if the read fails.
    pub fn get_raw(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.transaction.get(key)?)
    }

    /// Stages raw bytes for a system key in this transaction.
    pub fn put_raw(&mut self, key: &[u8], value: Vec<u8>) {
        self.transaction.put(key, value);
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionError::Backend`] if the commit fails.
    pub fn commit(self) -> Result<CommitInfo> {
        Ok(self.transaction.commit()?)
    }

    /// Rolls back the transaction.
    pub fn rollback(self) {
        self.transaction.rollback();
    }
}

impl<B: StorageBackend> fmt::Debug for LocalWritePath<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LocalWritePath")
    }
}
