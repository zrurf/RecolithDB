use recolith_common::{CommitTs, SchemaId, SchemaVersion};

/// Fixed record header fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHeader {
    schema_id: SchemaId,
    schema_version: SchemaVersion,
    flags: u16,
    commit_ts: CommitTs,
    expire_at: u64,
    payload_len: u32,
    payload_crc: u32,
}

impl RecordHeader {
    /// Creates a header for a new record.
    #[must_use]
    pub fn new(schema_id: SchemaId, schema_version: SchemaVersion, commit_ts: CommitTs) -> Self {
        Self {
            schema_id,
            schema_version,
            flags: 0,
            commit_ts,
            expire_at: 0,
            payload_len: 0,
            payload_crc: 0,
        }
    }

    /// Sets the header flags.
    #[must_use]
    pub fn with_flags(mut self, flags: u16) -> Self {
        self.flags = flags;
        self
    }

    /// Sets the optional expiration timestamp.
    #[must_use]
    pub fn with_expire_at(mut self, expire_at: u64) -> Self {
        self.expire_at = expire_at;
        self
    }

    /// Returns the schema identifier.
    #[must_use]
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Returns the schema version.
    #[must_use]
    pub fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the header flags.
    #[must_use]
    pub fn flags(&self) -> u16 {
        self.flags
    }

    /// Returns the commit timestamp.
    #[must_use]
    pub fn commit_ts(&self) -> CommitTs {
        self.commit_ts
    }

    /// Returns the expiration timestamp, or zero when unset.
    #[must_use]
    pub fn expire_at(&self) -> u64 {
        self.expire_at
    }

    /// Returns the encoded payload length.
    #[must_use]
    pub fn payload_len(&self) -> u32 {
        self.payload_len
    }

    /// Returns the encoded payload checksum.
    #[must_use]
    pub fn payload_crc(&self) -> u32 {
        self.payload_crc
    }

    pub(crate) fn with_payload(mut self, payload_len: u32, payload_crc: u32) -> Self {
        self.payload_len = payload_len;
        self.payload_crc = payload_crc;
        self
    }
}
