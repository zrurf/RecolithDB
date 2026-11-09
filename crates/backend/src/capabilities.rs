/// Optional and required backend capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capabilities {
    /// Snapshot reads are stable across concurrent writes.
    pub snapshot_isolation: bool,
    /// Multi-key writes commit atomically.
    pub multi_key_atomic: bool,
    /// Write-write conflicts are detected.
    pub conflict_detection: bool,
    /// Ordered range scans are supported.
    pub range_scan: bool,
    /// Reverse range scans are supported.
    pub reverse_scan: bool,
    /// The backend batches durable commits.
    pub group_commit: bool,
    /// The backend supports checkpoint or backup.
    pub checkpoint: bool,
}

impl Capabilities {
    /// Returns whether the backend satisfies the managed-collection contract.
    #[must_use]
    pub const fn supports_managed_collections(self) -> bool {
        self.snapshot_isolation
            && self.multi_key_atomic
            && self.conflict_detection
            && self.range_scan
    }
}
