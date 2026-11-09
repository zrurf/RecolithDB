/// Backend health and size statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendStats {
    /// Number of stored entries, including tombstones.
    pub entries: u64,
    /// Current commit sequence.
    pub sequence: u64,
}
