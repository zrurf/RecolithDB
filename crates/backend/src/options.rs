/// Durability requested for a commit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    /// Return after the write is buffered.
    Async,
    /// Batch durable commits.
    #[default]
    Group,
    /// Force a durable commit for this transaction.
    Sync,
}

/// Transaction options.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TxnOptions {
    /// Requested commit durability.
    pub durability: Durability,
}

/// Result of a committed transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitInfo {
    /// Monotonic commit sequence assigned by the backend.
    pub sequence: u64,
}
