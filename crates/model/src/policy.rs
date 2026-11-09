/// Read consistency policy for a collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Consistency {
    /// Read a single snapshot.
    Snapshot,
}
