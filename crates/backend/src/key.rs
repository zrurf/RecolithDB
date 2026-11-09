use std::ops::Bound;

/// Ordered key/value pair returned by scans.
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// Ordered key range with optional bounds.
#[derive(Clone, Debug)]
pub struct KeyRange {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl KeyRange {
    /// Creates an unbounded range.
    #[must_use]
    pub const fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Creates a range from explicit bounds.
    #[must_use]
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self {
        Self { start, end }
    }

    /// Creates a range containing every key with the given prefix.
    #[must_use]
    pub fn prefix(prefix: &[u8]) -> Self {
        let end = prefix_end(prefix).map_or(Bound::Unbounded, Bound::Excluded);

        Self {
            start: Bound::Included(prefix.to_vec()),
            end,
        }
    }

    /// Returns the start bound.
    #[must_use]
    pub fn start_bound(&self) -> &Bound<Vec<u8>> {
        &self.start
    }

    /// Returns the end bound.
    #[must_use]
    pub fn end_bound(&self) -> &Bound<Vec<u8>> {
        &self.end
    }

    /// Returns whether the range contains `key`.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        let after_start = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(start) => key >= start.as_slice(),
            Bound::Excluded(start) => key > start.as_slice(),
        };
        let before_end = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= end.as_slice(),
            Bound::Excluded(end) => key < end.as_slice(),
        };

        after_start && before_end
    }
}

/// Scan options.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanOptions {
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
    /// Iterate in reverse key order.
    pub reverse: bool,
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();

    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }

    None
}
