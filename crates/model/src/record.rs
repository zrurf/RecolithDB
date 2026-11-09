/// Physical record payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Record {
    /// Creates a record from ordered key bytes and an opaque value.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Returns the ordered record key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the opaque record value.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}
