/// Scalar type allowed in a document key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyType {
    /// Unsigned 64-bit integer.
    U64,
    /// Signed 64-bit integer.
    I64,
    /// UTF-8 string.
    String,
    /// Opaque bytes.
    Bytes,
    /// 128-bit UUID.
    Uuid,
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::U64 => "u64",
            Self::I64 => "i64",
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Uuid => "uuid",
        })
    }
}

/// One component of a document key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyField {
    name: String,
    ty: KeyType,
}

impl KeyField {
    /// Creates a key field.
    #[must_use]
    pub fn new(name: impl Into<String>, ty: KeyType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }

    /// Returns the field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the key type.
    #[must_use]
    pub fn ty(&self) -> KeyType {
        self.ty
    }
}

/// Ordered schema of a Collection's document key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentKeySchema {
    fields: Vec<KeyField>,
}

impl DocumentKeySchema {
    /// Creates a document key schema.
    #[must_use]
    pub fn new(fields: Vec<KeyField>) -> Self {
        Self { fields }
    }

    /// Returns the ordered key fields.
    #[must_use]
    pub fn fields(&self) -> &[KeyField] {
        &self.fields
    }

    /// Returns a key field by name.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&KeyField> {
        self.fields.iter().find(|field| field.name() == name)
    }
}

/// Typed value supplied for one document key component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyValue {
    /// Unsigned 64-bit integer.
    U64(u64),
    /// Signed 64-bit integer.
    I64(i64),
    /// UTF-8 string.
    String(String),
    /// Opaque bytes.
    Bytes(Vec<u8>),
    /// 128-bit UUID.
    Uuid([u8; 16]),
}
