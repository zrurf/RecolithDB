use std::cmp::Ordering;
use std::fmt;

use recolith_common::bytes::{get_ordered_bytes, put_ordered_bytes};

/// Maximum canonical document key length in bytes.
pub const MAX_DOCUMENT_KEY_LEN: usize = 256;

/// Document key encoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DocumentKeyError {
    /// The key schema has no fields.
    EmptySchema,
    /// The value count does not match the schema.
    CountMismatch {
        /// Expected value count.
        expected: usize,
        /// Supplied value count.
        actual: usize,
    },
    /// A value does not match its field type.
    TypeMismatch {
        /// Key field name.
        field: String,
        /// Expected field type.
        expected: KeyType,
    },
    /// The encoded key exceeds the length limit.
    TooLong {
        /// Encoded length.
        length: usize,
        /// Maximum length.
        limit: usize,
    },
    /// Encoded key bytes are malformed or have trailing data.
    InvalidEncoding,
}

impl fmt::Display for DocumentKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySchema => formatter.write_str("document key schema has no fields"),
            Self::CountMismatch { expected, actual } => {
                write!(
                    formatter,
                    "document key expects {expected} values, got {actual}"
                )
            }
            Self::TypeMismatch { field, expected } => {
                write!(formatter, "key field {field} expects {expected}")
            }
            Self::TooLong { length, limit } => {
                write!(
                    formatter,
                    "document key is {length} bytes, limit is {limit}"
                )
            }
            Self::InvalidEncoding => formatter.write_str("document key encoding is invalid"),
        }
    }
}

impl std::error::Error for DocumentKeyError {}

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

    /// Encodes typed key values into canonical ordered bytes.
    ///
    /// Fixed-width components are big-endian and signed integers are biased by
    /// their sign bit. Strings and bytes are self-terminating, so composite keys
    /// stay unambiguous.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentKeyError`] for an empty schema, a value count or type
    /// mismatch, or a key longer than [`MAX_DOCUMENT_KEY_LEN`].
    pub fn encode(&self, values: &[KeyValue]) -> Result<Vec<u8>, DocumentKeyError> {
        if self.fields.is_empty() {
            return Err(DocumentKeyError::EmptySchema);
        }
        if self.fields.len() != values.len() {
            return Err(DocumentKeyError::CountMismatch {
                expected: self.fields.len(),
                actual: values.len(),
            });
        }

        let mut output = Vec::new();
        for (field, value) in self.fields.iter().zip(values) {
            match (field.ty(), value) {
                (KeyType::U64, KeyValue::U64(value)) => {
                    output.extend_from_slice(&value.to_be_bytes());
                }
                (KeyType::I64, KeyValue::I64(value)) => {
                    output.extend_from_slice(
                        &((*value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes(),
                    );
                }
                (KeyType::String, KeyValue::String(value)) => {
                    put_ordered_bytes(value.as_bytes(), &mut output);
                }
                (KeyType::Bytes, KeyValue::Bytes(value)) => {
                    put_ordered_bytes(value, &mut output);
                }
                (KeyType::Uuid, KeyValue::Uuid(value)) => {
                    output.extend_from_slice(value);
                }
                (expected, _) => {
                    return Err(DocumentKeyError::TypeMismatch {
                        field: field.name().to_owned(),
                        expected,
                    });
                }
            }
        }

        if output.len() > MAX_DOCUMENT_KEY_LEN {
            return Err(DocumentKeyError::TooLong {
                length: output.len(),
                limit: MAX_DOCUMENT_KEY_LEN,
            });
        }

        Ok(output)
    }

    /// Decodes canonical ordered bytes into typed key values.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentKeyError::InvalidEncoding`] for malformed or trailing bytes.
    pub fn decode(&self, bytes: &[u8]) -> Result<Vec<KeyValue>, DocumentKeyError> {
        let mut values = Vec::with_capacity(self.fields.len());
        let mut offset = 0;
        for field in &self.fields {
            let value = match field.ty() {
                KeyType::U64 => {
                    let raw = read_fixed::<8>(bytes, &mut offset)?;
                    KeyValue::U64(u64::from_be_bytes(raw))
                }
                KeyType::I64 => {
                    let raw = read_fixed::<8>(bytes, &mut offset)?;
                    KeyValue::I64((u64::from_be_bytes(raw) ^ 0x8000_0000_0000_0000) as i64)
                }
                KeyType::Uuid => KeyValue::Uuid(read_fixed::<16>(bytes, &mut offset)?),
                KeyType::String => {
                    let (raw, consumed) = get_ordered_bytes(&bytes[offset..])
                        .map_err(|_| DocumentKeyError::InvalidEncoding)?;
                    offset += consumed;
                    KeyValue::String(
                        String::from_utf8(raw).map_err(|_| DocumentKeyError::InvalidEncoding)?,
                    )
                }
                KeyType::Bytes => {
                    let (raw, consumed) = get_ordered_bytes(&bytes[offset..])
                        .map_err(|_| DocumentKeyError::InvalidEncoding)?;
                    offset += consumed;
                    KeyValue::Bytes(raw)
                }
            };
            values.push(value);
        }
        if offset != bytes.len() {
            return Err(DocumentKeyError::InvalidEncoding);
        }

        Ok(values)
    }
}

fn read_fixed<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], DocumentKeyError> {
    let end = offset
        .checked_add(N)
        .ok_or(DocumentKeyError::InvalidEncoding)?;
    let raw = bytes
        .get(*offset..end)
        .ok_or(DocumentKeyError::InvalidEncoding)?;
    *offset = end;

    Ok(raw.try_into().expect("fixed length"))
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

impl KeyValue {
    /// Compares two key values of the same type.
    #[must_use]
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::U64(left), Self::U64(right)) => Some(left.cmp(right)),
            (Self::I64(left), Self::I64(right)) => Some(left.cmp(right)),
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
            (Self::Bytes(left), Self::Bytes(right)) => Some(left.cmp(right)),
            (Self::Uuid(left), Self::Uuid(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}
