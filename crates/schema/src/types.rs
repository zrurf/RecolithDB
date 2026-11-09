use std::fmt;

use crate::record::RecordSchema;

/// Physical encoding category for a schema type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireType {
    /// Unsigned LEB128 varint.
    Varint,
    /// 64-bit signed integer.
    I64,
    /// 32-bit signed integer.
    I32,
    /// 64-bit float.
    F64,
    /// 32-bit float.
    F32,
    /// Length-prefixed bytes.
    Bytes,
    /// Length-prefixed UTF-8 string.
    String,
    /// Length-prefixed sequence.
    List,
    /// Length-prefixed key/value sequence.
    Map,
    /// Nested field entry sequence.
    Nested,
}

/// Named enum with ordered variants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumSchema {
    name: String,
    variants: Vec<String>,
}

impl EnumSchema {
    /// Creates an enum schema.
    #[must_use]
    pub fn new(name: impl Into<String>, variants: Vec<String>) -> Self {
        Self {
            name: name.into(),
            variants,
        }
    }

    /// Returns the enum name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the ordered variants.
    #[must_use]
    pub fn variants(&self) -> &[String] {
        &self.variants
    }
}

/// Schema field type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Type {
    /// Boolean.
    Bool,
    /// Signed 32-bit integer.
    I32,
    /// Signed 64-bit integer.
    I64,
    /// Unsigned 32-bit integer.
    U32,
    /// Unsigned 64-bit integer.
    U64,
    /// 32-bit float.
    F32,
    /// 64-bit float.
    F64,
    /// UTF-8 string.
    String,
    /// Opaque bytes.
    Bytes,
    /// UTC timestamp.
    Timestamp,
    /// Duration.
    Duration,
    /// Enum.
    Enum(EnumSchema),
    /// Ordered list.
    List(Box<Type>),
    /// Ordered map.
    Map(Box<Type>, Box<Type>),
    /// Nested record.
    Record(Box<RecordSchema>),
}

impl Type {
    /// Returns the physical wire type.
    #[must_use]
    pub const fn wire_type(&self) -> WireType {
        match self {
            Self::Bool | Self::U32 | Self::U64 | Self::Timestamp | Self::Duration => {
                WireType::Varint
            }
            Self::I32 => WireType::I32,
            Self::I64 => WireType::I64,
            Self::F32 => WireType::F32,
            Self::F64 => WireType::F64,
            Self::String => WireType::String,
            Self::Bytes => WireType::Bytes,
            Self::Enum(_) => WireType::Varint,
            Self::List(_) => WireType::List,
            Self::Map(_, _) => WireType::Map,
            Self::Record(_) => WireType::Nested,
        }
    }

    /// Returns whether this type is accepted as a map key.
    #[must_use]
    pub const fn is_valid_map_key(&self) -> bool {
        matches!(
            self,
            Self::Bool | Self::I32 | Self::I64 | Self::U32 | Self::U64 | Self::String | Self::Bytes
        )
    }
}

impl fmt::Display for Type {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool => formatter.write_str("bool"),
            Self::I32 => formatter.write_str("i32"),
            Self::I64 => formatter.write_str("i64"),
            Self::U32 => formatter.write_str("u32"),
            Self::U64 => formatter.write_str("u64"),
            Self::F32 => formatter.write_str("f32"),
            Self::F64 => formatter.write_str("f64"),
            Self::String => formatter.write_str("string"),
            Self::Bytes => formatter.write_str("bytes"),
            Self::Timestamp => formatter.write_str("timestamp"),
            Self::Duration => formatter.write_str("duration"),
            Self::Enum(schema) => write!(formatter, "enum {}", schema.name()),
            Self::List(inner) => write!(formatter, "list<{inner}>"),
            Self::Map(key, value) => write!(formatter, "map<{key}, {value}>"),
            Self::Record(schema) => write!(formatter, "record {}", schema.name()),
        }
    }
}
