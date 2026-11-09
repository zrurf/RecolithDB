use std::collections::BTreeMap;

use recolith_common::{FieldId, Timestamp};

/// Schema-aware field value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Boolean.
    Bool(bool),
    /// Signed 32-bit integer.
    I32(i32),
    /// Signed 64-bit integer.
    I64(i64),
    /// Unsigned 32-bit integer.
    U32(u32),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// 32-bit float.
    F32(f32),
    /// 64-bit float.
    F64(f64),
    /// UTF-8 string.
    String(String),
    /// Opaque bytes.
    Bytes(Vec<u8>),
    /// UTC timestamp.
    Timestamp(Timestamp),
    /// Duration in microseconds.
    Duration(u64),
    /// Enum variant index.
    Enum(u32),
    /// Ordered list.
    List(Vec<Value>),
    /// Ordered map.
    Map(Vec<(Value, Value)>),
    /// Nested record.
    Record(Box<RecordData>),
}

/// Decoded record fields plus preserved unknown field entries.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordData {
    values: BTreeMap<FieldId, Value>,
    unknown: BTreeMap<FieldId, Vec<u8>>,
}

impl RecordData {
    /// Creates empty record data.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a known field value.
    pub fn insert(&mut self, field: FieldId, value: Value) -> Option<Value> {
        self.values.insert(field, value)
    }

    /// Returns a known field value.
    #[must_use]
    pub fn get(&self, field: FieldId) -> Option<&Value> {
        self.values.get(&field)
    }

    /// Removes a known field value.
    pub fn remove(&mut self, field: FieldId) -> Option<Value> {
        self.values.remove(&field)
    }

    /// Returns all known field values.
    #[must_use]
    pub fn values(&self) -> &BTreeMap<FieldId, Value> {
        &self.values
    }

    /// Returns preserved unknown field entries.
    #[must_use]
    pub fn unknown(&self) -> &BTreeMap<FieldId, Vec<u8>> {
        &self.unknown
    }

    /// Preserves an unknown field entry exactly as encoded.
    pub fn insert_unknown(&mut self, field: FieldId, entry: Vec<u8>) -> Option<Vec<u8>> {
        self.unknown.insert(field, entry)
    }
}
