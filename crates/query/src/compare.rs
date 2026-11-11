use std::cmp::Ordering;

use recolith_storage::Value;

pub(crate) fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Bool(left), Value::Bool(right)) => left.partial_cmp(right),
        (Value::I32(left), Value::I32(right)) => left.partial_cmp(right),
        (Value::I64(left), Value::I64(right)) => left.partial_cmp(right),
        (Value::U32(left), Value::U32(right)) => left.partial_cmp(right),
        (Value::U64(left), Value::U64(right)) => left.partial_cmp(right),
        (Value::F32(left), Value::F32(right)) => left.partial_cmp(right),
        (Value::F64(left), Value::F64(right)) => left.partial_cmp(right),
        (Value::String(left), Value::String(right)) => left.partial_cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.partial_cmp(right),
        (Value::Timestamp(left), Value::Timestamp(right)) => {
            left.as_micros().partial_cmp(&right.as_micros())
        }
        (Value::Duration(left), Value::Duration(right)) => left.partial_cmp(right),
        (Value::Enum(left), Value::Enum(right)) => left.partial_cmp(right),
        _ => None,
    }
}
