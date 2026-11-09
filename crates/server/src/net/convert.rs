use std::fmt;

use recolith_common::{FieldId, Timestamp};
use recolith_protocol::messages::{self, FragmentResult, value};
use recolith_storage::{DocumentFragments, RecordData, Value};

/// Wire value conversion error.
#[derive(Debug)]
pub(crate) struct ConvertError(String);

impl fmt::Display for ConvertError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ConvertError {}

fn invalid(message: impl Into<String>) -> ConvertError {
    ConvertError(message.into())
}

pub(crate) fn value_to_wire(value: &Value) -> messages::Value {
    let kind = match value {
        Value::Bool(value) => value::Kind::BoolValue(*value),
        Value::I32(value) => value::Kind::I32Value(*value),
        Value::I64(value) => value::Kind::I64Value(*value),
        Value::U32(value) => value::Kind::U32Value(*value),
        Value::U64(value) => value::Kind::U64Value(*value),
        Value::F32(value) => value::Kind::F32Value(*value),
        Value::F64(value) => value::Kind::F64Value(*value),
        Value::String(value) => value::Kind::StringValue(value.clone()),
        Value::Bytes(value) => value::Kind::BytesValue(value.clone()),
        Value::Timestamp(value) => value::Kind::TimestampMicros(value.as_micros()),
        Value::Duration(value) => value::Kind::DurationMicros(*value),
        Value::Enum(value) => value::Kind::EnumValue(*value),
        Value::List(values) => value::Kind::ListValue(messages::ListValue {
            values: values.iter().map(value_to_wire).collect(),
        }),
        Value::Map(entries) => value::Kind::MapValue(messages::MapValue {
            entries: entries
                .iter()
                .map(|(key, value)| messages::MapEntry {
                    key: Some(value_to_wire(key)),
                    value: Some(value_to_wire(value)),
                })
                .collect(),
        }),
        Value::Record(data) => value::Kind::RecordValue(messages::RecordValue {
            fields: record_to_wire(data),
        }),
    };

    messages::Value { kind: Some(kind) }
}

pub(crate) fn value_from_wire(value: &messages::Value) -> Result<Value, ConvertError> {
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| invalid("value has no kind"))?;

    match kind {
        value::Kind::BoolValue(value) => Ok(Value::Bool(*value)),
        value::Kind::I32Value(value) => Ok(Value::I32(*value)),
        value::Kind::I64Value(value) => Ok(Value::I64(*value)),
        value::Kind::U32Value(value) => Ok(Value::U32(*value)),
        value::Kind::U64Value(value) => Ok(Value::U64(*value)),
        value::Kind::F32Value(value) => Ok(Value::F32(*value)),
        value::Kind::F64Value(value) => Ok(Value::F64(*value)),
        value::Kind::StringValue(value) => Ok(Value::String(value.clone())),
        value::Kind::BytesValue(value) => Ok(Value::Bytes(value.clone())),
        value::Kind::TimestampMicros(value) => Ok(Value::Timestamp(Timestamp::from_micros(*value))),
        value::Kind::DurationMicros(value) => Ok(Value::Duration(*value)),
        value::Kind::EnumValue(value) => Ok(Value::Enum(*value)),
        value::Kind::ListValue(list) => Ok(Value::List(
            list.values
                .iter()
                .map(value_from_wire)
                .collect::<Result<_, _>>()?,
        )),
        value::Kind::MapValue(map) => {
            let mut entries = Vec::with_capacity(map.entries.len());
            for entry in &map.entries {
                let key = entry
                    .key
                    .as_ref()
                    .ok_or_else(|| invalid("map entry has no key"))?;
                let value = entry
                    .value
                    .as_ref()
                    .ok_or_else(|| invalid("map entry has no value"))?;
                entries.push((value_from_wire(key)?, value_from_wire(value)?));
            }
            Ok(Value::Map(entries))
        }
        value::Kind::RecordValue(record) => {
            Ok(Value::Record(Box::new(record_from_wire(&record.fields)?)))
        }
    }
}

pub(crate) fn record_to_wire(data: &RecordData) -> Vec<messages::FieldValue> {
    data.values()
        .iter()
        .map(|(field, value)| messages::FieldValue {
            field_id: field.get(),
            value: Some(value_to_wire(value)),
        })
        .collect()
}

pub(crate) fn record_from_wire(
    fields: &[messages::FieldValue],
) -> Result<RecordData, ConvertError> {
    let mut data = RecordData::new();
    for field in fields {
        let value = field
            .value
            .as_ref()
            .ok_or_else(|| invalid(format!("field {} has no value", field.field_id)))?;
        let field_id = FieldId::new(field.field_id);
        if data.insert(field_id, value_from_wire(value)?).is_some() {
            return Err(invalid(format!("duplicate field {}", field.field_id)));
        }
    }

    Ok(data)
}

pub(crate) fn fragments_to_wire(fragments: &DocumentFragments) -> Vec<FragmentResult> {
    fragments
        .iter()
        .flat_map(|(fragment_id, records)| {
            records.iter().map(|record| FragmentResult {
                fragment_id: fragment_id.get(),
                subkey: record.subkey().to_vec(),
                commit_ts: record.header().commit_ts().get(),
                fields: record_to_wire(record.data()),
            })
        })
        .collect()
}
