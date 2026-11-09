use recolith_protocol::messages::{DocumentResult, FragmentResult, Value, value};

use crate::hex;

/// Formats one query document.
#[must_use]
pub fn format_document(document: &DocumentResult) -> String {
    let mut output = format!("document 0x{}\n", hex::encode(&document.document_key));
    for fragment in &document.fragments {
        format_fragment(&mut output, fragment);
    }

    output
}

/// Formats a dynamic value.
#[must_use]
pub fn format_value(value: &Value) -> String {
    match &value.kind {
        None => "null".to_owned(),
        Some(value::Kind::BoolValue(value)) => value.to_string(),
        Some(value::Kind::I32Value(value)) => value.to_string(),
        Some(value::Kind::I64Value(value)) => value.to_string(),
        Some(value::Kind::U32Value(value)) => value.to_string(),
        Some(value::Kind::U64Value(value)) => value.to_string(),
        Some(value::Kind::F32Value(value)) => value.to_string(),
        Some(value::Kind::F64Value(value)) => value.to_string(),
        Some(value::Kind::StringValue(value)) => format!("{value:?}"),
        Some(value::Kind::BytesValue(value)) => format!("0x{}", hex::encode(value)),
        Some(value::Kind::TimestampMicros(value)) => format!("{value}us"),
        Some(value::Kind::DurationMicros(value)) => format!("{value}us"),
        Some(value::Kind::EnumValue(value)) => format!("enum({value})"),
        Some(value::Kind::ListValue(list)) => {
            let values = list
                .values
                .iter()
                .map(format_value)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{values}]")
        }
        Some(value::Kind::MapValue(map)) => {
            let entries = map
                .entries
                .iter()
                .map(|entry| {
                    let key = entry
                        .key
                        .as_ref()
                        .map_or_else(|| "null".to_owned(), format_value);
                    let value = entry
                        .value
                        .as_ref()
                        .map_or_else(|| "null".to_owned(), format_value);
                    format!("{key}: {value}")
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{entries}}}")
        }
        Some(value::Kind::RecordValue(record)) => {
            let fields = record
                .fields
                .iter()
                .map(|field| {
                    let value = field
                        .value
                        .as_ref()
                        .map_or_else(|| "null".to_owned(), format_value);
                    format!("{}: {value}", field.field_id)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{fields}}}")
        }
    }
}

fn format_fragment(output: &mut String, fragment: &FragmentResult) {
    let subkey = if fragment.subkey.is_empty() {
        String::new()
    } else {
        format!(" subkey=0x{}", hex::encode(&fragment.subkey))
    };
    output.push_str(&format!(
        "  fragment {} commit={}{}\n",
        fragment.fragment_id, fragment.commit_ts, subkey
    ));
    for field in &fragment.fields {
        let value = field
            .value
            .as_ref()
            .map_or_else(|| "null".to_owned(), format_value);
        output.push_str(&format!("    field {} = {value}\n", field.field_id));
    }
}
