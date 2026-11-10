use recolith_protocol::messages::{
    CollectionMetadata, DocumentResult, FragmentCardinality, FragmentMetadata, FragmentResult,
    SchemaScalarType, SchemaType, Value, schema_type, value,
};

use crate::hex;

/// Formats one query document.
#[must_use]
pub fn format_document(document: &DocumentResult) -> String {
    format_fragments(&document.document_key, &document.fragments)
}

/// Formats a document key and its fragments.
#[must_use]
pub fn format_fragments(document_key: &[u8], fragments: &[FragmentResult]) -> String {
    let mut output = format!("document 0x{}\n", hex::encode(document_key));
    for fragment in fragments {
        format_fragment(&mut output, fragment, None);
    }

    output
}

/// Formats a document key and fragments using collection names.
#[must_use]
pub fn format_fragments_named(
    document_key: &[u8],
    fragments: &[FragmentResult],
    collection: &CollectionMetadata,
) -> String {
    let mut output = format!("document 0x{}\n", hex::encode(document_key));
    for fragment in fragments {
        let metadata = collection
            .fragments
            .iter()
            .find(|candidate| candidate.id == fragment.fragment_id);
        format_fragment(&mut output, fragment, metadata);
    }

    output
}

/// Formats collection metadata.
#[must_use]
pub fn format_collection(collection: &CollectionMetadata) -> String {
    let mut output = format!(
        "collection {}.{} (id={}, affinity={})\n",
        collection.keyspace, collection.name, collection.id, collection.affinity_group
    );
    output.push_str("key:\n");
    for field in &collection.key_fields {
        output.push_str(&format!("  {}: {}\n", field.name, key_type(field.r#type)));
    }
    output.push_str("fragments:\n");
    for fragment in &collection.fragments {
        output.push_str(&format!(
            "  {} (id={}, {}, schema={}/{}):\n",
            fragment.name,
            fragment.id,
            cardinality(fragment.cardinality),
            fragment.schema_id,
            fragment.schema_version
        ));
        for field in &fragment.fields {
            let type_name = field
                .r#type
                .as_ref()
                .map_or_else(|| "unknown".to_owned(), format_schema_type);
            output.push_str(&format!(
                "    {} (id={}){}: {}\n",
                field.name,
                field.id,
                if field.optional { " optional" } else { "" },
                type_name
            ));
        }
    }
    if !collection.indexes.is_empty() {
        output.push_str("indexes:\n");
        for index in &collection.indexes {
            let fields = index
                .fields
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&format!(
                "  {} (id={}, bucket={}, {}): [{}]\n",
                index.name,
                index.id,
                index.bucket_id,
                index_kind(index.kind),
                fields
            ));
        }
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

fn format_fragment(
    output: &mut String,
    fragment: &FragmentResult,
    metadata: Option<&FragmentMetadata>,
) {
    let subkey = if fragment.subkey.is_empty() {
        String::new()
    } else {
        format!(" subkey=0x{}", hex::encode(&fragment.subkey))
    };
    let label = metadata.map_or_else(
        || fragment.fragment_id.to_string(),
        |metadata| format!("{} ({})", metadata.name, fragment.fragment_id),
    );
    output.push_str(&format!(
        "  fragment {label} commit={}{}\n",
        fragment.commit_ts, subkey
    ));
    for field in &fragment.fields {
        let value = field
            .value
            .as_ref()
            .map_or_else(|| "null".to_owned(), format_value);
        let label = metadata
            .and_then(|metadata| {
                metadata
                    .fields
                    .iter()
                    .find(|candidate| candidate.id == field.field_id)
            })
            .map_or_else(
                || field.field_id.to_string(),
                |field| format!("{} ({})", field.name, field.id),
            );
        output.push_str(&format!("    {label} = {value}\n"));
    }
}

fn format_schema_type(schema_type: &SchemaType) -> String {
    match schema_type.kind.as_ref() {
        None => "unknown".to_owned(),
        Some(schema_type::Kind::Scalar(scalar)) => SchemaScalarType::try_from(*scalar).map_or_else(
            |_| "unknown".to_owned(),
            |scalar| scalar_name(scalar).to_owned(),
        ),
        Some(schema_type::Kind::List(list)) => {
            let element = list.element.as_ref().map_or_else(
                || "unknown".to_owned(),
                |element| format_schema_type(element),
            );
            format!("list<{element}>")
        }
        Some(schema_type::Kind::Map(map)) => {
            let key = map
                .key
                .as_ref()
                .map_or_else(|| "unknown".to_owned(), |key| format_schema_type(key));
            let value = map
                .value
                .as_ref()
                .map_or_else(|| "unknown".to_owned(), |value| format_schema_type(value));
            format!("map<{key}, {value}>")
        }
        Some(schema_type::Kind::EnumType(schema)) => {
            format!("enum {} {{{}}}", schema.name, schema.variants.join(", "))
        }
        Some(schema_type::Kind::Record(record)) => format!("record {}", record.name),
    }
}

const fn scalar_name(scalar: SchemaScalarType) -> &'static str {
    match scalar {
        SchemaScalarType::Bool => "bool",
        SchemaScalarType::I32 => "i32",
        SchemaScalarType::I64 => "i64",
        SchemaScalarType::U32 => "u32",
        SchemaScalarType::U64 => "u64",
        SchemaScalarType::F32 => "f32",
        SchemaScalarType::F64 => "f64",
        SchemaScalarType::String => "string",
        SchemaScalarType::Bytes => "bytes",
        SchemaScalarType::Timestamp => "timestamp",
        SchemaScalarType::Duration => "duration",
        SchemaScalarType::Unspecified => "unspecified",
    }
}

fn key_type(field_type: i32) -> String {
    recolith_protocol::messages::KeyFieldType::try_from(field_type).map_or_else(
        |_| "unknown".to_owned(),
        |field_type| match field_type {
            recolith_protocol::messages::KeyFieldType::U64 => "u64".to_owned(),
            recolith_protocol::messages::KeyFieldType::I64 => "i64".to_owned(),
            recolith_protocol::messages::KeyFieldType::String => "string".to_owned(),
            recolith_protocol::messages::KeyFieldType::Bytes => "bytes".to_owned(),
            recolith_protocol::messages::KeyFieldType::Uuid => "uuid".to_owned(),
            recolith_protocol::messages::KeyFieldType::Unspecified => "unspecified".to_owned(),
        },
    )
}

fn cardinality(cardinality: i32) -> &'static str {
    match FragmentCardinality::try_from(cardinality) {
        Ok(FragmentCardinality::One) => "one",
        Ok(FragmentCardinality::Many) => "many",
        Ok(FragmentCardinality::Unspecified) | Err(_) => "unknown",
    }
}

fn index_kind(kind: i32) -> &'static str {
    match recolith_protocol::messages::IndexKind::try_from(kind) {
        Ok(recolith_protocol::messages::IndexKind::Equality) => "equality",
        Ok(recolith_protocol::messages::IndexKind::Range) => "range",
        Ok(recolith_protocol::messages::IndexKind::Composite) => "composite",
        Ok(recolith_protocol::messages::IndexKind::Unique) => "unique",
        Ok(recolith_protocol::messages::IndexKind::Unspecified) | Err(_) => "unknown",
    }
}
