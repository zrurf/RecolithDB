use recolith_protocol::messages::{
    AggregateColumn, AggregateFunction, AggregateResponse, CollectionMetadata, DocumentResult,
    ExplainFilterOp, ExplainResponse, FragmentCardinality, FragmentMetadata, FragmentResult,
    IndexKind, SchemaScalarType, SchemaType, Value, explain_response, schema_type, value,
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

/// Formats a query plan explanation.
#[must_use]
pub fn format_explain(response: &ExplainResponse) -> String {
    let mut output = format!("catalog generation: {}\n", response.catalog_generation);
    match &response.plan {
        Some(explain_response::Plan::Document(plan)) => {
            output.push_str("plan: document\n");
            output.push_str(&format!("collection: {}\n", plan.collection_id));
            output.push_str(&format!("partition: {}\n", plan.partition));
            output.push_str(&format!(
                "document key: 0x{}\n",
                hex::encode(&plan.document_key)
            ));
            output.push_str(&format!("fragments: {}\n", join_ids(&plan.fragment_ids)));
            append_explain_fields(&mut output, &plan.projections, &plan.filters);
        }
        Some(explain_response::Plan::Scan(plan)) => {
            output.push_str("plan: scan\n");
            output.push_str(&format!("collection: {}\n", plan.collection_id));
            output.push_str(&format!("partition: {}\n", plan.partition));
            output.push_str(&format!("fragments: {}\n", join_ids(&plan.fragment_ids)));
            output.push_str(&format!("candidate limit: {}\n", plan.candidate_limit));
            output.push_str("drives:\n");
            for drive in &plan.drives {
                if drive.bucket_scan {
                    output.push_str(&format!(
                        "  bucket {} fragment {} bucket scan\n",
                        drive.bucket_id, drive.fragment_id
                    ));
                } else {
                    let indexes = drive
                        .indexes
                        .iter()
                        .map(|index| {
                            format!(
                                "{}[fields {} kind {}]",
                                index.index_id,
                                join_ids(&index.field_ids),
                                explain_index_kind(index.kind)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    output.push_str(&format!(
                        "  bucket {} fragment {} indexes: {}\n",
                        drive.bucket_id, drive.fragment_id, indexes
                    ));
                }
            }
            if !plan.key_filters.is_empty() {
                output.push_str("key filters:\n");
                for filter in &plan.key_filters {
                    output.push_str(&format!(
                        "  key[{}] {}\n",
                        filter.field,
                        explain_op(filter.op)
                    ));
                }
            }
            append_explain_fields(&mut output, &plan.projections, &plan.filters);
            if let Some(order) = &plan.order {
                let mode = order
                    .index_id
                    .map_or_else(|| "sorted".to_owned(), |index| format!("index {index}"));
                output.push_str(&format!(
                    "order: fragment {} field {} {} ({})\n",
                    order.fragment_id,
                    order.field_id,
                    if order.descending { "DESC" } else { "ASC" },
                    mode
                ));
            }
        }
        None => output.push_str("plan: none\n"),
    }

    output
}

fn append_explain_fields(
    output: &mut String,
    projections: &[recolith_protocol::messages::ExplainFieldSelection],
    filters: &[recolith_protocol::messages::ExplainFilter],
) {
    if !projections.is_empty() {
        output.push_str("projection:\n");
        for selection in projections {
            output.push_str(&format!(
                "  fragment {} field {}\n",
                selection.fragment_id, selection.field_id
            ));
        }
    }
    if !filters.is_empty() {
        output.push_str("filters:\n");
        for filter in filters {
            output.push_str(&format!(
                "  fragment {} field {} {}\n",
                filter.fragment_id,
                filter.field_id,
                explain_op(filter.op)
            ));
        }
    }
}

fn join_ids(ids: &[u32]) -> String {
    if ids.is_empty() {
        "-".to_owned()
    } else {
        ids.iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn explain_op(op: i32) -> &'static str {
    match ExplainFilterOp::try_from(op) {
        Ok(ExplainFilterOp::Eq) => "=",
        Ok(ExplainFilterOp::Ne) => "!=",
        Ok(ExplainFilterOp::Lt) => "<",
        Ok(ExplainFilterOp::Le) => "<=",
        Ok(ExplainFilterOp::Gt) => ">",
        Ok(ExplainFilterOp::Ge) => ">=",
        Ok(ExplainFilterOp::Unspecified) | Err(_) => "?",
    }
}

fn explain_index_kind(kind: i32) -> &'static str {
    match IndexKind::try_from(kind) {
        Ok(IndexKind::Equality) => "equality",
        Ok(IndexKind::Range) => "range",
        Ok(IndexKind::Composite) => "composite",
        Ok(IndexKind::Unique) => "unique",
        Ok(IndexKind::Unspecified) | Err(_) => "unknown",
    }
}

/// Formats aggregate query results.
#[must_use]
pub fn format_aggregate(response: &AggregateResponse) -> String {
    let mut output = format!("catalog generation: {}\n", response.catalog_generation);
    if let Some(fragment_id) = response.group_fragment_id {
        output.push_str(&format!(
            "group: fragment {} field {}\n",
            fragment_id,
            response.group_field_id.unwrap_or(0)
        ));
    }
    output.push_str(&format!(
        "rows: {} (truncated: {})\n",
        response.rows.len(),
        response.truncated
    ));
    for (index, row) in response.rows.iter().enumerate() {
        output.push_str(&format!("row {index}:\n"));
        if let Some(group) = &row.group {
            output.push_str(&format!("  group = {}\n", format_value(group)));
        }
        for (column, value) in response.columns.iter().zip(&row.values) {
            output.push_str(&format!(
                "  {} = {}\n",
                aggregate_label(column),
                format_value(value)
            ));
        }
    }

    output
}

fn aggregate_label(column: &AggregateColumn) -> String {
    let function = match AggregateFunction::try_from(column.function) {
        Ok(AggregateFunction::Count) => "count(*)".to_owned(),
        Ok(AggregateFunction::Sum) => "sum".to_owned(),
        Ok(AggregateFunction::Min) => "min".to_owned(),
        Ok(AggregateFunction::Max) => "max".to_owned(),
        Ok(AggregateFunction::Avg) => "avg".to_owned(),
        Ok(AggregateFunction::Unspecified) | Err(_) => "aggregate".to_owned(),
    };
    match (column.fragment_id, column.field_id) {
        (Some(fragment_id), Some(field_id))
            if AggregateFunction::try_from(column.function) != Ok(AggregateFunction::Count) =>
        {
            format!("{function}(fragment {fragment_id} field {field_id})")
        }
        _ => function,
    }
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
