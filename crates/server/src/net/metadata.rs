use std::collections::BTreeMap;

use recolith_catalog::CatalogSnapshot;
use recolith_common::BucketId;
use recolith_model::{Collection, FragmentCardinality, IndexDefinition, IndexKind};
use recolith_protocol::messages::{
    self, CollectionMetadata, FragmentMetadata, IndexMetadata, KeyFieldMetadata, KeyFieldType,
    SchemaEnum, SchemaField, SchemaList, SchemaMap, SchemaRecord, SchemaScalarType, SchemaType,
    schema_type,
};
use recolith_schema::{Field, RecordSchema, Type};

use super::operations::OperationError;

pub(crate) fn collection_metadata(
    snapshot: &CatalogSnapshot,
    collection: &Collection,
) -> Result<CollectionMetadata, OperationError> {
    let keyspace = snapshot
        .keyspaces
        .get(&collection.keyspace_id())
        .ok_or_else(|| OperationError::internal("collection references missing keyspace"))?;

    let key_fields = collection
        .key_schema()
        .fields()
        .iter()
        .map(|field| KeyFieldMetadata {
            name: field.name().to_owned(),
            r#type: key_field_type(field.ty()) as i32,
        })
        .collect();

    let mut fragments = Vec::with_capacity(collection.fragments().len());
    let mut indexes = BTreeMap::new();
    for fragment in collection.fragments() {
        let bucket = snapshot
            .buckets
            .get(&fragment.bucket_id())
            .ok_or_else(|| OperationError::internal("fragment references missing bucket"))?;
        let schema = snapshot
            .schemas
            .schemas
            .get(&(bucket.schema_id(), bucket.schema_version()))
            .ok_or_else(|| OperationError::internal("bucket references missing schema"))?;
        fragments.push(FragmentMetadata {
            id: fragment.id().get(),
            name: fragment.name().to_owned(),
            cardinality: fragment_cardinality(fragment.cardinality()) as i32,
            bucket_id: fragment.bucket_id().get(),
            schema_id: bucket.schema_id().get(),
            schema_version: bucket.schema_version().get(),
            fields: schema_fields(schema)?,
        });

        if let Some(bucket_indexes) = snapshot.indexes.get(&fragment.bucket_id()) {
            for index in bucket_indexes.values() {
                indexes.insert(index.id(), index_metadata(fragment.bucket_id(), index));
            }
        }
    }

    Ok(CollectionMetadata {
        id: collection.id().get(),
        keyspace_id: collection.keyspace_id().get(),
        keyspace: keyspace.name().to_owned(),
        name: collection.name().to_owned(),
        affinity_group: collection.affinity_group().to_owned(),
        key_fields,
        fragments,
        indexes: indexes.into_values().collect(),
    })
}

fn schema_fields(schema: &RecordSchema) -> Result<Vec<SchemaField>, OperationError> {
    schema
        .fields()
        .iter()
        .map(schema_field)
        .collect::<Result<Vec<_>, _>>()
}

fn schema_field(field: &Field) -> Result<SchemaField, OperationError> {
    Ok(SchemaField {
        id: field.id().get(),
        name: field.name().to_owned(),
        optional: field.optional(),
        r#type: Some(schema_type(field.ty())?),
    })
}

fn schema_type(ty: &Type) -> Result<SchemaType, OperationError> {
    let kind = match ty {
        Type::Bool => schema_type::Kind::Scalar(SchemaScalarType::Bool as i32),
        Type::I32 => schema_type::Kind::Scalar(SchemaScalarType::I32 as i32),
        Type::I64 => schema_type::Kind::Scalar(SchemaScalarType::I64 as i32),
        Type::U32 => schema_type::Kind::Scalar(SchemaScalarType::U32 as i32),
        Type::U64 => schema_type::Kind::Scalar(SchemaScalarType::U64 as i32),
        Type::F32 => schema_type::Kind::Scalar(SchemaScalarType::F32 as i32),
        Type::F64 => schema_type::Kind::Scalar(SchemaScalarType::F64 as i32),
        Type::String => schema_type::Kind::Scalar(SchemaScalarType::String as i32),
        Type::Bytes => schema_type::Kind::Scalar(SchemaScalarType::Bytes as i32),
        Type::Timestamp => schema_type::Kind::Scalar(SchemaScalarType::Timestamp as i32),
        Type::Duration => schema_type::Kind::Scalar(SchemaScalarType::Duration as i32),
        Type::Enum(schema) => schema_type::Kind::EnumType(SchemaEnum {
            name: schema.name().to_owned(),
            variants: schema.variants().to_vec(),
        }),
        Type::List(element) => schema_type::Kind::List(Box::new(SchemaList {
            element: Some(Box::new(schema_type(element)?)),
        })),
        Type::Map(key, value) => schema_type::Kind::Map(Box::new(SchemaMap {
            key: Some(Box::new(schema_type(key)?)),
            value: Some(Box::new(schema_type(value)?)),
        })),
        Type::Record(record) => schema_type::Kind::Record(SchemaRecord {
            name: record.name().to_owned(),
            fields: schema_fields(record)?,
        }),
    };

    Ok(SchemaType { kind: Some(kind) })
}

fn index_metadata(bucket_id: BucketId, index: &IndexDefinition) -> IndexMetadata {
    IndexMetadata {
        id: index.id().get(),
        bucket_id: bucket_id.get(),
        name: index.name().to_owned(),
        kind: index_kind(index.kind()) as i32,
        fields: index.fields().iter().map(|field| field.get()).collect(),
    }
}

const fn key_field_type(ty: recolith_model::KeyType) -> KeyFieldType {
    match ty {
        recolith_model::KeyType::U64 => KeyFieldType::U64,
        recolith_model::KeyType::I64 => KeyFieldType::I64,
        recolith_model::KeyType::String => KeyFieldType::String,
        recolith_model::KeyType::Bytes => KeyFieldType::Bytes,
        recolith_model::KeyType::Uuid => KeyFieldType::Uuid,
    }
}

const fn fragment_cardinality(cardinality: FragmentCardinality) -> messages::FragmentCardinality {
    match cardinality {
        FragmentCardinality::One => messages::FragmentCardinality::One,
        FragmentCardinality::Many => messages::FragmentCardinality::Many,
    }
}

const fn index_kind(kind: IndexKind) -> messages::IndexKind {
    match kind {
        IndexKind::Equality => messages::IndexKind::Equality,
        IndexKind::Range => messages::IndexKind::Range,
        IndexKind::Composite => messages::IndexKind::Composite,
        IndexKind::Unique => messages::IndexKind::Unique,
    }
}
