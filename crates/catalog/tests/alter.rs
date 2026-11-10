use recolith_catalog::{Catalog, CatalogAlter, CatalogError};
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};

fn schema(version: u32, fields: Vec<Field>) -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(version),
        "profile",
        fields,
    )
}

fn catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .register_schema(schema(
            1,
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
            ],
        ))
        .expect("schema v1");
    catalog
        .register_schema(schema(
            2,
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
                Field::new(FieldId::new(3), "nickname", Type::String, true),
            ],
        ))
        .expect("schema v2");
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");
    catalog
        .create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(1),
                "player_profile",
                16,
                BucketPolicy::Strict,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )
        .expect("bucket");
    catalog
        .create_collection(Collection::new(
            CollectionId::new(1),
            KeyspaceId::new(1),
            "player",
            "player",
            DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
            vec![Fragment::new(
                FragmentId::new(1),
                "profile",
                BucketId::new(1),
                FragmentCardinality::One,
            )],
        ))
        .expect("collection");
    catalog
        .create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(1),
                "by-level",
                IndexKind::Range,
                vec![FieldId::new(2)],
            ),
        )
        .expect("index");

    catalog
}

#[test]
fn switches_bucket_schema_version() {
    let catalog = catalog();
    let before = catalog.generation().get();

    let altered = catalog
        .alter_object(CatalogAlter::BucketSchema {
            bucket: BucketId::new(1),
            version: SchemaVersion::new(2),
        })
        .expect("alter schema");
    let snapshot = altered.snapshot().expect("snapshot");

    assert_eq!(
        snapshot.buckets[&BucketId::new(1)].schema_version(),
        SchemaVersion::new(2)
    );
    assert_eq!(altered.generation().get(), before + 1);
}

#[test]
fn redefines_index_and_rejects_invalid_changes() {
    let catalog = catalog();
    let altered = catalog
        .alter_object(CatalogAlter::Index {
            bucket: BucketId::new(1),
            index: IndexId::new(1),
            definition: IndexDefinition::new(
                IndexId::new(1),
                "by-name",
                IndexKind::Equality,
                vec![FieldId::new(1)],
            ),
        })
        .expect("redefine index");
    let snapshot = altered.snapshot().expect("snapshot");
    let index = &snapshot.indexes[&BucketId::new(1)][&IndexId::new(1)];
    assert_eq!(index.name(), "by-name");
    assert_eq!(index.fields(), &[FieldId::new(1)]);

    assert!(matches!(
        catalog.alter_object(CatalogAlter::BucketSchema {
            bucket: BucketId::new(1),
            version: SchemaVersion::new(9),
        }),
        Err(CatalogError::NotFound("schema"))
    ));
    assert!(matches!(
        catalog.alter_object(CatalogAlter::Index {
            bucket: BucketId::new(1),
            index: IndexId::new(9),
            definition: IndexDefinition::new(
                IndexId::new(9),
                "missing",
                IndexKind::Equality,
                vec![FieldId::new(1)],
            ),
        }),
        Err(CatalogError::NotFound("index"))
    ));
    assert!(matches!(
        catalog.alter_object(CatalogAlter::Index {
            bucket: BucketId::new(1),
            index: IndexId::new(1),
            definition: IndexDefinition::new(
                IndexId::new(1),
                "by-missing",
                IndexKind::Equality,
                vec![FieldId::new(9)],
            ),
        }),
        Err(CatalogError::InvalidIndex(_))
    ));
}

#[test]
fn rejects_duplicate_index_names() {
    let catalog = catalog();
    catalog
        .create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(2),
                "by-name",
                IndexKind::Equality,
                vec![FieldId::new(1)],
            ),
        )
        .expect("second index");

    assert!(matches!(
        catalog.alter_object(CatalogAlter::Index {
            bucket: BucketId::new(1),
            index: IndexId::new(1),
            definition: IndexDefinition::new(
                IndexId::new(1),
                "by-name",
                IndexKind::Equality,
                vec![FieldId::new(1)],
            ),
        }),
        Err(CatalogError::Duplicate("index name"))
    ));
}
