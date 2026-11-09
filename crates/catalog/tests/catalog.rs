use recolith_common::{
    BackendId, BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId,
    SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};

use recolith_catalog::{BucketPlacement, Catalog, CatalogError};

fn populated_catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .register_schema(schema(
            1,
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
            ],
        ))
        .expect("schema");
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
            key_schema(),
            vec![Fragment::new(
                FragmentId::new(1),
                "profile",
                BucketId::new(1),
                FragmentCardinality::One,
            )],
        ))
        .expect("collection");

    catalog
}

fn key_schema() -> DocumentKeySchema {
    DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
}

fn schema(version: u32, fields: Vec<Field>) -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(version),
        "player",
        fields,
    )
}

#[test]
fn creates_metadata_and_tracks_generation() {
    let catalog = populated_catalog();

    assert_eq!(catalog.generation().get(), 4);
    let snapshot = catalog.snapshot().expect("snapshot");
    assert_eq!(snapshot.generation.get(), 4);
    assert_eq!(snapshot.keyspaces.len(), 1);
    assert_eq!(snapshot.buckets.len(), 1);
    assert_eq!(snapshot.collections.len(), 1);
}

#[test]
fn records_bucket_placement() {
    let catalog = populated_catalog();
    let placement = BucketPlacement::new(BucketId::new(1), BackendId::new(1), "player", 16);

    catalog.place_bucket(placement.clone()).expect("placement");

    assert_eq!(catalog.generation().get(), 5);
    assert_eq!(
        catalog.placement(BucketId::new(1)).expect("lookup"),
        Some(placement)
    );
}

#[test]
fn rejects_mismatched_partition_count() {
    let catalog = populated_catalog();
    let placement = BucketPlacement::new(BucketId::new(1), BackendId::new(1), "player", 32);

    assert!(matches!(
        catalog.place_bucket(placement),
        Err(CatalogError::InvalidPlacement(_))
    ));
    assert_eq!(catalog.generation().get(), 4);
}

#[test]
fn registers_compatible_schema_versions() {
    let catalog = Catalog::new();
    let first = schema(
        1,
        vec![Field::new(FieldId::new(1), "name", Type::String, false)],
    );
    let second = schema(
        2,
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, true),
        ],
    );

    catalog.register_schema(first).expect("schema v1");
    catalog.register_schema(second).expect("schema v2");

    assert_eq!(catalog.generation().get(), 2);
    let latest = catalog
        .latest_schema(SchemaId::new(1))
        .expect("latest")
        .expect("schema");
    assert_eq!(latest.version(), SchemaVersion::new(2));
}

#[test]
fn rejects_migration_requiring_schema_change() {
    let catalog = Catalog::new();
    catalog
        .register_schema(schema(
            1,
            vec![Field::new(FieldId::new(1), "name", Type::String, false)],
        ))
        .expect("schema v1");
    catalog
        .register_schema(schema(
            2,
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, true),
            ],
        ))
        .expect("schema v2");
    let before = catalog.generation();

    let result = catalog.register_schema(schema(
        3,
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, false),
        ],
    ));

    assert!(matches!(result, Err(CatalogError::SchemaRequiresMigration)));
    assert_eq!(catalog.generation(), before);
}

#[test]
fn rejects_collection_with_missing_bucket() {
    let catalog = populated_catalog();
    let before = catalog.generation();
    let collection = Collection::new(
        CollectionId::new(2),
        KeyspaceId::new(1),
        "inventory",
        "player",
        key_schema(),
        vec![Fragment::new(
            FragmentId::new(1),
            "items",
            BucketId::new(99),
            FragmentCardinality::Many,
        )],
    );

    assert!(matches!(
        catalog.create_collection(collection),
        Err(CatalogError::InvalidReference(_))
    ));
    assert_eq!(catalog.generation(), before);
}

#[test]
fn registers_bucket_index_and_validates_fields() {
    let catalog = populated_catalog();
    catalog
        .create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(1),
                "by_level",
                IndexKind::Equality,
                vec![FieldId::new(2)],
            ),
        )
        .expect("index");

    assert_eq!(catalog.generation().get(), 5);
    let snapshot = catalog.snapshot().expect("snapshot");
    assert_eq!(
        snapshot
            .index_for_field(BucketId::new(1), FieldId::new(2))
            .expect("index")
            .name(),
        "by_level"
    );

    assert!(matches!(
        catalog.create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(2),
                "by_missing",
                IndexKind::Equality,
                vec![FieldId::new(99)],
            ),
        ),
        Err(CatalogError::InvalidIndex(_))
    ));
    assert!(matches!(
        catalog.create_index(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(1),
                "duplicate",
                IndexKind::Equality,
                vec![FieldId::new(1)],
            ),
        ),
        Err(CatalogError::Duplicate("index"))
    ));
}

#[test]
fn rejects_bucket_with_missing_schema() {
    let catalog = Catalog::new();
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");

    assert!(matches!(
        catalog.create_bucket(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(1),
                "player_profile",
                16,
                BucketPolicy::Strict,
                SchemaId::new(99),
                SchemaVersion::new(1),
            ),
        ),
        Err(CatalogError::NotFound("schema"))
    ));
}
