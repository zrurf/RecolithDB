use recolith_catalog::{Catalog, CatalogDrop, CatalogError};
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};

fn catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .register_schema(RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "profile",
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
fn drops_index_collection_bucket_and_keyspace() {
    let catalog = catalog();
    let generation = catalog.generation().get();

    let without_index = catalog
        .drop_object(CatalogDrop::Index {
            bucket: BucketId::new(1),
            index: IndexId::new(1),
        })
        .expect("drop index");
    assert!(
        !without_index
            .snapshot()
            .expect("snapshot")
            .indexes
            .contains_key(&BucketId::new(1))
    );
    assert_eq!(without_index.generation().get(), generation + 1);

    let without_collection = without_index
        .drop_object(CatalogDrop::Collection(CollectionId::new(1)))
        .expect("drop collection");
    assert!(
        without_collection
            .snapshot()
            .expect("snapshot")
            .collections
            .is_empty()
    );

    let without_bucket = without_collection
        .drop_object(CatalogDrop::Bucket(BucketId::new(1)))
        .expect("drop bucket");
    assert!(
        without_bucket
            .snapshot()
            .expect("snapshot")
            .buckets
            .is_empty()
    );

    let empty = without_bucket
        .drop_object(CatalogDrop::Keyspace(KeyspaceId::new(1)))
        .expect("drop keyspace");
    let snapshot = empty.snapshot().expect("snapshot");
    assert!(snapshot.keyspaces.is_empty());
    assert!(snapshot.bucket_keyspaces.is_empty());
    assert_eq!(snapshot.schemas.schemas.len(), 1, "schemas are shared");
}

#[test]
fn rejects_dropping_referenced_buckets_and_collections() {
    let catalog = catalog();
    catalog
        .create_collection(Collection::new(
            CollectionId::new(2),
            KeyspaceId::new(1),
            "player_archive",
            "player_archive",
            DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
            vec![Fragment::new(
                FragmentId::new(2),
                "profile",
                BucketId::new(1),
                FragmentCardinality::One,
            )],
        ))
        .expect("second collection");

    assert!(matches!(
        catalog.drop_object(CatalogDrop::Bucket(BucketId::new(1))),
        Err(CatalogError::Referenced(_))
    ));
    assert!(matches!(
        catalog.drop_object(CatalogDrop::Collection(CollectionId::new(1))),
        Err(CatalogError::Referenced(_))
    ));
}

#[test]
fn rejects_missing_targets() {
    let catalog = catalog();

    assert!(matches!(
        catalog.drop_object(CatalogDrop::Index {
            bucket: BucketId::new(1),
            index: IndexId::new(9),
        }),
        Err(CatalogError::NotFound("index"))
    ));
    assert!(matches!(
        catalog.drop_object(CatalogDrop::Collection(CollectionId::new(9))),
        Err(CatalogError::NotFound("collection"))
    ));
    assert!(matches!(
        catalog.drop_object(CatalogDrop::Bucket(BucketId::new(9))),
        Err(CatalogError::NotFound("bucket"))
    ));
    assert!(matches!(
        catalog.drop_object(CatalogDrop::Keyspace(KeyspaceId::new(9))),
        Err(CatalogError::NotFound("keyspace"))
    ));
}

#[test]
fn drops_whole_keyspace_with_collections_and_buckets() {
    let catalog = catalog();
    catalog
        .create_collection(Collection::new(
            CollectionId::new(2),
            KeyspaceId::new(1),
            "player_archive",
            "player_archive",
            DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
            vec![Fragment::new(
                FragmentId::new(2),
                "profile",
                BucketId::new(1),
                FragmentCardinality::One,
            )],
        ))
        .expect("second collection");

    let empty = catalog
        .drop_object(CatalogDrop::Keyspace(KeyspaceId::new(1)))
        .expect("drop keyspace");
    let snapshot = empty.snapshot().expect("snapshot");

    assert!(snapshot.keyspaces.is_empty());
    assert!(snapshot.collections.is_empty());
    assert!(snapshot.buckets.is_empty());
    assert!(snapshot.indexes.is_empty());
}
