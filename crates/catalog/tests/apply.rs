use recolith_catalog::{Catalog, CatalogChanges, CatalogError, apply::PlacementChange};
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, IndexId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
    IndexDefinition, IndexKind, KeyField, KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};

fn changes() -> CatalogChanges {
    CatalogChanges {
        schemas: vec![RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "profile",
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
            ],
        )],
        keyspaces: vec![Keyspace::new(KeyspaceId::new(1), "game")],
        buckets: vec![(
            KeyspaceId::new(1),
            Bucket::new(
                BucketId::new(1),
                "player_profile",
                16,
                BucketPolicy::Strict,
                SchemaId::new(1),
                SchemaVersion::new(1),
            ),
        )],
        collections: vec![Collection::new(
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
        )],
        indexes: vec![(
            BucketId::new(1),
            IndexDefinition::new(
                IndexId::new(1),
                "by-level",
                IndexKind::Range,
                vec![FieldId::new(2)],
            ),
        )],
        placements: Vec::new(),
    }
}

#[test]
fn applies_placements_with_bucket_partition_count() {
    let catalog = Catalog::new();
    let updated = catalog
        .apply_changes(&changes())
        .expect("apply")
        .expect("changed");
    let mut placed = changes();
    placed.placements = vec![PlacementChange {
        bucket_id: BucketId::new(1),
        backend_id: recolith_common::BackendId::new(5),
        affinity_group: "us-east".to_owned(),
    }];

    let next = updated
        .apply_changes(&placed)
        .expect("apply")
        .expect("changed");
    let snapshot = next.snapshot().expect("snapshot");
    let placement = snapshot
        .placements
        .get(&BucketId::new(1))
        .expect("placement");
    assert_eq!(placement.backend_id().get(), 5);
    assert_eq!(placement.partition_count(), 16);

    // Reapplying the identical placement is a no-op.
    assert!(next.apply_changes(&placed).expect("apply").is_none());
    // A different backend for the same bucket conflicts.
    let mut conflicting = placed.clone();
    conflicting.placements[0].backend_id = recolith_common::BackendId::new(6);
    assert!(matches!(
        next.apply_changes(&conflicting),
        Err(CatalogError::Conflict(message)) if message.contains("placement")
    ));
    // An unknown bucket is rejected.
    let mut unknown = placed;
    unknown.placements[0].bucket_id = BucketId::new(9);
    assert!(matches!(
        next.apply_changes(&unknown),
        Err(CatalogError::NotFound("bucket"))
    ));
}

#[test]
fn applies_all_definitions_and_advances_generation() {
    let catalog = Catalog::new();
    let updated = catalog
        .apply_changes(&changes())
        .expect("apply")
        .expect("changed");
    let snapshot = updated.snapshot().expect("snapshot");

    assert_eq!(snapshot.generation.get(), 5);
    assert!(snapshot.keyspaces.contains_key(&KeyspaceId::new(1)));
    assert!(snapshot.buckets.contains_key(&BucketId::new(1)));
    assert!(snapshot.collections.contains_key(&CollectionId::new(1)));
    assert!(
        snapshot
            .indexes
            .get(&BucketId::new(1))
            .is_some_and(|indexes| indexes.contains_key(&IndexId::new(1)))
    );
    assert!(
        snapshot
            .schemas
            .schemas
            .contains_key(&(SchemaId::new(1), SchemaVersion::new(1)))
    );
    assert_eq!(
        catalog.generation().get(),
        0,
        "original catalog is unchanged"
    );
}

#[test]
fn reapplying_identical_definitions_is_a_noop() {
    let catalog = Catalog::new();
    let updated = catalog
        .apply_changes(&changes())
        .expect("apply")
        .expect("changed");

    assert!(
        updated
            .apply_changes(&changes())
            .expect("reapply")
            .is_none()
    );
}

#[test]
fn applies_schema_versions_in_monotonic_order() {
    let catalog = Catalog::new();
    let v1 = RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "profile",
        vec![Field::new(FieldId::new(1), "name", Type::String, false)],
    );
    let v2 = RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(2),
        "profile",
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, true),
        ],
    );
    let changes = CatalogChanges {
        schemas: vec![v2, v1],
        ..Default::default()
    };

    let updated = catalog
        .apply_changes(&changes)
        .expect("apply")
        .expect("changed");
    let snapshot = updated.snapshot().expect("snapshot");

    assert_eq!(snapshot.schemas.schemas.len(), 2);
    assert_eq!(
        snapshot.schemas.latest.get(&SchemaId::new(1)),
        Some(&SchemaVersion::new(2))
    );
}

#[test]
fn rejects_conflicting_definitions() {
    let catalog = Catalog::new();
    let updated = catalog
        .apply_changes(&changes())
        .expect("apply")
        .expect("changed");
    let mut conflicting = changes();
    conflicting.keyspaces[0] = Keyspace::new(KeyspaceId::new(1), "other");

    assert!(matches!(
        updated.apply_changes(&conflicting),
        Err(CatalogError::Conflict(message)) if message.contains("keyspace")
    ));
}

#[test]
fn rejects_missing_references_without_partial_changes() {
    let catalog = Catalog::new();
    let mut broken = changes();
    broken.keyspaces.clear();

    assert!(matches!(
        catalog.apply_changes(&broken),
        Err(CatalogError::NotFound("keyspace"))
    ));
    assert_eq!(catalog.generation().get(), 0);
    assert!(catalog.snapshot().expect("snapshot").collections.is_empty());
}
