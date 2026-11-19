use recolith_catalog::{
    Catalog, CatalogChanges, CatalogError, ReplicaMember, ReplicaRole, ReplicaSet,
    apply::PlacementChange,
};
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
        replicas: Vec::new(),
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

#[test]
fn applies_replica_sets_with_validation() {
    use recolith_model::{Bucket, BucketPolicy};

    let catalog = Catalog::new();
    let updated = catalog
        .apply_changes(&changes())
        .expect("apply")
        .expect("changed");
    let mut replica_changes = changes();
    replica_changes.replicas = vec![ReplicaSet::new(
        BucketId::new(1),
        "us-east",
        vec![
            ReplicaMember::new(recolith_common::BackendId::new(5), ReplicaRole::Leader),
            ReplicaMember::new(recolith_common::BackendId::new(6), ReplicaRole::Follower),
        ],
    )];

    let next = updated
        .apply_changes(&replica_changes)
        .expect("apply replicas")
        .expect("changed");
    let snapshot = next.snapshot().expect("snapshot");
    let replica_set = snapshot
        .replica_sets
        .get(&BucketId::new(1))
        .expect("replica set");
    assert_eq!(replica_set.leader(), recolith_common::BackendId::new(5));

    // Identical reapplication is a no-op.
    assert!(
        next.apply_changes(&replica_changes)
            .expect("apply")
            .is_none()
    );

    // A different membership conflicts.
    let mut conflicting = replica_changes.clone();
    conflicting.replicas[0] = ReplicaSet::new(
        BucketId::new(1),
        "us-east",
        vec![ReplicaMember::new(
            recolith_common::BackendId::new(7),
            ReplicaRole::Leader,
        )],
    );
    assert!(matches!(
        next.apply_changes(&conflicting),
        Err(CatalogError::Conflict(message)) if message.contains("replica set")
    ));

    // Replica sets and single-backend placements are mutually exclusive.
    let mut mixed = replica_changes;
    mixed.buckets = vec![(
        KeyspaceId::new(1),
        Bucket::new(
            BucketId::new(2),
            "extra",
            8,
            recolith_model::BucketPolicy::Raw,
            recolith_common::SchemaId::new(1),
            recolith_common::SchemaVersion::new(1),
        ),
    )];
    mixed.placements = vec![PlacementChange {
        bucket_id: BucketId::new(2),
        backend_id: recolith_common::BackendId::new(1),
        affinity_group: "us-west".to_owned(),
    }];
    let with_placement = next
        .apply_changes(&mixed)
        .expect("apply placement")
        .expect("changed");
    let mut conflicting_replica = mixed;
    conflicting_replica.replicas[0] = ReplicaSet::new(
        BucketId::new(2),
        "us-east",
        vec![ReplicaMember::new(
            recolith_common::BackendId::new(5),
            ReplicaRole::Leader,
        )],
    );
    assert!(matches!(
        with_placement.apply_changes(&conflicting_replica),
        Err(CatalogError::InvalidPlacement(_))
    ));

    // Two leaders are rejected.
    let mut two_leaders = changes();
    two_leaders.buckets = vec![(
        KeyspaceId::new(1),
        Bucket::new(
            BucketId::new(3),
            "spare",
            8,
            BucketPolicy::Raw,
            recolith_common::SchemaId::new(1),
            recolith_common::SchemaVersion::new(1),
        ),
    )];
    two_leaders.replicas = vec![ReplicaSet::new(
        BucketId::new(3),
        "us-east",
        vec![
            ReplicaMember::new(recolith_common::BackendId::new(5), ReplicaRole::Leader),
            ReplicaMember::new(recolith_common::BackendId::new(6), ReplicaRole::Leader),
        ],
    )];
    match with_placement.apply_changes(&two_leaders) {
        Err(CatalogError::InvalidPlacement(message)) => {
            assert!(message.contains("exactly one leader"), "got: {message}");
        }
        other => panic!("expected invalid placement, got: {other:?}"),
    }
}
