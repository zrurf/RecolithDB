use recolith_catalog::{CatalogDefinition, DefinitionError};

const DEFINITION: &str = r#"
[[keyspaces]]
id = 1
name = "game"

[[schemas]]
id = 1
version = 1
idl = """
record Profile {
    name: string @1;
    level: u32 @2;
}
"""

[[buckets]]
id = 1
keyspace_id = 1
name = "player_profile"
partitions = 16
policy = "strict"
schema_id = 1
schema_version = 1

[[collections]]
id = 1
keyspace_id = 1
name = "player"
affinity_group = "player"

[[collections.key]]
name = "player_id"
type = "u64"

[[collections.fragments]]
id = 1
name = "profile"
bucket_id = 1
cardinality = "one"

[[indexes]]
bucket_id = 1
id = 1
name = "by-level"
kind = "range"
fields = [2]

[[placements]]
bucket_id = 1
backend_id = 3
affinity_group = "us-east"

[[buckets]]
id = 2
keyspace_id = 1
name = "events"
partitions = 8
policy = "managed"
schema_id = 1
schema_version = 1

[[replicas]]
bucket_id = 2
affinity_group = "us-west"

[[replicas.members]]
backend_id = 5
role = "leader"

[[replicas.members]]
backend_id = 6
role = "follower"
"#;

#[test]
fn parses_definitions_into_catalog_changes() {
    let definition = CatalogDefinition::parse(DEFINITION).expect("definition");
    assert_eq!(definition.keyspaces.len(), 1);
    assert_eq!(definition.schemas.len(), 1);
    assert_eq!(definition.buckets.len(), 2);
    assert_eq!(definition.collections.len(), 1);
    assert_eq!(definition.indexes.len(), 1);
    assert_eq!(definition.placements.len(), 1);
    assert_eq!(definition.replicas.len(), 1);

    let changes = definition.into_changes().expect("changes");
    assert_eq!(changes.schemas[0].name(), "Profile");
    assert_eq!(changes.collections[0].name(), "player");
    assert_eq!(changes.indexes[0].1.fields().len(), 1);
    assert_eq!(changes.placements[0].bucket_id.get(), 1);
    assert_eq!(changes.placements[0].backend_id.get(), 3);
    assert_eq!(changes.placements[0].affinity_group, "us-east");
}

#[test]
fn builds_catalog_from_definitions() {
    let catalog = CatalogDefinition::parse(DEFINITION)
        .expect("definition")
        .into_catalog()
        .expect("catalog");
    let snapshot = catalog.snapshot().expect("snapshot");

    assert_eq!(snapshot.keyspaces.len(), 1);
    assert_eq!(snapshot.buckets.len(), 2);
    assert_eq!(snapshot.collections.len(), 1);
    assert_eq!(snapshot.schemas.schemas.len(), 1);
    assert_eq!(catalog.generation().get(), 8);
}

#[test]
fn rejects_empty_invalid_and_unknown_definitions() {
    assert!(matches!(
        CatalogDefinition::parse("# no metadata"),
        Err(DefinitionError::Empty)
    ));

    let invalid = r#"
[[schemas]]
id = 1
version = 1
idl = "record Broken {"
"#;
    assert!(matches!(
        CatalogDefinition::parse(invalid)
            .expect("definition")
            .into_catalog(),
        Err(DefinitionError::Schema { .. })
    ));

    let unknown = r#"
[[keyspaces]]
id = 1
name = "game"
unexpected = true
"#;
    assert!(matches!(
        CatalogDefinition::parse(unknown),
        Err(DefinitionError::Parse(_))
    ));
}

#[test]
fn builds_catalog_with_placement_from_definitions() {
    let catalog = CatalogDefinition::parse(DEFINITION)
        .expect("definition")
        .into_catalog()
        .expect("catalog");
    let snapshot = catalog.snapshot().expect("snapshot");
    let placement = snapshot
        .placements
        .get(&recolith_common::BucketId::new(1))
        .expect("placement");
    assert_eq!(placement.backend_id().get(), 3);
    assert_eq!(placement.affinity_group(), "us-east");
    assert_eq!(placement.partition_count(), 16);
}

#[test]
fn rejects_placement_for_unknown_bucket() {
    let broken = DEFINITION.replace(
        "bucket_id = 1
backend_id = 3",
        "bucket_id = 9
backend_id = 3",
    );
    let error = CatalogDefinition::parse(&broken)
        .expect("parse")
        .into_catalog()
        .expect_err("unknown bucket");
    assert!(error.to_string().contains("not found") || error.to_string().contains("bucket"));
}

#[test]
fn builds_catalog_with_replica_set_from_definitions() {
    let catalog = CatalogDefinition::parse(DEFINITION)
        .expect("definition")
        .into_catalog()
        .expect("catalog");
    let snapshot = catalog.snapshot().expect("snapshot");
    let replica_set = snapshot
        .replica_sets
        .get(&recolith_common::BucketId::new(2))
        .expect("replica set");
    assert_eq!(replica_set.affinity_group(), "us-west");
    assert_eq!(replica_set.leader(), recolith_common::BackendId::new(5));
    assert_eq!(replica_set.members().len(), 2);
}
