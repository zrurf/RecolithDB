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
"#;

#[test]
fn parses_definitions_into_catalog_changes() {
    let definition = CatalogDefinition::parse(DEFINITION).expect("definition");
    assert_eq!(definition.keyspaces.len(), 1);
    assert_eq!(definition.schemas.len(), 1);
    assert_eq!(definition.buckets.len(), 1);
    assert_eq!(definition.collections.len(), 1);
    assert_eq!(definition.indexes.len(), 1);

    let changes = definition.into_changes().expect("changes");
    assert_eq!(changes.schemas[0].name(), "Profile");
    assert_eq!(changes.collections[0].name(), "player");
    assert_eq!(changes.indexes[0].1.fields().len(), 1);
}

#[test]
fn builds_catalog_from_definitions() {
    let catalog = CatalogDefinition::parse(DEFINITION)
        .expect("definition")
        .into_catalog()
        .expect("catalog");
    let snapshot = catalog.snapshot().expect("snapshot");

    assert_eq!(snapshot.keyspaces.len(), 1);
    assert_eq!(snapshot.buckets.len(), 1);
    assert_eq!(snapshot.collections.len(), 1);
    assert_eq!(snapshot.schemas.schemas.len(), 1);
    assert_eq!(catalog.generation().get(), 5);
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
