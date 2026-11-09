use recolith_backend_memory::MemoryBackend;
use recolith_catalog::{load_catalog, save_catalog};
use recolith_server::{BootstrapError, parse_bootstrap};

const BOOTSTRAP: &str = r#"
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
name = "profile_name"
kind = "equality"
fields = [1]
"#;

#[test]
fn builds_catalog_from_bootstrap() {
    let catalog = parse_bootstrap(BOOTSTRAP).expect("bootstrap");
    let snapshot = catalog.snapshot().expect("snapshot");

    assert_eq!(snapshot.keyspaces.len(), 1);
    assert_eq!(snapshot.buckets.len(), 1);
    assert_eq!(snapshot.collections.len(), 1);
    assert_eq!(snapshot.schemas.schemas.len(), 1);
    assert_eq!(
        snapshot.indexes[&recolith_common::BucketId::new(1)].len(),
        1
    );
    let collection = snapshot.collection_by_name("player").expect("collection");
    assert_eq!(collection.fragments()[0].name(), "profile");
    assert_eq!(collection.key_schema().fields()[0].name(), "player_id");
}

#[test]
fn persists_and_reloads_bootstrapped_catalog() {
    let backend = MemoryBackend::new();
    let catalog = parse_bootstrap(BOOTSTRAP).expect("bootstrap");
    let generation = catalog.generation();
    save_catalog(&backend, &catalog).expect("save catalog");

    let loaded = load_catalog(&backend).expect("load catalog");
    assert_eq!(loaded.generation(), generation);
    assert_eq!(
        loaded
            .snapshot()
            .expect("snapshot")
            .collection_by_name("player")
            .expect("collection")
            .name(),
        "player"
    );
}

#[test]
fn rejects_invalid_schema_idl() {
    let input = r#"
[[schemas]]
id = 1
version = 1
idl = "record Broken { name string @1; }"
"#;

    assert!(matches!(
        parse_bootstrap(input),
        Err(BootstrapError::Schema { .. })
    ));
}

#[test]
fn rejects_empty_bootstrap() {
    assert!(matches!(
        parse_bootstrap("# no metadata"),
        Err(BootstrapError::Empty)
    ));
}

#[test]
fn rejects_unknown_bootstrap_fields() {
    let input = r#"
[[keyspaces]]
id = 1
name = "game"
unexpected = true
"#;

    assert!(matches!(
        parse_bootstrap(input),
        Err(BootstrapError::Parse(_))
    ));
}
