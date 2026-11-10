use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_cli::{Args, CatalogCommand, Command, DocCommand, run};
use recolith_server::{DocumentStore, ServerConfig, serve};
use tokio::net::TcpListener;

const IDL: &str = r#"
record Profile {
    name: string @1;
    level: u32 @2;
}
"#;

const CATALOG_DEFINITION: &str = r#"
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

fn args(endpoint: String, command: Command) -> Args {
    Args {
        endpoint,
        token: None,
        tls_ca: None,
        tls_server_name: None,
        timeout_secs: 5,
        command,
    }
}

#[tokio::test]
async fn applies_a_catalog_definition_file() {
    let dir = std::env::temp_dir().join(format!("recolith-cli-apply-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("temp dir");
    let definition: PathBuf = dir.join("catalog.toml");
    fs::write(&definition, CATALOG_DEFINITION).expect("definition");

    let store = DocumentStore::new(Arc::new(Catalog::new()), Arc::new(MemoryBackend::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(serve(listener, ServerConfig::new(address), Arc::new(store)));
    let endpoint = address.to_string();

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::Apply {
                file: definition.clone(),
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("apply file");

    run(args(
        endpoint.clone(),
        Command::Describe {
            collection: "player".to_owned(),
            keyspace: Some("game".to_owned()),
        },
    ))
    .await
    .expect("describe");

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Put {
                collection: "player".to_owned(),
                keyspace: Some("game".to_owned()),
                partition: 0,
                key_hex: None,
                keys: vec!["player_id=u64:1".to_owned()],
                idempotency_key: "apply-file-1".to_owned(),
                fields: vec![
                    "profile.name=string:alex".to_owned(),
                    "profile.level=u32:10".to_owned(),
                ],
                subkeys: Vec::new(),
            },
        },
    ))
    .await
    .expect("put");

    run(args(
        endpoint,
        Command::Catalog {
            command: CatalogCommand::Apply {
                file: definition,
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("idempotent reapply");

    let _ = fs::remove_dir_all(dir);
}

#[tokio::test]
async fn creates_catalog_objects_and_uses_them_from_the_cli() {
    let dir = std::env::temp_dir().join(format!("recolith-cli-catalog-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("temp dir");
    let idl_file: PathBuf = dir.join("schema.idl");
    fs::write(&idl_file, IDL).expect("idl");

    let store = DocumentStore::new(Arc::new(Catalog::new()), Arc::new(MemoryBackend::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(serve(listener, ServerConfig::new(address), Arc::new(store)));
    let endpoint = address.to_string();

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::CreateKeyspace {
                id: 1,
                name: "game".to_owned(),
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("keyspace");

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::RegisterSchema {
                id: 1,
                version: 1,
                idl_file,
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("schema");

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::CreateBucket {
                id: 1,
                keyspace_id: 1,
                name: "player_profile".to_owned(),
                partitions: 16,
                schema_id: 1,
                schema_version: 1,
                policy: "strict".to_owned(),
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("bucket");

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::CreateCollection {
                id: 1,
                keyspace_id: 1,
                name: "player".to_owned(),
                affinity_group: None,
                key_fields: vec!["player_id:u64".to_owned()],
                fragments: vec!["1:profile:1:one".to_owned()],
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("collection");

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::CreateIndex {
                bucket_id: 1,
                id: 1,
                name: "by-level".to_owned(),
                kind: "range".to_owned(),
                fields: vec![2],
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("index");

    let idl_v2: PathBuf = dir.join("schema-v2.idl");
    fs::write(
        &idl_v2,
        r#"
record Profile {
    name: string @1;
    level: u32 @2;
    optional nickname: string @3;
}
"#,
    )
    .expect("idl v2");
    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::RegisterSchema {
                id: 1,
                version: 2,
                idl_file: idl_v2,
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("schema v2");

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::AlterBucketSchema {
                bucket_id: 1,
                schema_version: 2,
                mappings: Vec::new(),
                conversions: Vec::new(),
                defaults: Vec::new(),
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("alter bucket schema");

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::AlterIndex {
                bucket_id: 1,
                index_id: 1,
                name: "by-level-v2".to_owned(),
                kind: "range".to_owned(),
                fields: vec![2],
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("alter index");

    run(args(
        endpoint.clone(),
        Command::Describe {
            collection: "player".to_owned(),
            keyspace: Some("game".to_owned()),
        },
    ))
    .await
    .expect("describe");

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Put {
                collection: "player".to_owned(),
                keyspace: Some("game".to_owned()),
                partition: 0,
                key_hex: None,
                keys: vec!["player_id=u64:1".to_owned()],
                idempotency_key: "catalog-cli-1".to_owned(),
                fields: vec![
                    "profile.name=string:alex".to_owned(),
                    "profile.level=u32:10".to_owned(),
                ],
                subkeys: Vec::new(),
            },
        },
    ))
    .await
    .expect("put");

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Get {
                collection: "player".to_owned(),
                keyspace: Some("game".to_owned()),
                partition: 0,
                key_hex: None,
                keys: vec!["player_id=u64:1".to_owned()],
                fragments: Vec::new(),
            },
        },
    ))
    .await
    .expect("get");

    run(args(
        endpoint.clone(),
        Command::Catalog {
            command: CatalogCommand::DropIndex {
                bucket_id: 1,
                index_id: 1,
                purge: true,
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("drop index");

    run(args(
        endpoint,
        Command::Catalog {
            command: CatalogCommand::DropCollection {
                id: 1,
                purge: true,
                catalog_generation: 0,
            },
        },
    ))
    .await
    .expect("drop collection");

    let _ = fs::remove_dir_all(dir);
}
