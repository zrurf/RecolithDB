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
        endpoint,
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

    let _ = fs::remove_dir_all(dir);
}
