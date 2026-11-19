use std::sync::Arc;

use recolith_backend_memory::MemoryBackend;
use recolith_cli::{Args, BucketCommand, Command, DocCommand, WorkflowCommand, run};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_server::{DocumentStore, ServerConfig, serve};
use recolith_storage::encode_document_key;
use tokio::net::TcpListener;

#[tokio::test]
async fn writes_reads_and_queries_through_the_cli() {
    let backup_directory =
        std::env::temp_dir().join(format!("recolith-cli-backup-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&backup_directory);
    std::fs::create_dir_all(&backup_directory).expect("backup directory");
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    )
    .with_backup_directory(backup_directory.clone());
    let key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[KeyValue::U64(1)],
    )
    .expect("document key");
    let key_hex = hexadecimal(&key);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(serve(listener, ServerConfig::new(address), Arc::new(store)));
    let endpoint = address.to_string();

    run(args(
        endpoint.clone(),
        Command::Ping {
            payload: "hello".to_owned(),
        },
    ))
    .await
    .expect("ping command");

    run(args(
        endpoint.clone(),
        Command::Describe {
            collection: "player".to_owned(),
            keyspace: Some("game".to_owned()),
        },
    ))
    .await
    .expect("describe command");

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Put {
                collection: "player".to_owned(),
                keyspace: Some("game".to_owned()),
                partition: 0,
                key_hex: None,
                keys: vec!["player_id=u64:1".to_owned()],
                idempotency_key: "cli-idempotency-1".to_owned(),
                ttl_seconds: Some(60),
                fields: vec![
                    "profile.name=string:alex".to_owned(),
                    "profile.level=u32:10".to_owned(),
                ],
                subkeys: Vec::new(),
            },
        },
    ))
    .await
    .expect("put command");

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Get {
                collection: "1".to_owned(),
                keyspace: None,
                partition: 0,
                key_hex: Some(key_hex.clone()),
                keys: Vec::new(),
                fragments: vec!["1".to_owned()],
            },
        },
    ))
    .await
    .expect("get command");

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Put {
                collection: "1".to_owned(),
                keyspace: None,
                partition: 0,
                key_hex: Some(key_hex),
                keys: Vec::new(),
                idempotency_key: "cli-idempotency-2".to_owned(),
                ttl_seconds: None,
                fields: vec!["1:1=string:alex".to_owned(), "1:2=u32:11".to_owned()],
                subkeys: Vec::new(),
            },
        },
    ))
    .await
    .expect("numeric put command");

    run(args(
        endpoint.clone(),
        Command::Explain {
            rql: "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile"
                .to_owned(),
            partition: 0,
            parameters: vec!["id=u64:1".to_owned()],
        },
    ))
    .await
    .expect("explain command");

    run(args(
        endpoint.clone(),
        Command::Aggregate {
            rql: "SELECT COUNT(*), SUM(profile.level) FROM player \
                  WHERE profile.level >= 0 INCLUDE FRAGMENTS profile"
                .to_owned(),
            partition: 0,
            parameters: Vec::new(),
            limit: 10,
        },
    ))
    .await
    .expect("aggregate command");

    run(args(
        endpoint.clone(),
        Command::Backup {
            name: "cli.backup".to_owned(),
        },
    ))
    .await
    .expect("backup command");
    assert!(backup_directory.join("cli.backup").exists());

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Delete {
                collection: "player".to_owned(),
                keyspace: Some("game".to_owned()),
                partition: 0,
                key_hex: None,
                keys: vec!["player_id=u64:1".to_owned()],
            },
        },
    ))
    .await
    .expect("delete command");

    run(args(
        endpoint,
        Command::Query {
            rql: "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile"
                .to_owned(),
            partition: 0,
            parameters: vec!["id=u64:1".to_owned()],
            limit: 10,
            cursor: None,
            stream: false,
            chunk_size: 100,
        },
    ))
    .await
    .expect("query command");

    let _ = std::fs::remove_dir_all(backup_directory);
}

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

fn hexadecimal(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test]
async fn tracks_workflows_through_the_cli() {
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(serve(listener, ServerConfig::new(address), Arc::new(store)));
    let endpoint = address.to_string();

    run(args(
        endpoint.clone(),
        Command::Workflow {
            command: WorkflowCommand::List {
                limit: 100,
                start_after: None,
            },
        },
    ))
    .await
    .expect("empty list");

    run(args(
        endpoint.clone(),
        Command::Doc {
            command: DocCommand::Put {
                collection: "player".to_owned(),
                keyspace: Some("game".to_owned()),
                partition: 0,
                key_hex: None,
                keys: vec!["player_id=u64:1".to_owned()],
                idempotency_key: "wf-cli-1".to_owned(),
                ttl_seconds: None,
                fields: vec![
                    "profile.name=string:alex".to_owned(),
                    "profile.level=u32:10".to_owned(),
                ],
                subkeys: Vec::new(),
            },
        },
    ))
    .await
    .expect("put command");

    run(args(
        endpoint.clone(),
        Command::Workflow {
            command: WorkflowCommand::Status {
                id: "missing".to_owned(),
            },
        },
    ))
    .await
    .expect_err("missing workflow status");

    run(args(
        endpoint,
        Command::Workflow {
            command: WorkflowCommand::List {
                limit: 100,
                start_after: None,
            },
        },
    ))
    .await
    .expect("list command");
}

#[tokio::test]
async fn manages_bucket_records_through_the_cli() {
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(serve(listener, ServerConfig::new(address), Arc::new(store)));
    let endpoint = address.to_string();

    run(args(
        endpoint.clone(),
        Command::Bucket {
            command: BucketCommand::Put {
                keyspace: Some("game".to_owned()),
                bucket: "scratch".to_owned(),
                partition: 1,
                fragment: 9,
                key_hex: None,
                key: Some("k1".to_owned()),
                subkey_hex: None,
                subkey: None,
                fields: vec!["1=string:hello".to_owned(), "2=u32:10".to_owned()],
                if_match: None,
            },
        },
    ))
    .await
    .expect("bucket put");

    run(args(
        endpoint.clone(),
        Command::Bucket {
            command: BucketCommand::Get {
                keyspace: Some("game".to_owned()),
                bucket: "scratch".to_owned(),
                partition: 1,
                fragment: 9,
                key_hex: None,
                key: Some("k1".to_owned()),
                subkey_hex: None,
                subkey: None,
            },
        },
    ))
    .await
    .expect("bucket get");

    run(args(
        endpoint.clone(),
        Command::Bucket {
            command: BucketCommand::Scan {
                keyspace: None,
                bucket: "2".to_owned(),
                partition: 1,
                fragment: Some(9),
                limit: 10,
                start_after_hex: None,
            },
        },
    ))
    .await
    .expect("bucket scan");

    run(args(
        endpoint.clone(),
        Command::Bucket {
            command: BucketCommand::Delete {
                keyspace: Some("game".to_owned()),
                bucket: "scratch".to_owned(),
                partition: 1,
                fragment: 9,
                key_hex: None,
                key: Some("k1".to_owned()),
                subkey_hex: None,
                subkey: None,
            },
        },
    ))
    .await
    .expect("bucket delete");
}
