use std::sync::Arc;

use recolith_backend_memory::MemoryBackend;
use recolith_cli::{Args, Command, DocCommand, run};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_server::{DocumentStore, ServerConfig, serve};
use recolith_storage::encode_document_key;
use tokio::net::TcpListener;

#[tokio::test]
async fn writes_reads_and_queries_through_the_cli() {
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    );
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
                fields: vec!["1:1=string:alex".to_owned(), "1:2=u32:11".to_owned()],
                subkeys: Vec::new(),
            },
        },
    ))
    .await
    .expect("numeric put command");

    run(args(
        endpoint,
        Command::Query {
            rql: "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile"
                .to_owned(),
            partition: 0,
            parameters: vec!["id=u64:1".to_owned()],
            limit: 10,
        },
    ))
    .await
    .expect("query command");
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
