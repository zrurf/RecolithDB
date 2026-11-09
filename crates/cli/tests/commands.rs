use std::sync::Arc;

use recolith_backend_memory::MemoryBackend;
use recolith_cli::{Args, Command, run};
use recolith_common::{CollectionId, FieldId, FragmentId, PartitionId};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_server::{DocumentStore, DocumentWrite, FragmentWriteRequest, ServerConfig, serve};
use recolith_storage::{RecordData, Value, encode_document_key};
use tokio::net::TcpListener;

#[tokio::test]
async fn pings_and_queries_a_running_server() {
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::new(MemoryBackend::new()),
    );
    let key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[KeyValue::U64(1)],
    )
    .expect("document key");
    let mut profile = RecordData::new();
    profile.insert(FieldId::new(1), Value::String("alex".to_owned()));
    profile.insert(FieldId::new(2), Value::U32(10));
    store
        .write(
            &DocumentWrite::new(CollectionId::new(1), key, PartitionId::new(0)).with_fragment(
                FragmentWriteRequest::new(FragmentId::new(1), Vec::new(), profile),
            ),
        )
        .expect("write document");

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
