mod support;

use std::sync::Arc;

use prost::Message;
use recolith_client::{BatchOperation, Client, ClientConfig, ClientError};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::messages::{
    AggregateRequest, BackupRequest, DescribeCollectionRequest, DocumentDeleteRequest,
    DocumentGetRequest, DocumentGetResponse, ExplainRequest, FieldSelection, NamedParameter,
    QueryRequest, QueryStreamRequest, SecurityMode, Value, describe_collection_request,
    explain_response, feature, opcode, status, value,
};
use recolith_server::ServerConfig;

#[tokio::test]
async fn negotiates_pings_and_closes() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        support::game_store(),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");

    assert_eq!(client.welcome().protocol_major, 1);
    assert_eq!(client.welcome().security, SecurityMode::None as i32);
    assert_eq!(client.welcome().features & feature::BATCH, feature::BATCH);
    assert_eq!(client.compression(), CompressionAlgorithm::Zstd);
    assert_eq!(
        client.ping(b"hello").await.expect("ping"),
        b"hello".to_vec()
    );

    client.close().await.expect("close");
    assert!(client.is_closed());
    assert!(matches!(
        client.ping(b"after close").await,
        Err(ClientError::Closed)
    ));
}

#[tokio::test]
async fn writes_reads_and_queries_documents() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        support::game_store(),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");
    let key = support::document_key(42);

    let metadata = client
        .describe_collection(&DescribeCollectionRequest {
            meta: None,
            keyspace: "game".to_owned(),
            target: Some(describe_collection_request::Target::CollectionName(
                "player".to_owned(),
            )),
        })
        .await
        .expect("describe");
    let collection = metadata.collection.expect("collection");
    assert_eq!(collection.name, "player");
    assert_eq!(collection.key_fields[0].name, "player_id");
    assert_eq!(collection.fragments[0].name, "profile");

    let put = client
        .document_put(&support::profile_put(&key, "alex", b"idem-1"))
        .await
        .expect("put");
    assert_eq!(put.commit_ts, 1);
    assert_eq!(put.versions.len(), 1);

    let get = DocumentGetRequest {
        meta: None,
        collection_id: 1,
        partition: 0,
        document_key: key.clone(),
        fragments: vec![1],
        projection: vec![FieldSelection {
            fragment_id: 1,
            field_id: 1,
        }],
    };
    let response = client.document_get(&get).await.expect("get");
    assert_eq!(response.fragments.len(), 1);
    assert_eq!(
        response.fragments[0].fields[0].value,
        Some(Value {
            kind: Some(value::Kind::StringValue("alex".to_owned())),
        })
    );

    let query = QueryRequest {
        meta: None,
        partition: 0,
        rql: "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile"
            .to_owned(),
        parameters: vec![NamedParameter {
            name: "id".to_owned(),
            value: Some(Value {
                kind: Some(value::Kind::U64Value(42)),
            }),
        }],
        limit: 10,
        cursor: Vec::new(),
    };
    let response = client.query(&query).await.expect("query");
    assert_eq!(response.documents.len(), 1);
    assert_eq!(response.documents[0].document_key, key);

    for (id, idempotency) in [(43_u64, b"idem-2"), (44, b"idem-3")] {
        client
            .document_put(&support::profile_put(
                &support::document_key(id),
                "ben",
                idempotency,
            ))
            .await
            .expect("put scan document");
    }
    let scan = |cursor: Vec<u8>, limit: u64| QueryRequest {
        meta: None,
        partition: 0,
        rql: "SELECT profile.name FROM player WHERE profile.level >= 0 \
              INCLUDE FRAGMENTS profile"
            .to_owned(),
        parameters: Vec::new(),
        limit,
        cursor,
    };
    let first = client
        .query(&scan(Vec::new(), 2))
        .await
        .expect("first page");
    assert_eq!(first.documents.len(), 2);
    assert!(!first.next_cursor.is_empty());
    let second = client
        .query(&scan(first.next_cursor, 2))
        .await
        .expect("second page");
    assert_eq!(second.documents.len(), 1);
    assert!(second.next_cursor.is_empty());

    let mut chunks = Vec::new();
    client
        .query_stream(
            &QueryStreamRequest {
                meta: None,
                partition: 0,
                rql: "SELECT profile.name FROM player WHERE profile.level >= 0 \
                      INCLUDE FRAGMENTS profile"
                    .to_owned(),
                parameters: Vec::new(),
                limit: 10,
                chunk_size: 2,
            },
            |chunk| chunks.push(chunk.clone()),
        )
        .await
        .expect("stream");
    assert!(chunks.len() >= 2);
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.documents.len())
            .sum::<usize>(),
        3
    );
    assert!(chunks.last().expect("chunks").r#final);
}

#[tokio::test]
async fn explains_queries_without_executing_them() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        support::game_store(),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");

    let point = client
        .explain(&ExplainRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE player_id = :id \
                  INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: vec![NamedParameter {
                name: "id".to_owned(),
                value: Some(Value {
                    kind: Some(value::Kind::U64Value(42)),
                }),
            }],
        })
        .await
        .expect("explain point");
    let Some(explain_response::Plan::Document(plan)) = point.plan else {
        panic!("expected document plan");
    };
    assert_eq!(plan.document_key, support::document_key(42));
    assert_eq!(plan.fragment_ids, vec![1]);

    let scan = client
        .explain(&ExplainRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
                  INCLUDE FRAGMENTS profile ORDER BY profile.level DESC"
                .to_owned(),
            parameters: Vec::new(),
        })
        .await
        .expect("explain scan");
    let Some(explain_response::Plan::Scan(plan)) = scan.plan else {
        panic!("expected scan plan");
    };
    assert_eq!(plan.collection_id, 1);
    assert_eq!(plan.fragment_ids, vec![1]);
    assert!(plan.drives[0].bucket_scan);
    let order = plan.order.expect("order");
    assert_eq!(order.field_id, 2);
    assert!(order.descending);
    assert_eq!(order.index_id, None);
}

#[tokio::test]
async fn aggregates_documents_through_the_client() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        support::game_store(),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");

    for (index, player) in [42_u64, 43, 44].into_iter().enumerate() {
        client
            .document_put(&support::profile_put(
                &support::document_key(player),
                "alex",
                format!("aggregate-{index}").as_bytes(),
            ))
            .await
            .expect("put");
    }

    let response = client
        .aggregate(&AggregateRequest {
            meta: None,
            partition: 0,
            rql: "SELECT COUNT(*), SUM(profile.level), AVG(profile.level) \
                  FROM player WHERE profile.level >= 0 INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
        })
        .await
        .expect("aggregate");
    assert_eq!(response.columns.len(), 3);
    assert_eq!(response.rows.len(), 1);
    assert_eq!(
        response.rows[0].values,
        vec![
            Value {
                kind: Some(value::Kind::U64Value(3)),
            },
            Value {
                kind: Some(value::Kind::U64Value(30)),
            },
            Value {
                kind: Some(value::Kind::F64Value(10.0)),
            },
        ]
    );

    let grouped = client
        .aggregate(&AggregateRequest {
            meta: None,
            partition: 0,
            rql: "SELECT COUNT(*) FROM player WHERE profile.level >= 0 \
                  INCLUDE FRAGMENTS profile GROUP BY profile.level"
                .to_owned(),
            parameters: Vec::new(),
            limit: 10,
        })
        .await
        .expect("grouped aggregate");
    assert_eq!(grouped.group_fragment_id, Some(1));
    assert_eq!(grouped.group_field_id, Some(2));
    assert_eq!(grouped.rows.len(), 1);
    assert!(!grouped.truncated);
    assert_eq!(
        grouped.rows[0].values,
        vec![Value {
            kind: Some(value::Kind::U64Value(3)),
        }]
    );
}

#[tokio::test]
async fn writes_online_backups_through_the_client() {
    let directory =
        std::env::temp_dir().join(format!("recolith-client-backup-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("backup directory");
    let store = Arc::try_unwrap(support::game_store())
        .expect("unique store")
        .with_backup_directory(directory.clone());
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        Arc::new(store),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");

    client
        .document_put(&support::profile_put(
            &support::document_key(42),
            "alex",
            b"backup-document",
        ))
        .await
        .expect("put");

    let response = client
        .backup(&BackupRequest {
            meta: None,
            name: "client.backup".to_owned(),
        })
        .await
        .expect("backup");
    assert_eq!(response.file_name, "client.backup");
    assert!(response.entries > 0);
    assert!(directory.join("client.backup").exists());

    let _ = std::fs::remove_dir_all(directory);
}

#[tokio::test]
async fn deletes_documents_through_the_client() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        support::game_store(),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");
    let key = support::document_key(42);

    client
        .document_put(&support::profile_put(&key, "alex", b"delete-1"))
        .await
        .expect("put");

    let request = DocumentDeleteRequest {
        meta: None,
        collection_id: 1,
        partition: 0,
        document_key: key.clone(),
    };
    assert!(
        client
            .document_delete(&request)
            .await
            .expect("delete")
            .deleted
    );
    assert!(
        !client
            .document_delete(&request)
            .await
            .expect("idempotent delete")
            .deleted
    );
}

#[tokio::test]
async fn executes_batches_and_reports_individual_failures() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        support::game_store(),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");
    let key = support::document_key(42);

    client
        .document_put(&support::profile_put(&key, "alex", b"idem-2"))
        .await
        .expect("put");

    let operations = [
        BatchOperation::ping(b"batch".to_vec()),
        BatchOperation::document_get(&DocumentGetRequest {
            meta: None,
            collection_id: 99,
            partition: 0,
            document_key: support::document_key(999),
            fragments: vec![1],
            projection: Vec::new(),
        }),
    ];
    let results = client.batch(&operations).await.expect("batch");
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    assert_eq!(results[0].opcode, opcode::PONG);
    assert_eq!(results[1].opcode, opcode::ERROR);
    assert_eq!(results[1].status, status::NOT_FOUND);
    assert!(results[1].error_message().is_some());

    let operations = [
        BatchOperation::ping(b"one".to_vec()),
        BatchOperation::document_get(&DocumentGetRequest {
            meta: None,
            collection_id: 1,
            partition: 0,
            document_key: key.clone(),
            fragments: vec![1],
            projection: Vec::new(),
        }),
    ];
    let results = client.batch(&operations).await.expect("batch");
    let response =
        DocumentGetResponse::decode(results[1].payload.as_slice()).expect("document response");
    assert_eq!(response.fragments.len(), 1);
}

#[tokio::test]
async fn maps_server_status_codes() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")),
        support::game_store(),
    )
    .await;
    let mut client = Client::connect(ClientConfig::new(address.to_string()))
        .await
        .expect("connect");

    let request = DocumentGetRequest {
        meta: None,
        collection_id: 99,
        partition: 0,
        document_key: support::document_key(1),
        fragments: vec![1],
        projection: Vec::new(),
    };
    let error = client.document_get(&request).await.expect_err("get");
    assert!(
        matches!(
            error,
            ClientError::Server {
                status: status::NOT_FOUND,
                ..
            }
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn authenticates_with_bearer_tokens() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address"))
            .with_auth_token(b"secret".to_vec()),
        support::game_store(),
    )
    .await;

    let mut client =
        Client::connect(ClientConfig::new(address.to_string()).with_auth_token(b"secret".to_vec()))
            .await
            .expect("connect");
    assert_eq!(client.ping(b"ok").await.expect("ping"), b"ok".to_vec());

    let error =
        Client::connect(ClientConfig::new(address.to_string()).with_auth_token(b"wrong".to_vec()))
            .await
            .expect_err("authentication must fail");
    assert!(
        matches!(
            error,
            ClientError::Server {
                status: status::AUTH_FAILED,
                ..
            }
        ),
        "unexpected error: {error}"
    );
}
