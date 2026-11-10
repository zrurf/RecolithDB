mod support;

use recolith_client::{Client, ClientConfig};
use recolith_protocol::compression::CompressionAlgorithm;
use recolith_protocol::messages::{DocumentGetRequest, FieldSelection, SecurityMode, Value, value};
use recolith_server::ServerConfig;

#[tokio::test]
async fn connects_over_tls_and_compresses_frames() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address"))
            .with_auth_token(b"tls-secret".to_vec())
            .with_tls(
                recolith_testkit::tls_server_certificate(),
                recolith_testkit::tls_server_key(),
            ),
        support::game_store(),
    )
    .await;

    let mut client = Client::connect(
        ClientConfig::new(address.to_string())
            .with_auth_token(b"tls-secret".to_vec())
            .with_tls("localhost", recolith_testkit::tls_ca_certificate()),
    )
    .await
    .expect("connect");

    assert_eq!(client.welcome().security, SecurityMode::Tls13 as i32);
    assert_eq!(client.compression(), CompressionAlgorithm::Zstd);

    let name = "x".repeat(4096);
    let key = support::document_key(7);
    let put = client
        .document_put(&support::profile_put(&key, &name, b"tls-idem-1"))
        .await
        .expect("put");
    assert_eq!(put.commit_ts, 1);

    let response = client
        .document_get(&DocumentGetRequest {
            meta: None,
            collection_id: 1,
            partition: 0,
            document_key: key,
            fragments: vec![1],
            projection: vec![FieldSelection {
                fragment_id: 1,
                field_id: 1,
            }],
        })
        .await
        .expect("get");
    assert_eq!(
        response.fragments[0].fields[0].value,
        Some(Value {
            kind: Some(value::Kind::StringValue(name)),
        })
    );

    client.close().await.expect("close");
}

#[tokio::test]
async fn rejects_wrong_token_over_tls() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address"))
            .with_auth_token(b"tls-secret".to_vec())
            .with_tls(
                recolith_testkit::tls_server_certificate(),
                recolith_testkit::tls_server_key(),
            ),
        support::game_store(),
    )
    .await;

    let error = Client::connect(
        ClientConfig::new(address.to_string())
            .with_auth_token(b"wrong".to_vec())
            .with_tls("localhost", recolith_testkit::tls_ca_certificate()),
    )
    .await
    .expect_err("authentication must fail");
    assert!(
        matches!(
            error,
            recolith_client::ClientError::Server {
                status: recolith_protocol::messages::status::AUTH_FAILED,
                ..
            }
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn rejects_plaintext_client_on_tls_listener() {
    let address = support::start_server(
        ServerConfig::new("127.0.0.1:0".parse().expect("address")).with_tls(
            recolith_testkit::tls_server_certificate(),
            recolith_testkit::tls_server_key(),
        ),
        support::game_store(),
    )
    .await;

    assert!(
        Client::connect(ClientConfig::new(address.to_string()))
            .await
            .is_err()
    );
}
