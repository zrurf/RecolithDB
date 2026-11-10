use std::sync::Arc;
use std::time::Duration;

use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_server::{DocumentStore, ServerConfig, serve_with_shutdown};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[tokio::test]
async fn shutdown_stops_accepting_and_drains_sessions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let store = Arc::new(DocumentStore::new(
        Arc::new(Catalog::new()),
        Arc::new(MemoryBackend::new()),
    ));
    let (shutdown, wait) = oneshot::channel();
    let server = tokio::spawn(serve_with_shutdown(
        listener,
        ServerConfig::new(address),
        store,
        async move {
            let _ = wait.await;
        },
    ));

    let stream = TcpStream::connect(address).await.expect("connect");
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.send(()).expect("signal shutdown");
    drop(stream);

    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server did not stop")
        .expect("server task")
        .expect("server result");
}
