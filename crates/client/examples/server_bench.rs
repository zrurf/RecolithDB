//! Concurrent server benchmark over the wire protocol.

use std::sync::Arc;
use std::time::Instant;

use recolith_backend_fjall::FjallBackend;
use recolith_client::{Client, ClientConfig};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_protocol::messages::{
    DocumentGetRequest, DocumentPutRequest, FieldValue, FragmentPayload, NamedParameter,
    QueryRequest, RequestMeta, Value, value,
};
use recolith_server::{DocumentStore, ServerConfig, serve};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

const CLIENTS: u64 = 8;
const WRITES_PER_CLIENT: u64 = 500;
const READS_PER_CLIENT: usize = 2_000;
const QUERIES_PER_CLIENT: usize = 250;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = bench_dir("server-concurrent-fjall");
    let backend = Arc::new(FjallBackend::open(&dir)?);
    let store = Arc::new(DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        backend,
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(serve(listener, ServerConfig::new(address), store));

    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..CLIENTS {
        tasks.spawn(workload(worker, address.to_string()));
    }

    let mut writes = Vec::new();
    let mut reads = Vec::new();
    let mut queries = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let workload = result??;
        writes.extend(workload.writes);
        reads.extend(workload.reads);
        queries.extend(workload.queries);
    }
    let elapsed = started.elapsed();

    server.abort();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);

    println!("clients: {CLIENTS}");
    report("document write", &writes);
    report("document read", &reads);
    report("point query", &queries);

    let operations = writes.len() + reads.len() + queries.len();
    println!("total:");
    println!("  operations: {operations}");
    println!("  wall:      {:.3} s", elapsed.as_secs_f64());
    println!(
        "  rate:      {:.0} ops/s",
        operations as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

struct Workload {
    writes: Vec<u64>,
    reads: Vec<u64>,
    queries: Vec<u64>,
}

async fn workload(
    worker: u64,
    endpoint: String,
) -> Result<Workload, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = ClientConfig::new(endpoint);
    config.client_id = format!("bench-{worker}");
    let mut client = Client::connect(config).await?;
    let base = worker * WRITES_PER_CLIENT;

    let mut writes = Vec::with_capacity(WRITES_PER_CLIENT as usize);
    for offset in 0..WRITES_PER_CLIENT {
        let request = put_request(base + offset);
        let started = Instant::now();
        client.document_put(&request).await?;
        writes.push(elapsed_nanos(started));
    }

    let mut reads = Vec::with_capacity(READS_PER_CLIENT);
    let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ worker;
    for _ in 0..READS_PER_CLIENT {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let id = base + state % WRITES_PER_CLIENT;
        let request = DocumentGetRequest {
            meta: None,
            collection_id: 1,
            partition: 0,
            document_key: document_key(id),
            fragments: vec![1],
            projection: Vec::new(),
        };
        let started = Instant::now();
        client.document_get(&request).await?;
        reads.push(elapsed_nanos(started));
    }

    let mut queries = Vec::with_capacity(QUERIES_PER_CLIENT);
    for offset in 0..QUERIES_PER_CLIENT {
        let id = base + offset as u64 % WRITES_PER_CLIENT;
        let request = QueryRequest {
            meta: None,
            partition: 0,
            rql: "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile"
                .to_owned(),
            parameters: vec![NamedParameter {
                name: "id".to_owned(),
                value: Some(Value {
                    kind: Some(value::Kind::U64Value(id)),
                }),
            }],
            limit: 1,
        };
        let started = Instant::now();
        client.query(&request).await?;
        queries.push(elapsed_nanos(started));
    }

    client.close().await?;

    Ok(Workload {
        writes,
        reads,
        queries,
    })
}

fn put_request(id: u64) -> DocumentPutRequest {
    DocumentPutRequest {
        meta: Some(RequestMeta {
            idempotency_key: format!("bench-{id}").into_bytes(),
            ..Default::default()
        }),
        collection_id: 1,
        partition: 0,
        document_key: document_key(id),
        fragments: vec![FragmentPayload {
            fragment_id: 1,
            subkey: Vec::new(),
            fields: vec![
                FieldValue {
                    field_id: 1,
                    value: Some(Value {
                        kind: Some(value::Kind::StringValue(format!("player-{id}"))),
                    }),
                },
                FieldValue {
                    field_id: 2,
                    value: Some(Value {
                        kind: Some(value::Kind::U32Value(10)),
                    }),
                },
            ],
            if_match: None,
        }],
    }
}

fn document_key(id: u64) -> Vec<u8> {
    let schema = DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]);
    schema.encode(&[KeyValue::U64(id)]).expect("document key")
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).expect("elapsed fits u64")
}

fn report(label: &str, latencies: &[u64]) {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let total: u64 = sorted.iter().sum();
    let count = sorted.len() as u64;

    println!("{label}:");
    println!("  samples: {count}");
    println!("  mean:    {:.2} us", total as f64 / count as f64 / 1_000.0);
    println!(
        "  p50:     {:.2} us",
        percentile(&sorted, 50) as f64 / 1_000.0
    );
    println!(
        "  p95:     {:.2} us",
        percentile(&sorted, 95) as f64 / 1_000.0
    );
    println!(
        "  p99:     {:.2} us",
        percentile(&sorted, 99) as f64 / 1_000.0
    );
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

fn bench_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("recolith-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create bench dir");

    path
}
