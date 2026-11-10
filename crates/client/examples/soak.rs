//! Mixed-workload soak benchmark over the wire protocol.
//!
//! Environment variables: `SOAK_SECONDS` and `SOAK_CLIENTS`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use recolith_backend_fjall::FjallBackend;
use recolith_client::{Client, ClientConfig, ClientError};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_protocol::messages::{
    DocumentGetRequest, DocumentPutRequest, FieldValue, FragmentPayload, NamedParameter,
    QueryRequest, RequestMeta, Value, value,
};
use recolith_server::{DocumentStore, MetricsSnapshot, ServerConfig, ServerMetrics, serve};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Ping,
    Get,
    Put,
    Query,
}

#[derive(Default)]
struct Latencies {
    ping: Vec<u64>,
    get: Vec<u64>,
    put: Vec<u64>,
    query: Vec<u64>,
}

impl Latencies {
    fn push(&mut self, operation: Operation, latency: u64) {
        match operation {
            Operation::Ping => self.ping.push(latency),
            Operation::Get => self.get.push(latency),
            Operation::Put => self.put.push(latency),
            Operation::Query => self.query.push(latency),
        }
    }

    fn extend(&mut self, other: Self) {
        self.ping.extend(other.ping);
        self.get.extend(other.get);
        self.put.extend(other.put);
        self.query.extend(other.query);
    }
}

#[derive(Default)]
struct Samples {
    first_half: Latencies,
    second_half: Latencies,
    errors: u64,
}

impl Samples {
    fn record(&mut self, operation: Operation, latency: u64, first_half: bool) {
        if first_half {
            self.first_half.push(operation, latency);
        } else {
            self.second_half.push(operation, latency);
        }
    }

    fn extend(&mut self, other: Self) {
        self.first_half.extend(other.first_half);
        self.second_half.extend(other.second_half);
        self.errors += other.errors;
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let seconds = env_u64("SOAK_SECONDS", 30).max(1);
    let clients = env_u64("SOAK_CLIENTS", 16).max(1);
    let duration = Duration::from_secs(seconds);

    let dir = bench_dir("soak-fjall");
    let backend = Arc::new(FjallBackend::open(&dir)?);
    let store = Arc::new(DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        backend,
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = ServerConfig::new(address);
    let metrics = config.metrics();
    let server = tokio::spawn(serve(listener, config, store));
    let endpoint = address.to_string();

    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..clients {
        tasks.spawn(worker_load(worker, duration, endpoint.clone()));
    }
    let mut samples = Samples::default();
    while let Some(joined) = tasks.join_next().await {
        samples.extend(joined??);
    }
    let elapsed = started.elapsed();
    wait_for_idle(&metrics).await;

    server.abort();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);

    report(seconds, clients, elapsed, &samples, &metrics.snapshot());

    Ok(())
}

async fn worker_load(
    worker: u64,
    duration: Duration,
    endpoint: String,
) -> Result<Samples, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = ClientConfig::new(endpoint);
    config.client_id = format!("soak-{worker}");
    let mut client = Client::connect(config).await?;
    let base = (worker + 1) * 10_000_000;
    let started = Instant::now();
    let half = duration / 2;
    let mut samples = Samples::default();
    let mut sequence = 0_u64;
    let mut written = 0_u64;
    let mut last_id = base;

    while started.elapsed() < duration {
        let operation = match sequence % 10 {
            0..=3 => Operation::Put,
            4..=7 if written > 0 => Operation::Get,
            8 if written > 0 => Operation::Query,
            _ => Operation::Ping,
        };
        let id = if operation == Operation::Put {
            base + sequence
        } else {
            last_id
        };
        let latency_started = Instant::now();
        let result = match operation {
            Operation::Ping => client.ping(b"soak").await.map(|_| ()),
            Operation::Get => {
                client
                    .document_get(&DocumentGetRequest {
                        meta: None,
                        collection_id: 1,
                        partition: 0,
                        document_key: document_key(id),
                        fragments: vec![1],
                        projection: Vec::new(),
                    })
                    .await
                    .map(|_| ())
            }
            Operation::Put => {
                client
                    .document_put(&put_request(id, worker, sequence))
                    .await
                    .map(|_| ())
            }
            Operation::Query => {
                client
                    .query(&QueryRequest {
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
                    })
                    .await
                    .map(|_| ())
            }
        };
        let first_half = started.elapsed() < half;
        samples.record(operation, elapsed_nanos(latency_started), first_half);
        match result {
            Ok(()) => {
                if operation == Operation::Put {
                    last_id = id;
                    written += 1;
                }
            }
            Err(ClientError::Server { .. }) => samples.errors += 1,
            Err(_) => {
                samples.errors += 1;
                break;
            }
        }
        sequence += 1;
    }

    let _ = client.close().await;

    Ok(samples)
}

fn put_request(id: u64, worker: u64, sequence: u64) -> DocumentPutRequest {
    DocumentPutRequest {
        meta: Some(RequestMeta {
            idempotency_key: format!("soak-{worker}-{sequence}").into_bytes(),
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

fn report(
    seconds: u64,
    clients: u64,
    elapsed: Duration,
    samples: &Samples,
    server: &MetricsSnapshot,
) {
    let operations = samples.first_half.count() + samples.second_half.count();
    println!("soak:");
    println!("  seconds:   {seconds}");
    println!("  clients:   {clients}");
    println!("  operations:{operations}");
    println!("  errors:    {}", samples.errors);
    println!("  wall:      {:.3} s", elapsed.as_secs_f64());
    println!(
        "  rate:      {:.0} ops/s",
        operations as f64 / elapsed.as_secs_f64()
    );

    println!("client latency:");
    println!(
        "  ping p99:  {} -> {} us",
        percentile(&samples.first_half.ping, 99),
        percentile(&samples.second_half.ping, 99)
    );
    println!(
        "  get p99:   {} -> {} us",
        percentile(&samples.first_half.get, 99),
        percentile(&samples.second_half.get, 99)
    );
    println!(
        "  put p99:   {} -> {} us",
        percentile(&samples.first_half.put, 99),
        percentile(&samples.second_half.put, 99)
    );
    println!(
        "  query p99: {} -> {} us",
        percentile(&samples.first_half.query, 99),
        percentile(&samples.second_half.query, 99)
    );

    println!("server:");
    println!(
        "  sessions:  {} accepted, {} active",
        server.accepted_sessions, server.active_sessions
    );
    println!("  requests:  {}", server.requests);
    println!("  errors:    {}", server.errors);
    println!("  ping p99:  {} us", server.ping_latency.p99_micros);
    println!(
        "  read p99:  {} us",
        server.document_read_latency.p99_micros
    );
    println!(
        "  write p99: {} us",
        server.document_write_latency.p99_micros
    );
    println!("  query p99: {} us", server.query_latency.p99_micros);
}

impl Latencies {
    fn count(&self) -> u64 {
        (self.ping.len() + self.get.len() + self.put.len() + self.query.len()) as u64
    }
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index] / 1_000
}

async fn wait_for_idle(metrics: &ServerMetrics) {
    for _ in 0..200 {
        if metrics.snapshot().active_sessions == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("sessions did not drain: {:?}", metrics.snapshot());
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).expect("elapsed fits u64")
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn bench_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("recolith-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create bench dir");

    path
}
