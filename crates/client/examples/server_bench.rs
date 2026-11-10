//! Concurrent server benchmark over the wire protocol.
//!
//! Environment variables: `BENCH_CLIENTS`, `BENCH_WRITES`, `BENCH_READS`,
//! `BENCH_QUERIES`, `BENCH_RUNS`, and `BENCH_WARMUP`.

use std::sync::Arc;
use std::time::{Duration, Instant};

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

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    clients: u64,
    writes: u64,
    reads: u64,
    queries: u64,
    runs: u64,
    warmup: u64,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            clients: env_u64("BENCH_CLIENTS", 8).max(1),
            writes: env_u64("BENCH_WRITES", 500).max(1),
            reads: env_u64("BENCH_READS", 2_000),
            queries: env_u64("BENCH_QUERIES", 250),
            runs: env_u64("BENCH_RUNS", 3).max(1),
            warmup: env_u64("BENCH_WARMUP", 1),
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = BenchConfig::from_env();
    let dir = bench_dir("server-concurrent-fjall");
    let backend = Arc::new(FjallBackend::open(&dir)?);
    let store = Arc::new(DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        backend,
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(serve(listener, ServerConfig::new(address), store));
    let endpoint = address.to_string();

    let mut measured = Vec::new();
    for run in 0..config.warmup.saturating_add(config.runs) {
        let started = Instant::now();
        let mut tasks = JoinSet::new();
        for worker in 0..config.clients {
            tasks.spawn(workload(worker, run, endpoint.clone(), config));
        }

        let mut result = RunResult::default();
        while let Some(joined) = tasks.join_next().await {
            let workload = joined??;
            result.writes.extend(workload.writes);
            result.reads.extend(workload.reads);
            result.queries.extend(workload.queries);
        }
        result.elapsed = started.elapsed();
        if run >= config.warmup {
            measured.push(result);
        }
    }

    server.abort();
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);

    report(&config, &measured);

    Ok(())
}

struct Workload {
    writes: Vec<u64>,
    reads: Vec<u64>,
    queries: Vec<u64>,
}

#[derive(Default)]
struct RunResult {
    writes: Vec<u64>,
    reads: Vec<u64>,
    queries: Vec<u64>,
    elapsed: Duration,
}

async fn workload(
    worker: u64,
    run: u64,
    endpoint: String,
    config: BenchConfig,
) -> Result<Workload, Box<dyn std::error::Error + Send + Sync>> {
    let mut client_config = ClientConfig::new(endpoint);
    client_config.client_id = format!("bench-{run}-{worker}");
    let mut client = Client::connect(client_config).await?;
    let base = run * config.clients * config.writes + worker * config.writes;

    let mut writes = Vec::with_capacity(usize::try_from(config.writes).unwrap_or(usize::MAX));
    for offset in 0..config.writes {
        let request = put_request(base + offset);
        let started = Instant::now();
        client.document_put(&request).await?;
        writes.push(elapsed_nanos(started));
    }

    let mut reads = Vec::with_capacity(usize::try_from(config.reads).unwrap_or(usize::MAX));
    let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ worker ^ run;
    for _ in 0..config.reads {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let id = base + state % config.writes;
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

    let mut queries = Vec::with_capacity(usize::try_from(config.queries).unwrap_or(usize::MAX));
    for offset in 0..config.queries {
        let id = base + offset % config.writes;
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
            cursor: Vec::new(),
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

fn report(config: &BenchConfig, runs: &[RunResult]) {
    let mut writes = Vec::new();
    let mut reads = Vec::new();
    let mut queries = Vec::new();
    let mut throughputs = Vec::with_capacity(runs.len());
    for run in runs {
        writes.extend(&run.writes);
        reads.extend(&run.reads);
        queries.extend(&run.queries);
        let operations = run.writes.len() + run.reads.len() + run.queries.len();
        throughputs.push(operations as f64 / run.elapsed.as_secs_f64());
    }

    println!(
        "runs: {} measured, {} warmup; clients: {}",
        config.runs, config.warmup, config.clients
    );
    report_latency("document write", &writes);
    report_latency("document read", &reads);
    report_latency("point query", &queries);

    let throughput = summarize(&mut throughputs);
    println!("run throughput:");
    println!("  median:  {:.0} ops/s", throughput.median);
    println!(
        "  min/max: {:.0} / {:.0} ops/s",
        throughput.min, throughput.max
    );
}

fn report_latency(label: &str, latencies: &[u64]) {
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

struct Summary {
    median: f64,
    min: f64,
    max: f64,
}

fn summarize(values: &mut [f64]) -> Summary {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    let median = if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    };

    Summary {
        median,
        min: values[0],
        max: values[values.len() - 1],
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
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
