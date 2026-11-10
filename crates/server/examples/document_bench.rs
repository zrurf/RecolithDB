//! Document write and read benchmark for the in-process store.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use recolith_backend::StorageBackend;
use recolith_backend_fjall::FjallBackend;
use recolith_backend_memory::MemoryBackend;
use recolith_common::{CollectionId, FieldId, FragmentId, PartitionId};
use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
use recolith_query::DocumentReadRequest;
use recolith_server::{DocumentStore, DocumentWrite, FragmentWriteRequest, Idempotency};
use recolith_storage::{RecordData, Value, encode_document_key};

const WRITES: u64 = 20_000;
const READS: usize = 100_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench("memory", Arc::new(MemoryBackend::new()))?;

    let dir = bench_dir("server-document-fjall");
    bench("fjall", Arc::new(FjallBackend::open(&dir)?))?;
    let _ = std::fs::remove_dir_all(&dir);

    Ok(())
}

fn bench<B: StorageBackend>(
    label: &str,
    backend: Arc<B>,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = DocumentStore::new(
        Arc::new(recolith_testkit::game_catalog()),
        Arc::clone(&backend),
    );

    let mut write_latencies = Vec::with_capacity(WRITES as usize);
    for index in 0..WRITES {
        let start = Instant::now();
        write_document(&store, index)?;
        write_latencies.push(elapsed_nanos(start));
    }

    let mut read_latencies = Vec::with_capacity(READS);
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..READS {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let index = state % WRITES;
        let start = Instant::now();
        read_document(&store, index)?;
        read_latencies.push(elapsed_nanos(start));
    }

    println!("{label} document write:");
    report(&write_latencies);
    println!("{label} document read:");
    report(&read_latencies);

    Ok(())
}

fn write_document<B: StorageBackend>(
    store: &DocumentStore<B>,
    index: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut profile = RecordData::new();
    profile.insert(FieldId::new(1), Value::String("player".to_owned()));
    profile.insert(FieldId::new(2), Value::U32(10));
    let request = DocumentWrite::new(
        CollectionId::new(1),
        document_key(index),
        PartitionId::new(0),
    )
    .with_fragment(FragmentWriteRequest::new(
        FragmentId::new(1),
        Vec::new(),
        profile,
    ))
    .with_idempotency(Idempotency::new(
        format!("bench-idempotency-{index:016}").into_bytes(),
        [0_u8; 32],
    ));
    store.write(&request)?;

    Ok(())
}

fn read_document<B: StorageBackend>(
    store: &DocumentStore<B>,
    index: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = DocumentReadRequest::new(
        CollectionId::new(1),
        document_key(index),
        PartitionId::new(0),
    )
    .with_fragment(FragmentId::new(1));
    store.read(&request)?;

    Ok(())
}

fn document_key(index: u64) -> Vec<u8> {
    let schema = DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]);
    encode_document_key(&schema, &[KeyValue::U64(index)]).expect("document key")
}

fn report(latencies: &[u64]) {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let total: u64 = sorted.iter().sum();
    let count = sorted.len() as u64;

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
    println!(
        "  rate:    {:.0} ops/s",
        count as f64 / (total as f64 / 1_000_000_000.0)
    );
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

fn elapsed_nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).expect("elapsed fits u64")
}

fn bench_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("recolith-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("create bench dir");

    path
}
