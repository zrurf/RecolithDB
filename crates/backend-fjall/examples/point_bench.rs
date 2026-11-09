//! Point read/write latency measurement for the fjall backend.

use std::path::PathBuf;
use std::time::Instant;

use recolith_backend::{Durability, Snapshot, StorageBackend, Transaction, TxnOptions};
use recolith_backend_fjall::FjallBackend;

const WRITES: usize = 20_000;
const READS: usize = 100_000;
const SYNC_WRITES: usize = 2_000;
const VALUE_BYTES: usize = 256;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = bench_dir("fjall-point");
    let backend = FjallBackend::open(&dir)?;

    let write_latencies = measure_writes(&backend, Durability::Async, WRITES)?;
    backend.persist()?;
    report("write (async durability)", &write_latencies);

    let sync_latencies = measure_writes(&backend, Durability::Sync, SYNC_WRITES)?;
    report("write (sync durability)", &sync_latencies);

    let read_latencies = measure_reads(&backend)?;
    report("point read", &read_latencies);

    drop(backend);
    let _ = std::fs::remove_dir_all(&dir);

    Ok(())
}

fn measure_writes(
    backend: &FjallBackend,
    durability: Durability,
    count: usize,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    for index in 0..1_000 {
        write(backend, index, durability)?;
    }

    let mut latencies = Vec::with_capacity(count);
    for index in 0..count {
        let start = Instant::now();
        write(backend, index, durability)?;
        latencies.push(elapsed_nanos(start));
    }

    Ok(latencies)
}

fn write(
    backend: &FjallBackend,
    index: usize,
    durability: Durability,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut transaction = backend.begin(TxnOptions { durability })?;
    transaction.put(key(index).as_bytes(), vec![0x5A; VALUE_BYTES]);
    transaction.commit()?;

    Ok(())
}

fn measure_reads(backend: &FjallBackend) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut latencies = Vec::with_capacity(READS);

    for _ in 0..READS {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let index = (state as usize) % WRITES;
        let key = key(index);
        let start = Instant::now();
        let snapshot = backend.snapshot()?;
        let value = snapshot.get(key.as_bytes())?;
        latencies.push(elapsed_nanos(start));
        assert_eq!(value.map(|value| value.len()), Some(VALUE_BYTES));
    }

    Ok(latencies)
}

fn key(index: usize) -> String {
    format!("bench:write:{index:016}")
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
    println!(
        "  max:     {:.2} us",
        *sorted.last().expect("sample") as f64 / 1_000.0
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
