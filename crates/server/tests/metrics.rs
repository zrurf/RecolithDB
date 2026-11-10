use std::time::Duration;

use recolith_protocol::messages::opcode;
use recolith_server::ServerMetrics;

#[test]
fn latency_histograms_track_percentiles() {
    let metrics = ServerMetrics::new();
    for _ in 0..99 {
        metrics.observe_latency(opcode::PING, Duration::from_micros(100));
    }
    metrics.observe_latency(opcode::PING, Duration::from_micros(10_000));

    let latency = metrics.snapshot().ping_latency;
    assert_eq!(latency.count, 100);
    assert_eq!(latency.mean_micros, 199);
    assert_eq!(latency.p50_micros, 128);
    assert_eq!(latency.p95_micros, 128);
    assert_eq!(latency.p99_micros, 128);
    assert_eq!(latency.max_micros, 10_000);
}

#[test]
fn empty_latency_snapshot_is_zero() {
    let latency = ServerMetrics::new().snapshot().query_latency;

    assert_eq!(latency, Default::default());
}

#[test]
fn unknown_opcodes_do_not_observe_latency() {
    let metrics = ServerMetrics::new();
    metrics.observe_latency(opcode::HELLO, Duration::from_micros(10));

    assert_eq!(metrics.snapshot().ping_latency.count, 0);
    assert_eq!(metrics.snapshot().document_read_latency.count, 0);
}

#[test]
fn overflow_bucket_uses_maximum() {
    let metrics = ServerMetrics::new();
    metrics.observe_latency(opcode::DOC_GET, Duration::from_secs(10));

    let latency = metrics.snapshot().document_read_latency;
    assert_eq!(latency.count, 1);
    assert_eq!(latency.p99_micros, 10_000_000);
    assert_eq!(latency.max_micros, 10_000_000);
}
