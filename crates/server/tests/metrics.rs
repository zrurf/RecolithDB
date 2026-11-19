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

#[test]
fn tracks_new_operation_counters_and_latency() {
    let metrics = ServerMetrics::new();
    let operations = [
        (opcode::DOC_DELETE, Duration::from_micros(50)),
        (opcode::EXPLAIN, Duration::from_micros(60)),
        (opcode::AGGREGATE, Duration::from_micros(70)),
        (opcode::BACKUP, Duration::from_micros(80)),
    ];
    for (operation, duration) in operations {
        metrics.request(operation);
        metrics.observe_latency(operation, duration);
    }

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.document_deletes, 1);
    assert_eq!(snapshot.explanations, 1);
    assert_eq!(snapshot.aggregates, 1);
    assert_eq!(snapshot.backups, 1);
    assert_eq!(snapshot.document_delete_latency.count, 1);
    assert_eq!(snapshot.document_delete_latency.mean_micros, 50);
    assert_eq!(snapshot.explain_latency.count, 1);
    assert_eq!(snapshot.explain_latency.mean_micros, 60);
    assert_eq!(snapshot.aggregate_latency.count, 1);
    assert_eq!(snapshot.aggregate_latency.mean_micros, 70);
    assert_eq!(snapshot.backup_latency.count, 1);
    assert_eq!(snapshot.backup_latency.mean_micros, 80);
}

#[test]
fn tracks_bucket_operation_counters_and_latency() {
    let metrics = ServerMetrics::new();
    let operations = [
        opcode::BUCKET_GET,
        opcode::BUCKET_PUT,
        opcode::BUCKET_DELETE,
        opcode::BUCKET_SCAN,
    ];
    for operation in operations {
        metrics.request(operation);
        metrics.observe_latency(operation, Duration::from_micros(40));
    }

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.bucket_ops, 4);
    assert_eq!(snapshot.bucket_get_latency.count, 1);
    assert_eq!(snapshot.bucket_put_latency.count, 1);
    assert_eq!(snapshot.bucket_delete_latency.count, 1);
    assert_eq!(snapshot.bucket_scan_latency.count, 1);
    assert_eq!(snapshot.bucket_get_latency.p99_micros, 64);
}
