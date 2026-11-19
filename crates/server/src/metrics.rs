use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use recolith_protocol::messages::{WorkflowState, opcode};

const LATENCY_BUCKET_MICROS: [u64; 22] = [
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536,
    131_072, 262_144, 524_288, 1_048_576, 4_194_304,
];
const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_MICROS.len();

/// Approximate latency summary for one operation class.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LatencySnapshot {
    /// Observed operations.
    pub count: u64,
    /// Mean latency in microseconds.
    pub mean_micros: u64,
    /// Approximate p50 latency in microseconds.
    pub p50_micros: u64,
    /// Approximate p95 latency in microseconds.
    pub p95_micros: u64,
    /// Approximate p99 latency in microseconds.
    pub p99_micros: u64,
    /// Maximum observed latency in microseconds.
    pub max_micros: u64,
}

/// Point-in-time metrics snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetricsSnapshot {
    /// TCP sessions accepted.
    pub accepted_sessions: u64,
    /// Connections rejected by the connection limit.
    pub rejected_connections: u64,
    /// Sessions currently running.
    pub active_sessions: u64,
    /// Sessions that ended with a transport or protocol error.
    pub session_errors: u64,
    /// Logical requests processed.
    pub requests: u64,
    /// Error responses sent.
    pub errors: u64,
    /// PING requests.
    pub pings: u64,
    /// DOC_GET requests.
    pub document_reads: u64,
    /// DOC_PUT requests.
    pub document_writes: u64,
    /// QUERY requests.
    pub queries: u64,
    /// DESCRIBE_COLLECTION requests.
    pub metadata_requests: u64,
    /// DOC_DELETE requests.
    pub document_deletes: u64,
    /// EXPLAIN requests.
    pub explanations: u64,
    /// AGGREGATE requests.
    pub aggregates: u64,
    /// BACKUP requests.
    pub backups: u64,
    /// Completed TTL reaper sweeps.
    pub ttl_sweeps: u64,
    /// Expired documents deleted by the reaper.
    pub expired_documents: u64,
    /// TTL reaper failures.
    pub ttl_errors: u64,
    /// Workflows accepted.
    pub workflows_started: u64,
    /// Workflows that completed all steps.
    pub workflows_completed: u64,
    /// Workflows compensated after a failure or cancellation.
    pub workflows_compensated: u64,
    /// Workflows moved to dead-letter state.
    pub workflows_failed: u64,
    /// Direct bucket record operations (BUCKET_GET/PUT/DELETE/SCAN).
    pub bucket_ops: u64,
    /// PING latency.
    pub ping_latency: LatencySnapshot,
    /// DOC_GET latency.
    pub document_read_latency: LatencySnapshot,
    /// DOC_PUT latency.
    pub document_write_latency: LatencySnapshot,
    /// DOC_DELETE latency.
    pub document_delete_latency: LatencySnapshot,
    /// QUERY latency.
    pub query_latency: LatencySnapshot,
    /// EXPLAIN latency.
    pub explain_latency: LatencySnapshot,
    /// AGGREGATE latency.
    pub aggregate_latency: LatencySnapshot,
    /// BACKUP latency.
    pub backup_latency: LatencySnapshot,
    /// DESCRIBE_COLLECTION latency.
    pub metadata_latency: LatencySnapshot,
    /// BUCKET_GET latency.
    pub bucket_get_latency: LatencySnapshot,
    /// BUCKET_PUT latency.
    pub bucket_put_latency: LatencySnapshot,
    /// BUCKET_DELETE latency.
    pub bucket_delete_latency: LatencySnapshot,
    /// BUCKET_SCAN latency.
    pub bucket_scan_latency: LatencySnapshot,
}

#[derive(Debug, Default)]
struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    overflow: AtomicU64,
    count: AtomicU64,
    sum_micros: AtomicU64,
    max_micros: AtomicU64,
}

impl LatencyHistogram {
    fn observe(&self, duration: Duration) {
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        match LATENCY_BUCKET_MICROS
            .iter()
            .position(|limit| micros <= *limit)
        {
            Some(index) => {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.overflow.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        add_saturating(&self.sum_micros, micros);
        self.max_micros.fetch_max(micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencySnapshot {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return LatencySnapshot::default();
        }
        let sum = self.sum_micros.load(Ordering::Relaxed);

        LatencySnapshot {
            count,
            mean_micros: sum / count,
            p50_micros: self.percentile(50),
            p95_micros: self.percentile(95),
            p99_micros: self.percentile(99),
            max_micros: self.max_micros.load(Ordering::Relaxed),
        }
    }

    fn percentile(&self, percentile: u64) -> u64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        let target = count.saturating_mul(percentile).saturating_add(99) / 100;
        let mut cumulative = 0;
        for (index, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return LATENCY_BUCKET_MICROS[index];
            }
        }
        self.max_micros.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
struct OperationLatency {
    ping: LatencyHistogram,
    document_read: LatencyHistogram,
    document_write: LatencyHistogram,
    document_delete: LatencyHistogram,
    query: LatencyHistogram,
    explain: LatencyHistogram,
    aggregate: LatencyHistogram,
    backup: LatencyHistogram,
    metadata: LatencyHistogram,
    bucket_get: LatencyHistogram,
    bucket_put: LatencyHistogram,
    bucket_delete: LatencyHistogram,
    bucket_scan: LatencyHistogram,
}

/// Process-local server counters and latency histograms.
#[derive(Debug, Default)]
pub struct ServerMetrics {
    accepted_sessions: AtomicU64,
    rejected_connections: AtomicU64,
    active_sessions: AtomicU64,
    session_errors: AtomicU64,
    requests: AtomicU64,
    errors: AtomicU64,
    pings: AtomicU64,
    document_reads: AtomicU64,
    document_writes: AtomicU64,
    queries: AtomicU64,
    metadata_requests: AtomicU64,
    document_deletes: AtomicU64,
    explanations: AtomicU64,
    aggregates: AtomicU64,
    backups: AtomicU64,
    ttl_sweeps: AtomicU64,
    expired_documents: AtomicU64,
    ttl_errors: AtomicU64,
    workflows_started: AtomicU64,
    workflows_completed: AtomicU64,
    workflows_compensated: AtomicU64,
    workflows_failed: AtomicU64,
    bucket_ops: AtomicU64,
    latency: OperationLatency,
}

impl ServerMetrics {
    /// Creates an empty metrics collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an accepted session.
    pub fn session_accepted(&self) {
        self.accepted_sessions.fetch_add(1, Ordering::Relaxed);
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a connection rejected by the connection limit.
    pub fn connection_rejected(&self) {
        self.rejected_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a finished session.
    pub fn session_finished(&self) {
        let mut active = self.active_sessions.load(Ordering::Relaxed);
        while active > 0 {
            match self.active_sessions.compare_exchange_weak(
                active,
                active - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }
    }

    /// Records a session that failed due to transport or protocol errors.
    pub fn session_error(&self) {
        self.session_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a logical request.
    pub fn request(&self, opcode: u16) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match opcode {
            opcode::PING => self.pings.fetch_add(1, Ordering::Relaxed),
            opcode::DOC_GET => self.document_reads.fetch_add(1, Ordering::Relaxed),
            opcode::DOC_PUT => self.document_writes.fetch_add(1, Ordering::Relaxed),
            opcode::DOC_DELETE => self.document_deletes.fetch_add(1, Ordering::Relaxed),
            opcode::QUERY => self.queries.fetch_add(1, Ordering::Relaxed),
            opcode::EXPLAIN => self.explanations.fetch_add(1, Ordering::Relaxed),
            opcode::AGGREGATE => self.aggregates.fetch_add(1, Ordering::Relaxed),
            opcode::BACKUP => self.backups.fetch_add(1, Ordering::Relaxed),
            opcode::DESCRIBE_COLLECTION => self.metadata_requests.fetch_add(1, Ordering::Relaxed),
            opcode::BUCKET_GET
            | opcode::BUCKET_PUT
            | opcode::BUCKET_DELETE
            | opcode::BUCKET_SCAN => self.bucket_ops.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// Records the latency of a completed operation.
    pub fn observe_latency(&self, opcode: u16, duration: Duration) {
        let histogram = match opcode {
            opcode::PING => &self.latency.ping,
            opcode::DOC_GET => &self.latency.document_read,
            opcode::DOC_PUT => &self.latency.document_write,
            opcode::DOC_DELETE => &self.latency.document_delete,
            opcode::QUERY => &self.latency.query,
            opcode::EXPLAIN => &self.latency.explain,
            opcode::AGGREGATE => &self.latency.aggregate,
            opcode::BACKUP => &self.latency.backup,
            opcode::DESCRIBE_COLLECTION => &self.latency.metadata,
            opcode::BUCKET_GET => &self.latency.bucket_get,
            opcode::BUCKET_PUT => &self.latency.bucket_put,
            opcode::BUCKET_DELETE => &self.latency.bucket_delete,
            opcode::BUCKET_SCAN => &self.latency.bucket_scan,
            _ => return,
        };
        histogram.observe(duration);
    }

    /// Records an error response.
    pub fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one completed TTL sweep.
    pub fn ttl_sweep(&self, deleted: u64) {
        self.ttl_sweeps.fetch_add(1, Ordering::Relaxed);
        add_saturating(&self.expired_documents, deleted);
    }

    /// Records a failed TTL sweep.
    pub fn ttl_error(&self) {
        self.ttl_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one accepted workflow.
    pub fn workflow_started(&self) {
        self.workflows_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one workflow reaching a terminal state.
    pub fn workflow_finished(&self, state: WorkflowState) {
        match state {
            WorkflowState::Completed => {
                self.workflows_completed.fetch_add(1, Ordering::Relaxed);
            }
            WorkflowState::Compensated => {
                self.workflows_compensated.fetch_add(1, Ordering::Relaxed);
            }
            WorkflowState::Failed => {
                self.workflows_failed.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Takes a consistent-enough point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            accepted_sessions: self.accepted_sessions.load(Ordering::Relaxed),
            rejected_connections: self.rejected_connections.load(Ordering::Relaxed),
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            session_errors: self.session_errors.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            pings: self.pings.load(Ordering::Relaxed),
            document_reads: self.document_reads.load(Ordering::Relaxed),
            document_writes: self.document_writes.load(Ordering::Relaxed),
            queries: self.queries.load(Ordering::Relaxed),
            metadata_requests: self.metadata_requests.load(Ordering::Relaxed),
            document_deletes: self.document_deletes.load(Ordering::Relaxed),
            explanations: self.explanations.load(Ordering::Relaxed),
            aggregates: self.aggregates.load(Ordering::Relaxed),
            backups: self.backups.load(Ordering::Relaxed),
            ttl_sweeps: self.ttl_sweeps.load(Ordering::Relaxed),
            expired_documents: self.expired_documents.load(Ordering::Relaxed),
            ttl_errors: self.ttl_errors.load(Ordering::Relaxed),
            workflows_started: self.workflows_started.load(Ordering::Relaxed),
            workflows_completed: self.workflows_completed.load(Ordering::Relaxed),
            workflows_compensated: self.workflows_compensated.load(Ordering::Relaxed),
            workflows_failed: self.workflows_failed.load(Ordering::Relaxed),
            bucket_ops: self.bucket_ops.load(Ordering::Relaxed),
            ping_latency: self.latency.ping.snapshot(),
            document_read_latency: self.latency.document_read.snapshot(),
            document_write_latency: self.latency.document_write.snapshot(),
            document_delete_latency: self.latency.document_delete.snapshot(),
            query_latency: self.latency.query.snapshot(),
            explain_latency: self.latency.explain.snapshot(),
            aggregate_latency: self.latency.aggregate.snapshot(),
            backup_latency: self.latency.backup.snapshot(),
            metadata_latency: self.latency.metadata.snapshot(),
            bucket_get_latency: self.latency.bucket_get.snapshot(),
            bucket_put_latency: self.latency.bucket_put.snapshot(),
            bucket_delete_latency: self.latency.bucket_delete.snapshot(),
            bucket_scan_latency: self.latency.bucket_scan.snapshot(),
        }
    }
}

fn add_saturating(atomic: &AtomicU64, value: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(value);
        match atomic.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

pub(crate) struct SessionGuard {
    metrics: Arc<ServerMetrics>,
}

impl SessionGuard {
    pub(crate) fn new(metrics: Arc<ServerMetrics>) -> Self {
        metrics.session_accepted();
        Self { metrics }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.metrics.session_finished();
    }
}
