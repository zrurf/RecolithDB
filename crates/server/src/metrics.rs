use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use recolith_protocol::messages::opcode;

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
    /// Completed TTL reaper sweeps.
    pub ttl_sweeps: u64,
    /// Expired documents deleted by the reaper.
    pub expired_documents: u64,
    /// TTL reaper failures.
    pub ttl_errors: u64,
    /// PING latency.
    pub ping_latency: LatencySnapshot,
    /// DOC_GET latency.
    pub document_read_latency: LatencySnapshot,
    /// DOC_PUT latency.
    pub document_write_latency: LatencySnapshot,
    /// QUERY latency.
    pub query_latency: LatencySnapshot,
    /// DESCRIBE_COLLECTION latency.
    pub metadata_latency: LatencySnapshot,
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
    query: LatencyHistogram,
    metadata: LatencyHistogram,
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
    ttl_sweeps: AtomicU64,
    expired_documents: AtomicU64,
    ttl_errors: AtomicU64,
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
            opcode::QUERY => self.queries.fetch_add(1, Ordering::Relaxed),
            opcode::DESCRIBE_COLLECTION => self.metadata_requests.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// Records the latency of a completed operation.
    pub fn observe_latency(&self, opcode: u16, duration: Duration) {
        let histogram = match opcode {
            opcode::PING => &self.latency.ping,
            opcode::DOC_GET => &self.latency.document_read,
            opcode::DOC_PUT => &self.latency.document_write,
            opcode::QUERY => &self.latency.query,
            opcode::DESCRIBE_COLLECTION => &self.latency.metadata,
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
            ttl_sweeps: self.ttl_sweeps.load(Ordering::Relaxed),
            expired_documents: self.expired_documents.load(Ordering::Relaxed),
            ttl_errors: self.ttl_errors.load(Ordering::Relaxed),
            ping_latency: self.latency.ping.snapshot(),
            document_read_latency: self.latency.document_read.snapshot(),
            document_write_latency: self.latency.document_write.snapshot(),
            query_latency: self.latency.query.snapshot(),
            metadata_latency: self.latency.metadata.snapshot(),
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
