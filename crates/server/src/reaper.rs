//! Background document expiry reaper.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use recolith_backend::StorageBackend;

use crate::document::DocumentStore;
use crate::metrics::ServerMetrics;

/// Runs periodic TTL sweeps until the task is aborted.
pub async fn run_ttl_reaper<B: StorageBackend>(
    store: Arc<DocumentStore<B>>,
    interval_secs: u64,
    batch_size: usize,
    metrics: Arc<ServerMetrics>,
) {
    if interval_secs == 0 || batch_size == 0 {
        return;
    }
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let store = Arc::clone(&store);
        let outcome =
            tokio::task::spawn_blocking(move || store.sweep_expired(now_micros(), batch_size))
                .await;
        match outcome {
            Ok(Ok(sweep)) => {
                metrics.ttl_sweep(sweep.deleted);
                if sweep.deleted > 0 {
                    tracing::info!(
                        scanned = sweep.scanned,
                        deleted = sweep.deleted,
                        "deleted expired documents"
                    );
                }
            }
            Ok(Err(error)) => {
                metrics.ttl_error();
                tracing::warn!(%error, "TTL sweep failed");
            }
            Err(error) => {
                metrics.ttl_error();
                tracing::warn!(%error, "TTL worker failed");
            }
        }
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
        })
}
