use std::sync::atomic::{AtomicU64, Ordering};

use recolith_common::CommitTs;

/// In-memory logical commit clock.
///
/// Persistence of the high-water mark belongs to the transaction/backend layer.
#[derive(Debug)]
pub struct CommitClock {
    high_water_mark: AtomicU64,
}

impl CommitClock {
    /// Creates a clock starting at `high_water_mark`.
    #[must_use]
    pub fn new(high_water_mark: CommitTs) -> Self {
        Self {
            high_water_mark: AtomicU64::new(high_water_mark.get()),
        }
    }

    /// Reserves the next commit timestamp.
    #[must_use]
    pub fn reserve(&self) -> CommitTs {
        let next = self.high_water_mark.fetch_add(1, Ordering::SeqCst) + 1;
        CommitTs::new(next)
    }

    /// Advances the clock to at least `observed`.
    pub fn observe(&self, observed: CommitTs) {
        self.high_water_mark
            .fetch_max(observed.get(), Ordering::SeqCst);
    }

    /// Returns the last reserved or observed timestamp.
    #[must_use]
    pub fn high_water_mark(&self) -> CommitTs {
        CommitTs::new(self.high_water_mark.load(Ordering::SeqCst))
    }
}

impl Default for CommitClock {
    fn default() -> Self {
        Self::new(CommitTs::new(0))
    }
}
