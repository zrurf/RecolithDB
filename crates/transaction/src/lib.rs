//! Local transactions, atomic operations, unique reservations, and workflows.
#![forbid(unsafe_code)]

pub mod atomic;
pub mod clock;
pub mod error;
pub mod local;
pub mod local_write;
pub mod unique;

pub use atomic::{AtomicOp, apply_atomic};
pub use clock::{
    BatchedCommitClock, DEFAULT_CLOCK_KEY, DEFAULT_TIMESTAMP_BATCH, PersistentCommitClock,
};
pub use error::{Result, TransactionError};
pub use local::LocalTransaction;
pub use local_write::{FragmentWrite, IndexUpdate, LocalWritePath};
pub use unique::{ReservationState, UniqueReservation};
