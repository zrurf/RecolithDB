//! In-memory storage backend for tests and local development.
#![forbid(unsafe_code)]

mod engine;

pub use engine::{MemoryBackend, MemorySnapshot, MemoryTransaction};
