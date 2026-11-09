//! Server composition root and in-process document services.
#![forbid(unsafe_code)]

pub mod document;

pub use document::{DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest};
