//! Server composition root, in-process document services, and TCP sessions.
#![forbid(unsafe_code)]

pub mod document;
pub mod net;

pub use document::{DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest};
pub use net::{SERVER_VERSION, ServerConfig, TlsServerConfig, serve};
