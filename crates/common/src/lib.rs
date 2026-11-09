//! Shared identifiers, errors, byte helpers, time, URI, and version types.
#![forbid(unsafe_code)]

pub mod bytes;
pub mod error;
pub mod ids;
pub mod time;
pub mod uri;
pub mod version;

pub use error::{Error, Result};
pub use ids::{
    BackendId, BucketId, CatalogGeneration, CollectionId, CommitTs, FieldId, FragmentId, IndexId,
    KeyspaceId, PartitionId, PlacementGroupId, SchemaId, SchemaVersion, TxnId,
};
pub use time::Timestamp;
pub use uri::Uri;
pub use version::ProtocolVersion;
