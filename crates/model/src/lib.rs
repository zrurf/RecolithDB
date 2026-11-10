//! Logical data model types.
#![forbid(unsafe_code)]

pub mod bucket;
pub mod collection;
pub mod document;
pub mod fragment;
pub mod index;
pub mod key;
pub mod keyspace;
pub mod partition;
pub mod policy;
pub mod record;

pub use bucket::{Bucket, BucketPolicy};
pub use collection::Collection;
pub use document::{Document, DocumentKey};
pub use fragment::{Fragment, FragmentCardinality};
pub use index::{IndexDefinition, IndexKind};
pub use key::{
    DocumentKeyError, DocumentKeySchema, KeyField, KeyType, KeyValue, MAX_DOCUMENT_KEY_LEN,
};
pub use keyspace::Keyspace;
pub use partition::BucketPartition;
pub use policy::Consistency;
pub use record::Record;
