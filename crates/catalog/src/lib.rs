//! Local catalog metadata, schema registry, and placement records.
#![forbid(unsafe_code)]

pub mod apply;
pub mod catalog;
pub mod definition;
pub mod drop;
pub mod error;
pub mod placement;
pub mod schema_registry;
pub mod store;

pub use apply::CatalogChanges;
pub use catalog::{Catalog, CatalogSnapshot};
pub use definition::{CatalogDefinition, DefinitionError, SchemaDefinition};
pub use drop::CatalogDrop;
pub use error::{CatalogError, Result};
pub use placement::BucketPlacement;
pub use schema_registry::{SchemaRegistry, SchemaSnapshot};
pub use store::{CatalogWrite, load_catalog, save_catalog};
