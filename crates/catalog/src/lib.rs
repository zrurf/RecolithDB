//! Local catalog metadata, schema registry, and placement records.
#![forbid(unsafe_code)]

pub mod catalog;
pub mod error;
pub mod placement;
pub mod schema_registry;
pub mod store;

pub use catalog::{Catalog, CatalogSnapshot};
pub use error::{CatalogError, Result};
pub use placement::BucketPlacement;
pub use schema_registry::{SchemaRegistry, SchemaSnapshot};
pub use store::{load_catalog, save_catalog};
