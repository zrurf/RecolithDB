//! Catalog bootstrap loading.

use std::path::Path;

use recolith_catalog::{Catalog, CatalogDefinition};

/// Catalog bootstrap loading failure.
pub use recolith_catalog::DefinitionError as BootstrapError;

/// Loads a catalog bootstrap file.
///
/// # Errors
///
/// Returns [`BootstrapError`] for I/O, TOML, schema, or catalog validation failures.
pub fn load_bootstrap(path: impl AsRef<Path>) -> Result<Catalog, BootstrapError> {
    CatalogDefinition::load(path)?.into_catalog()
}

/// Builds a catalog from bootstrap TOML.
///
/// # Errors
///
/// Returns [`BootstrapError`] for TOML, schema, or catalog validation failures.
pub fn parse_bootstrap(contents: &str) -> Result<Catalog, BootstrapError> {
    CatalogDefinition::parse(contents)?.into_catalog()
}
