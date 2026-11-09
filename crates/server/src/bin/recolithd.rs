use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use recolith_backend::StorageBackend;
use recolith_catalog::{Catalog, CatalogError, load_catalog, save_catalog};
use recolith_server::{
    BootstrapError, Config, ConfigError, DocumentStore, ServerConfig, StorageSettings,
    load_bootstrap, serve_with_shutdown,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "recolithd", version, about = "RecolithDB server")]
struct Args {
    /// Path to the server TOML configuration.
    #[arg(long, default_value = "recolith.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("recolithd: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), RuntimeError> {
    let config = Config::load(&args.config)?;
    let server_config = config.server_config()?;

    match &config.storage {
        StorageSettings::Memory => {
            #[cfg(feature = "backend-memory")]
            {
                let backend = Arc::new(recolith_backend_memory::MemoryBackend::new());
                run_backend(&config, server_config, backend).await
            }
            #[cfg(not(feature = "backend-memory"))]
            {
                Err(RuntimeError::Message(
                    "the memory backend is not compiled in".to_owned(),
                ))
            }
        }
        StorageSettings::Fjall { path } => {
            #[cfg(feature = "backend-fjall")]
            {
                let backend = Arc::new(recolith_backend_fjall::FjallBackend::open(path)?);
                run_backend(&config, server_config, backend).await
            }
            #[cfg(not(feature = "backend-fjall"))]
            {
                let _ = path;
                Err(RuntimeError::Message(
                    "the fjall backend is not compiled in".to_owned(),
                ))
            }
        }
    }
}

async fn run_backend<B: StorageBackend>(
    config: &Config,
    server_config: ServerConfig,
    backend: Arc<B>,
) -> Result<(), RuntimeError> {
    let catalog = load_catalog(backend.as_ref())?;
    let catalog = if catalog_is_empty(&catalog)? {
        let bootstrap_path = config.catalog.bootstrap.as_ref().ok_or_else(|| {
            RuntimeError::Message(
                "catalog is empty; set catalog.bootstrap in the configuration".to_owned(),
            )
        })?;
        let bootstrap = load_bootstrap(bootstrap_path)?;
        save_catalog(backend.as_ref(), &bootstrap)?;
        tracing::info!(path = %bootstrap_path.display(), "catalog bootstrap applied");
        bootstrap
    } else {
        if config.catalog.bootstrap.is_some() {
            tracing::info!("catalog already contains metadata; bootstrap ignored");
        }
        catalog
    };

    let store = Arc::new(DocumentStore::new(Arc::new(catalog), backend));
    let listener = TcpListener::bind(server_config.listen).await?;
    let address = listener.local_addr()?;
    tracing::info!(%address, "server listening");

    serve_with_shutdown(listener, server_config, store, async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for shutdown signal");
        }
    })
    .await?;

    tracing::info!("server stopped");

    Ok(())
}

fn catalog_is_empty(catalog: &Catalog) -> Result<bool, CatalogError> {
    let snapshot = catalog.snapshot()?;

    Ok(snapshot.keyspaces.is_empty()
        && snapshot.buckets.is_empty()
        && snapshot.collections.is_empty()
        && snapshot.indexes.is_empty()
        && snapshot.schemas.schemas.is_empty())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[derive(Debug, Error)]
enum RuntimeError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Backend(#[from] recolith_backend::BackendError),
    #[error("{0}")]
    Message(String),
}
