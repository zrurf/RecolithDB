use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use recolith_backend_fjall::FjallBackend;
use recolith_server::{backup, restore, verify};

#[derive(Debug, Parser)]
#[command(
    name = "recolith-backup",
    version,
    about = "Offline backup and restore"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Writes an offline logical backup. Stop the server first.
    Backup {
        /// Fjall data directory.
        #[arg(long)]
        data_dir: PathBuf,
        /// Backup output file.
        #[arg(long)]
        output: PathBuf,
    },
    /// Restores a backup into an empty data directory. Stop the server first.
    Restore {
        /// Fjall data directory.
        #[arg(long)]
        data_dir: PathBuf,
        /// Backup input file.
        #[arg(long)]
        input: PathBuf,
    },
    /// Validates a backup file without restoring it.
    Verify {
        /// Backup input file.
        #[arg(long)]
        input: PathBuf,
    },
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("recolith-backup: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match args.command {
        Command::Backup { data_dir, output } => {
            let backend = FjallBackend::open(data_dir)?;
            let stats = backup(&backend, output)?;
            backend.persist()?;
            println!(
                "backup complete: {} entries, {} bytes",
                stats.entries, stats.bytes
            );
        }
        Command::Restore { data_dir, input } => {
            let backend = FjallBackend::open(data_dir)?;
            let stats = restore(&backend, input)?;
            backend.persist()?;
            println!(
                "restore complete: {} entries, {} bytes",
                stats.entries, stats.bytes
            );
        }
        Command::Verify { input } => {
            let stats = verify(input)?;
            println!(
                "backup valid: {} entries, {} bytes",
                stats.entries, stats.bytes
            );
        }
    }

    Ok(())
}
