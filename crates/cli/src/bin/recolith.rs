use std::process::ExitCode;

use clap::Parser;
use recolith_cli::{Args, run};

#[tokio::main]
async fn main() -> ExitCode {
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("recolith: {error}");
            ExitCode::FAILURE
        }
    }
}
