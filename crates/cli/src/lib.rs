//! Command-line client for RecolithDB.
#![forbid(unsafe_code)]

mod args;
mod command;
mod error;
mod format;
mod hex;

pub use args::{Args, Command};
pub use command::{parse_parameter, run};
pub use error::CliError;
pub use format::{format_document, format_value};
