//! Command-line client for RecolithDB.
#![forbid(unsafe_code)]

mod args;
mod catalog;
mod command;
mod error;
mod format;
mod hex;
mod metadata;
mod token;

pub use args::{Args, CatalogCommand, Command, DocCommand, TokenCommand};
pub use command::{parse_field, parse_parameter, run};
pub use error::CliError;
pub use format::{
    format_collection, format_document, format_fragments, format_fragments_named, format_value,
};
