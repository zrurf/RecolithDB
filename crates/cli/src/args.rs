use std::path::PathBuf;

use clap::{ArgGroup, Parser, Subcommand};

/// Command-line client configuration.
#[derive(Debug, Parser)]
#[command(name = "recolith", version, about = "RecolithDB command-line client")]
pub struct Args {
    /// Server endpoint.
    #[arg(
        long,
        env = "RECOLITH_ENDPOINT",
        default_value = "127.0.0.1:7420",
        global = true
    )]
    pub endpoint: String,
    /// Shared bearer token.
    #[arg(long, env = "RECOLITH_TOKEN", global = true, hide_env_values = true)]
    pub token: Option<String>,
    /// PEM certificate authority for TLS.
    #[arg(long, env = "RECOLITH_TLS_CA", global = true)]
    pub tls_ca: Option<PathBuf>,
    /// Server name verified against the TLS certificate.
    #[arg(long, env = "RECOLITH_TLS_SERVER_NAME", global = true)]
    pub tls_server_name: Option<String>,
    /// Connection and request timeout in seconds.
    #[arg(long, default_value_t = 30, global = true)]
    pub timeout_secs: u64,
    /// Command to execute.
    #[command(subcommand)]
    pub command: Command,
}

/// Supported client commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Sends a ping request.
    Ping {
        /// Payload echoed by the server.
        #[arg(default_value = "ping")]
        payload: String,
    },
    /// Executes an RQL query.
    Query {
        /// RQL query text.
        rql: String,
        /// Partition to query.
        #[arg(long, default_value_t = 0)]
        partition: u32,
        /// Named parameter in `name=type:value` form.
        #[arg(long = "param", value_name = "NAME=TYPE:VALUE")]
        parameters: Vec<String>,
        /// Maximum number of documents.
        #[arg(long, default_value_t = 100)]
        limit: u64,
    },
    /// Describes a collection.
    Describe {
        /// Collection name or identifier.
        #[arg(value_name = "NAME|ID")]
        collection: String,
        /// Keyspace name used to disambiguate the collection.
        #[arg(long)]
        keyspace: Option<String>,
    },
    /// Reads or writes documents.
    Doc {
        /// Document command.
        #[command(subcommand)]
        command: DocCommand,
    },
}

/// Document read and write commands.
#[derive(Debug, Subcommand)]
pub enum DocCommand {
    /// Reads document fragments.
    #[command(group(
        ArgGroup::new("get_key")
            .required(true)
            .args(["key_hex", "keys"])
    ))]
    Get {
        /// Collection name or identifier.
        #[arg(long)]
        collection: String,
        /// Keyspace name used to disambiguate the collection.
        #[arg(long)]
        keyspace: Option<String>,
        /// Partition index.
        #[arg(long, default_value_t = 0)]
        partition: u32,
        /// Encoded document key in hexadecimal.
        #[arg(long)]
        key_hex: Option<String>,
        /// Document key component in `name=type:value` form.
        #[arg(long = "key", value_name = "NAME=TYPE:VALUE")]
        keys: Vec<String>,
        /// Fragment name or identifier. Omit to read every fragment.
        #[arg(long = "fragment")]
        fragments: Vec<String>,
    },
    /// Writes document fragments.
    #[command(group(
        ArgGroup::new("put_key")
            .required(true)
            .args(["key_hex", "keys"])
    ))]
    Put {
        /// Collection name or identifier.
        #[arg(long)]
        collection: String,
        /// Keyspace name used to disambiguate the collection.
        #[arg(long)]
        keyspace: Option<String>,
        /// Partition index.
        #[arg(long, default_value_t = 0)]
        partition: u32,
        /// Encoded document key in hexadecimal.
        #[arg(long)]
        key_hex: Option<String>,
        /// Document key component in `name=type:value` form.
        #[arg(long = "key", value_name = "NAME=TYPE:VALUE")]
        keys: Vec<String>,
        /// Caller-provided idempotency key.
        #[arg(long)]
        idempotency_key: String,
        /// Field value in `fragment.field=type:value` or `fragment:field=type:value` form.
        #[arg(
            long = "field",
            value_name = "FRAGMENT.FIELD=TYPE:VALUE",
            required = true
        )]
        fields: Vec<String>,
        /// Many-fragment subkey in `fragment=hex` form.
        #[arg(long = "subkey", value_name = "FRAGMENT=HEX")]
        subkeys: Vec<String>,
    },
}
