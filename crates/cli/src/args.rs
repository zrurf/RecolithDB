use std::path::PathBuf;

use clap::{ArgGroup, Parser, Subcommand};
use recolith_security::DEFAULT_TTL_SECONDS;

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
        /// Pagination cursor as hexadecimal.
        #[arg(long)]
        cursor: Option<String>,
        /// Streams result chunks instead of one response.
        #[arg(long)]
        stream: bool,
        /// Documents per streamed chunk.
        #[arg(long, default_value_t = 100)]
        chunk_size: u32,
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
    /// Issues local authentication tokens.
    Token {
        /// Token command.
        #[command(subcommand)]
        command: TokenCommand,
    },
    /// Creates catalog objects through the server.
    Catalog {
        /// Catalog command.
        #[command(subcommand)]
        command: CatalogCommand,
    },
}

/// Catalog definition commands.
#[derive(Debug, Subcommand)]
pub enum CatalogCommand {
    /// Applies a catalog definition TOML file atomically.
    Apply {
        /// Catalog definition file.
        #[arg(long)]
        file: PathBuf,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Creates a keyspace.
    CreateKeyspace {
        /// Keyspace identifier.
        #[arg(long)]
        id: u32,
        /// Keyspace name.
        #[arg(long)]
        name: String,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Registers a schema version from an IDL file.
    RegisterSchema {
        /// Schema identifier.
        #[arg(long)]
        id: u64,
        /// Schema version.
        #[arg(long)]
        version: u32,
        /// File containing the record IDL.
        #[arg(long)]
        idl_file: PathBuf,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Creates a bucket.
    CreateBucket {
        /// Bucket identifier.
        #[arg(long)]
        id: u64,
        /// Owning keyspace identifier.
        #[arg(long)]
        keyspace_id: u32,
        /// Bucket name.
        #[arg(long)]
        name: String,
        /// Partition count.
        #[arg(long)]
        partitions: u32,
        /// Schema identifier.
        #[arg(long)]
        schema_id: u64,
        /// Schema version.
        #[arg(long)]
        schema_version: u32,
        /// Bucket policy: strict, managed, or raw.
        #[arg(long, default_value = "strict")]
        policy: String,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Creates a collection.
    CreateCollection {
        /// Collection identifier.
        #[arg(long)]
        id: u64,
        /// Owning keyspace identifier.
        #[arg(long)]
        keyspace_id: u32,
        /// Collection name.
        #[arg(long)]
        name: String,
        /// Affinity group. Defaults to the collection name.
        #[arg(long)]
        affinity_group: Option<String>,
        /// Document key field in `name:type` form.
        #[arg(long = "key", required = true)]
        key_fields: Vec<String>,
        /// Fragment in `id:name:bucket_id[:one|many]` form.
        #[arg(long = "fragment", required = true)]
        fragments: Vec<String>,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Creates an index on a bucket.
    CreateIndex {
        /// Bucket identifier.
        #[arg(long)]
        bucket_id: u64,
        /// Index identifier.
        #[arg(long)]
        id: u64,
        /// Index name.
        #[arg(long)]
        name: String,
        /// Index kind: equality, range, composite, or unique.
        #[arg(long, default_value = "equality")]
        kind: String,
        /// Indexed field identifier.
        #[arg(long = "field", required = true)]
        fields: Vec<u32>,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Switches a bucket's schema version, optionally migrating records.
    AlterBucketSchema {
        /// Bucket identifier.
        #[arg(long)]
        bucket_id: u64,
        /// New schema version.
        #[arg(long)]
        schema_version: u32,
        /// Copies a field in `to=from` form.
        #[arg(long = "mapping")]
        mappings: Vec<String>,
        /// Converts a field in `to=from:conversion` form.
        #[arg(long = "convert")]
        conversions: Vec<String>,
        /// Sets a field in `to=type:value` form.
        #[arg(long = "default")]
        defaults: Vec<String>,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Redefines an index on a bucket.
    AlterIndex {
        /// Bucket identifier.
        #[arg(long)]
        bucket_id: u64,
        /// Index identifier.
        #[arg(long)]
        index_id: u64,
        /// New index name.
        #[arg(long)]
        name: String,
        /// Index kind: equality, range, composite, or unique.
        #[arg(long, default_value = "equality")]
        kind: String,
        /// Indexed field identifier.
        #[arg(long = "field", required = true)]
        fields: Vec<u32>,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Drops an index and optionally its entries.
    DropIndex {
        /// Bucket identifier.
        #[arg(long)]
        bucket_id: u64,
        /// Index identifier.
        #[arg(long)]
        index_id: u64,
        /// Delete existing index entries.
        #[arg(long)]
        purge: bool,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Drops a collection and optionally its records.
    DropCollection {
        /// Collection identifier.
        #[arg(long)]
        id: u64,
        /// Delete existing records and index entries.
        #[arg(long)]
        purge: bool,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Drops a bucket and optionally its records.
    DropBucket {
        /// Bucket identifier.
        #[arg(long)]
        id: u64,
        /// Delete existing records and index entries.
        #[arg(long)]
        purge: bool,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
    /// Drops a keyspace and optionally its data.
    DropKeyspace {
        /// Keyspace identifier.
        #[arg(long)]
        id: u32,
        /// Delete existing records and index entries.
        #[arg(long)]
        purge: bool,
        /// Expected catalog generation; zero disables the check.
        #[arg(long, default_value_t = 0)]
        catalog_generation: u64,
    },
}

/// Local token commands.
#[derive(Debug, Subcommand)]
pub enum TokenCommand {
    /// Signs a token with a local secret file.
    Issue {
        /// Hexadecimal HMAC secret file.
        #[arg(long)]
        secret_file: PathBuf,
        /// Principal identifier.
        #[arg(long)]
        subject: String,
        /// Role name. Repeat for multiple roles.
        #[arg(long = "role", required = true)]
        roles: Vec<String>,
        /// Token lifetime in seconds.
        #[arg(long, default_value_t = DEFAULT_TTL_SECONDS)]
        ttl: u64,
    },
    /// Prints token claims without verifying the signature.
    Inspect {
        /// File containing the token. Reads stdin when omitted.
        #[arg(long)]
        token_file: Option<PathBuf>,
    },
    /// Verifies a token signature and prints its claims.
    Verify {
        /// Hexadecimal HMAC secret file.
        #[arg(long)]
        secret_file: PathBuf,
        /// Optional previous secret accepted during rotation.
        #[arg(long)]
        previous_secret_file: Option<PathBuf>,
        /// File containing the token. Reads stdin when omitted.
        #[arg(long)]
        token_file: Option<PathBuf>,
    },
    /// Prints the SHA-256 fingerprint used by the revocation list.
    Hash {
        /// File containing the token. Reads stdin when omitted.
        #[arg(long)]
        token_file: Option<PathBuf>,
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
