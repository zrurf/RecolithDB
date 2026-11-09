use std::time::Duration;

use recolith_client::{Client, ClientConfig};
use recolith_protocol::messages::{NamedParameter, QueryRequest, Value, value};

use crate::args::{Args, Command};
use crate::error::CliError;
use crate::format::format_document;
use crate::hex;

/// Executes a CLI command.
///
/// # Errors
///
/// Returns [`CliError`] for invalid parameters or client failures.
pub async fn run(args: Args) -> Result<(), CliError> {
    let mut config = ClientConfig::new(args.endpoint)
        .with_connect_timeout(Some(Duration::from_secs(args.timeout_secs)))
        .with_request_timeout(Some(Duration::from_secs(args.timeout_secs)));
    if let Some(token) = args.token {
        config = config.with_auth_token(token.into_bytes());
    }
    match (args.tls_ca, args.tls_server_name) {
        (Some(ca), Some(server_name)) => config = config.with_tls(server_name, ca),
        (None, None) => {}
        _ => return Err(CliError::IncompleteTls),
    }

    let mut client = Client::connect(config).await?;
    match args.command {
        Command::Ping { payload } => {
            let response = client.ping(payload.into_bytes()).await?;
            println!("pong: {} bytes", response.len());
        }
        Command::Query {
            rql,
            partition,
            parameters,
            limit,
        } => {
            let parameters = parameters
                .iter()
                .map(|parameter| parse_parameter(parameter))
                .collect::<Result<Vec<_>, _>>()?;
            let request = QueryRequest {
                meta: None,
                partition,
                rql,
                parameters,
                limit,
            };
            let response = client.query(&request).await?;
            println!("documents: {}", response.documents.len());
            for document in &response.documents {
                print!("{}", format_document(document));
            }
        }
    }
    client.close().await?;

    Ok(())
}

/// Parses a named parameter in `name=type:value` form.
///
/// Supported types are `bool`, `i32`, `i64`, `u32`, `u64`, `f32`, `f64`,
/// `string`, `bytes`, `timestamp`, and `duration`.
///
/// # Errors
///
/// Returns [`CliError`] when the syntax or value is invalid.
pub fn parse_parameter(input: &str) -> Result<NamedParameter, CliError> {
    let (name, specification) = input
        .split_once('=')
        .ok_or_else(|| CliError::InvalidParameter(input.to_owned()))?;
    let (kind, raw) = specification
        .split_once(':')
        .ok_or_else(|| CliError::InvalidParameter(input.to_owned()))?;
    if name.is_empty() {
        return Err(CliError::InvalidParameter(input.to_owned()));
    }

    let kind = kind.trim().to_ascii_lowercase();
    let value = match kind.as_str() {
        "bool" => value::Kind::BoolValue(parse(raw, "bool")?),
        "i32" => value::Kind::I32Value(parse(raw, "i32")?),
        "i64" => value::Kind::I64Value(parse(raw, "i64")?),
        "u32" => value::Kind::U32Value(parse(raw, "u32")?),
        "u64" => value::Kind::U64Value(parse(raw, "u64")?),
        "f32" => value::Kind::F32Value(parse(raw, "f32")?),
        "f64" => value::Kind::F64Value(parse(raw, "f64")?),
        "string" | "str" => value::Kind::StringValue(raw.to_owned()),
        "bytes" | "hex" => {
            value::Kind::BytesValue(hex::decode(raw).ok_or_else(|| CliError::InvalidValue {
                kind: "bytes",
                value: raw.to_owned(),
            })?)
        }
        "timestamp" => value::Kind::TimestampMicros(parse(raw, "timestamp")?),
        "duration" => value::Kind::DurationMicros(parse(raw, "duration")?),
        _ => return Err(CliError::InvalidParameter(input.to_owned())),
    };

    Ok(NamedParameter {
        name: name.to_owned(),
        value: Some(Value { kind: Some(value) }),
    })
}

fn parse<T>(raw: &str, kind: &'static str) -> Result<T, CliError>
where
    T: std::str::FromStr,
{
    raw.parse().map_err(|_| CliError::InvalidValue {
        kind,
        value: raw.to_owned(),
    })
}
