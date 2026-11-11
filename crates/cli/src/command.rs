use std::time::Duration;

use recolith_client::{Client, ClientConfig};
use recolith_protocol::messages::{
    AggregateRequest, CollectionMetadata, DocumentGetRequest, DocumentPutRequest, ExplainRequest,
    NamedParameter, QueryRequest, QueryStreamRequest, RequestMeta, Value, value,
};

use crate::args::{Args, Command, DocCommand, TokenCommand};
use crate::error::CliError;
use crate::format::{
    format_aggregate, format_collection, format_document, format_explain, format_fragments_named,
};
use crate::hex;
use crate::metadata;

/// Executes a CLI command.
///
/// # Errors
///
/// Returns [`CliError`] for invalid parameters or client failures.
pub async fn run(args: Args) -> Result<(), CliError> {
    if let Command::Token { command } = &args.command {
        match command {
            TokenCommand::Issue {
                secret_file,
                subject,
                roles,
                ttl,
            } => println!(
                "{}",
                crate::token::issue(secret_file, subject.clone(), roles.clone(), *ttl)?
            ),
            TokenCommand::Inspect { token_file } => {
                let (claims, fingerprint) = crate::token::inspect(token_file.as_deref())?;
                print!(
                    "{}",
                    crate::token::format_claims(&claims, false, &fingerprint)
                );
            }
            TokenCommand::Verify {
                secret_file,
                previous_secret_file,
                token_file,
            } => {
                let (claims, fingerprint) = crate::token::verify(
                    secret_file,
                    previous_secret_file.as_deref(),
                    token_file.as_deref(),
                )?;
                print!(
                    "{}",
                    crate::token::format_claims(&claims, true, &fingerprint)
                );
            }
            TokenCommand::Hash { token_file } => {
                println!("{}", crate::token::hash(token_file.as_deref())?);
            }
            TokenCommand::Revoke {
                token_file,
                revoked_file,
            } => {
                let (added, fingerprint) =
                    crate::token::revoke(token_file.as_deref(), revoked_file)?;
                println!(
                    "{}: {fingerprint}",
                    if added { "revoked" } else { "already revoked" }
                );
            }
        }
        return Ok(());
    }

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
            cursor,
            stream,
            chunk_size,
        } => {
            let parameters = parameters
                .iter()
                .map(|parameter| parse_parameter(parameter))
                .collect::<Result<Vec<_>, _>>()?;
            if stream {
                let request = QueryStreamRequest {
                    meta: None,
                    partition,
                    rql,
                    parameters,
                    limit,
                    chunk_size,
                };
                let mut total = 0_usize;
                client
                    .query_stream(&request, |chunk| {
                        total += chunk.documents.len();
                        for document in &chunk.documents {
                            print!("{}", format_document(document));
                        }
                    })
                    .await?;
                println!("documents: {total}");
            } else {
                let cursor = match cursor {
                    Some(cursor) => {
                        crate::hex::decode(&cursor).ok_or(CliError::InvalidKey(cursor))?
                    }
                    None => Vec::new(),
                };
                let request = QueryRequest {
                    meta: None,
                    partition,
                    rql,
                    parameters,
                    limit,
                    cursor,
                };
                let response = client.query(&request).await?;
                println!("documents: {}", response.documents.len());
                for document in &response.documents {
                    print!("{}", format_document(document));
                }
                if !response.next_cursor.is_empty() {
                    println!("next_cursor: {}", crate::hex::encode(&response.next_cursor));
                }
            }
        }
        Command::Explain {
            rql,
            partition,
            parameters,
        } => {
            let parameters = parameters
                .iter()
                .map(|parameter| parse_parameter(parameter))
                .collect::<Result<Vec<_>, _>>()?;
            let response = client
                .explain(&ExplainRequest {
                    meta: None,
                    partition,
                    rql,
                    parameters,
                })
                .await?;
            print!("{}", format_explain(&response));
        }
        Command::Aggregate {
            rql,
            partition,
            parameters,
            limit,
        } => {
            let parameters = parameters
                .iter()
                .map(|parameter| parse_parameter(parameter))
                .collect::<Result<Vec<_>, _>>()?;
            let response = client
                .aggregate(&AggregateRequest {
                    meta: None,
                    partition,
                    rql,
                    parameters,
                    limit,
                })
                .await?;
            print!("{}", format_aggregate(&response));
        }
        Command::Describe {
            collection,
            keyspace,
        } => {
            let collection = describe(&mut client, &collection, keyspace.as_deref()).await?;
            print!("{}", format_collection(&collection));
        }
        Command::Token { .. } => {}
        Command::Catalog { command } => match crate::catalog::request(command)? {
            crate::catalog::CatalogRequest::Apply(request) => {
                let response = client.apply_catalog(&request).await?;
                println!("catalog generation: {}", response.catalog_generation);
            }
            crate::catalog::CatalogRequest::Drop(request) => {
                let response = client.drop_catalog(&request).await?;
                println!("catalog generation: {}", response.catalog_generation);
            }
            crate::catalog::CatalogRequest::Alter(request) => {
                let response = client.alter_catalog(&request).await?;
                println!("catalog generation: {}", response.catalog_generation);
            }
        },
        Command::Doc { command } => match command {
            DocCommand::Get {
                collection,
                keyspace,
                partition,
                key_hex,
                keys,
                fragments,
            } => {
                let collection = describe(&mut client, &collection, keyspace.as_deref()).await?;
                let request = DocumentGetRequest {
                    meta: None,
                    collection_id: collection.id,
                    partition,
                    document_key: metadata::document_key(&keys, key_hex.as_deref(), &collection)?,
                    fragments: metadata::fragment_ids(&fragments, &collection)?,
                    projection: Vec::new(),
                };
                let response = client.document_get(&request).await?;
                print!(
                    "{}",
                    format_fragments_named(
                        &response.document_key,
                        &response.fragments,
                        &collection
                    )
                );
            }
            DocCommand::Put {
                collection,
                keyspace,
                partition,
                key_hex,
                keys,
                idempotency_key,
                fields,
                subkeys,
            } => {
                if idempotency_key.is_empty() {
                    return Err(CliError::EmptyIdempotencyKey);
                }
                let collection = describe(&mut client, &collection, keyspace.as_deref()).await?;
                let request = DocumentPutRequest {
                    meta: Some(RequestMeta {
                        idempotency_key: idempotency_key.into_bytes(),
                        ..Default::default()
                    }),
                    collection_id: collection.id,
                    partition,
                    document_key: metadata::document_key(&keys, key_hex.as_deref(), &collection)?,
                    fragments: metadata::fragments(&fields, &subkeys, &collection)?,
                };
                let response = client.document_put(&request).await?;
                println!("commit_ts: {}", response.commit_ts);
                for version in &response.versions {
                    println!(
                        "fragment {} commit={}",
                        version.fragment_id, version.commit_ts
                    );
                }
            }
        },
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
    if name.is_empty() {
        return Err(CliError::InvalidParameter(input.to_owned()));
    }
    let value = parse_typed_value(specification)?;

    Ok(NamedParameter {
        name: name.to_owned(),
        value: Some(Value { kind: Some(value) }),
    })
}

/// Parses a numeric field specification in `fragment:field=type:value` form.
///
/// # Errors
///
/// Returns [`CliError`] when the target or value is invalid.
pub fn parse_field(input: &str) -> Result<(u32, u32, value::Kind), CliError> {
    let (target, specification) = input
        .split_once('=')
        .ok_or_else(|| CliError::InvalidField(input.to_owned()))?;
    let (fragment, field) = target
        .split_once(':')
        .ok_or_else(|| CliError::InvalidField(input.to_owned()))?;
    let fragment = fragment
        .parse()
        .map_err(|_| CliError::InvalidField(input.to_owned()))?;
    let field = field
        .parse()
        .map_err(|_| CliError::InvalidField(input.to_owned()))?;
    let value = parse_typed_value(specification)?;

    Ok((fragment, field, value))
}

pub(crate) fn parse_typed_value(specification: &str) -> Result<value::Kind, CliError> {
    let (kind, raw) = specification
        .split_once(':')
        .ok_or_else(|| CliError::InvalidParameter(specification.to_owned()))?;
    let kind = kind.trim().to_ascii_lowercase();

    match kind.as_str() {
        "bool" => Ok(value::Kind::BoolValue(parse(raw, "bool")?)),
        "i32" => Ok(value::Kind::I32Value(parse(raw, "i32")?)),
        "i64" => Ok(value::Kind::I64Value(parse(raw, "i64")?)),
        "u32" => Ok(value::Kind::U32Value(parse(raw, "u32")?)),
        "u64" => Ok(value::Kind::U64Value(parse(raw, "u64")?)),
        "f32" => Ok(value::Kind::F32Value(parse(raw, "f32")?)),
        "f64" => Ok(value::Kind::F64Value(parse(raw, "f64")?)),
        "string" | "str" => Ok(value::Kind::StringValue(raw.to_owned())),
        "bytes" | "hex" => Ok(value::Kind::BytesValue(hex::decode(raw).ok_or_else(
            || CliError::InvalidValue {
                kind: "bytes",
                value: raw.to_owned(),
            },
        )?)),
        "timestamp" => Ok(value::Kind::TimestampMicros(parse(raw, "timestamp")?)),
        "duration" => Ok(value::Kind::DurationMicros(parse(raw, "duration")?)),
        _ => Err(CliError::UnknownValueType(kind)),
    }
}

async fn describe(
    client: &mut Client,
    collection: &str,
    keyspace: Option<&str>,
) -> Result<CollectionMetadata, CliError> {
    let request = metadata::describe_request(collection, keyspace);
    let response = client.describe_collection(&request).await?;

    metadata::collection(response)
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
