use std::time::Duration;

use recolith_client::{Client, ClientConfig};
use recolith_protocol::messages::{
    AggregateRequest, BackupRequest, BucketDeleteRequest, BucketGetRequest, BucketPutRequest,
    BucketRecordId, BucketScanRequest, CollectionMetadata, DocumentDeleteRequest,
    DocumentGetRequest, DocumentPutRequest, ExplainRequest, FieldValue, NamedParameter,
    QueryRequest, QueryStreamRequest, RequestMeta, Value, WorkflowCancelRequest,
    WorkflowListRequest, WorkflowResolveAction, WorkflowResolveRequest, WorkflowStatusRequest,
    value,
};

use crate::args::{Args, BucketCommand, Command, DocCommand, TokenCommand, WorkflowCommand};
use crate::error::CliError;
use crate::format::{
    format_aggregate, format_backup, format_bucket_record, format_collection, format_document,
    format_explain, format_fragments_named, format_workflow,
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
        Command::Backup { name } => {
            let response = client.backup(&BackupRequest { meta: None, name }).await?;
            print!("{}", format_backup(&response));
        }
        Command::Describe {
            collection,
            keyspace,
        } => {
            let collection = describe(&mut client, &collection, keyspace.as_deref()).await?;
            print!("{}", format_collection(&collection));
        }
        Command::Workflow { command } => match command {
            WorkflowCommand::Status { id } => {
                let response = client
                    .workflow_status(&WorkflowStatusRequest {
                        meta: None,
                        id: id.clone(),
                    })
                    .await?;
                print!("{}", format_workflow(&response));
            }
            WorkflowCommand::Cancel { id } => {
                let response = client
                    .cancel_workflow(&WorkflowCancelRequest {
                        meta: None,
                        id: id.clone(),
                    })
                    .await?;
                print!("{}", format_workflow(&response));
            }
            WorkflowCommand::List { limit, start_after } => {
                let response = client
                    .workflow_list(&WorkflowListRequest {
                        meta: None,
                        start_after: start_after.clone().unwrap_or_default(),
                        limit,
                    })
                    .await?;
                if response.workflows.is_empty() {
                    println!("workflows: none");
                }
                for workflow in &response.workflows {
                    println!(
                        "workflow {} (step {}) {}",
                        workflow.id,
                        workflow.current_step,
                        crate::format::workflow_state(workflow.state)
                    );
                }
            }
            WorkflowCommand::Resolve { id, action } => {
                let resolve_action = match action.as_str() {
                    "retry" => WorkflowResolveAction::Retry,
                    _ => WorkflowResolveAction::Abandon,
                };
                let response = client
                    .resolve_workflow(&WorkflowResolveRequest {
                        meta: None,
                        id: id.clone(),
                        action: resolve_action as i32,
                    })
                    .await?;
                if let Some(workflow) = &response.workflow {
                    print!("{}", format_workflow(workflow));
                }
            }
        },
        Command::Bucket { command } => match command {
            BucketCommand::Get {
                keyspace,
                bucket,
                partition,
                fragment,
                key_hex,
                key,
                subkey_hex,
                subkey,
            } => {
                let (bucket_id, bucket_name) = bucket_reference(&bucket);
                let request = BucketGetRequest {
                    meta: None,
                    id: Some(BucketRecordId {
                        bucket_id,
                        partition,
                        fragment_id: fragment,
                        document_key: key_bytes(key_hex.as_deref(), key.as_deref())?,
                        subkey: subkey_bytes(subkey_hex.as_deref(), subkey.as_deref())?,
                        keyspace: keyspace.clone(),
                        bucket_name,
                    }),
                };
                let response = client.bucket_get(&request).await?;
                match response.record {
                    Some(record) => print!("{}", format_bucket_record(&record)),
                    None => println!("record: absent"),
                }
            }
            BucketCommand::Put {
                keyspace,
                bucket,
                partition,
                fragment,
                key_hex,
                key,
                subkey_hex,
                subkey,
                fields,
                if_match,
            } => {
                let (bucket_id, bucket_name) = bucket_reference(&bucket);
                let parsed = fields
                    .iter()
                    .map(|input| bucket_field(input))
                    .collect::<Result<Vec<_>, _>>()?;
                let request = BucketPutRequest {
                    meta: None,
                    id: Some(BucketRecordId {
                        bucket_id,
                        partition,
                        fragment_id: fragment,
                        document_key: key_bytes(key_hex.as_deref(), key.as_deref())?,
                        subkey: subkey_bytes(subkey_hex.as_deref(), subkey.as_deref())?,
                        keyspace: keyspace.clone(),
                        bucket_name,
                    }),
                    fields: parsed,
                    if_match,
                };
                let response = client.bucket_put(&request).await?;
                println!("commit_ts: {}", response.commit_ts);
            }
            BucketCommand::Delete {
                keyspace,
                bucket,
                partition,
                fragment,
                key_hex,
                key,
                subkey_hex,
                subkey,
            } => {
                let (bucket_id, bucket_name) = bucket_reference(&bucket);
                let request = BucketDeleteRequest {
                    meta: None,
                    id: Some(BucketRecordId {
                        bucket_id,
                        partition,
                        fragment_id: fragment,
                        document_key: key_bytes(key_hex.as_deref(), key.as_deref())?,
                        subkey: subkey_bytes(subkey_hex.as_deref(), subkey.as_deref())?,
                        keyspace: keyspace.clone(),
                        bucket_name,
                    }),
                };
                let response = client.bucket_delete(&request).await?;
                println!("deleted: {}", response.deleted);
            }
            BucketCommand::Scan {
                keyspace,
                bucket,
                partition,
                fragment,
                limit,
                start_after_hex,
            } => {
                let (bucket_id, bucket_name) = bucket_reference(&bucket);
                let start_after = match start_after_hex.as_deref() {
                    Some(text) => {
                        crate::hex::decode(text).ok_or_else(|| CliError::InvalidValue {
                            kind: "start-after-hex",
                            value: text.to_owned(),
                        })?
                    }
                    None => Vec::new(),
                };
                let request = BucketScanRequest {
                    meta: None,
                    bucket_id,
                    partition,
                    fragment_id: fragment,
                    start_after,
                    limit,
                    keyspace: keyspace.clone(),
                    bucket_name,
                };
                let response = client.bucket_scan(&request).await?;
                if response.records.is_empty() {
                    println!("records: none");
                }
                for record in &response.records {
                    print!("{}", format_bucket_record(record));
                }
            }
        },
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
                if response.fragments.is_empty() {
                    println!("document: absent or expired");
                } else {
                    print!(
                        "{}",
                        format_fragments_named(
                            &response.document_key,
                            &response.fragments,
                            &collection
                        )
                    );
                }
            }
            DocCommand::Delete {
                collection,
                keyspace,
                partition,
                key_hex,
                keys,
            } => {
                let collection = describe(&mut client, &collection, keyspace.as_deref()).await?;
                let response = client
                    .document_delete(&DocumentDeleteRequest {
                        meta: None,
                        collection_id: collection.id,
                        partition,
                        document_key: metadata::document_key(
                            &keys,
                            key_hex.as_deref(),
                            &collection,
                        )?,
                    })
                    .await?;
                println!("deleted: {}", response.deleted);
            }
            DocCommand::Put {
                collection,
                keyspace,
                partition,
                key_hex,
                keys,
                idempotency_key,
                ttl_seconds,
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
                    ttl_seconds,
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

/// Splits a bucket reference into an identifier and an optional name.
///
/// Numeric references use the identifier form; anything else is treated as a
/// bucket name resolved by the server.
fn bucket_reference(bucket: &str) -> (u64, Option<String>) {
    match bucket.parse::<u64>() {
        Ok(id) => (id, None),
        Err(_) => (0, Some(bucket.to_owned())),
    }
}

/// Decodes a document key from hexadecimal or UTF-8 text.
///
/// # Errors
///
/// Returns [`CliError`] when the key is missing or malformed.
fn key_bytes(key_hex: Option<&str>, key: Option<&str>) -> Result<Vec<u8>, CliError> {
    if let Some(text) = key_hex {
        return crate::hex::decode(text).ok_or_else(|| CliError::InvalidValue {
            kind: "key-hex",
            value: text.to_owned(),
        });
    }
    key.map(|text| text.as_bytes().to_vec())
        .ok_or_else(|| CliError::InvalidParameter("a document key is required".to_owned()))
}

/// Decodes a record subkey from hexadecimal or UTF-8 text.
///
/// # Errors
///
/// Returns [`CliError`] when the subkey is malformed.
fn subkey_bytes(subkey_hex: Option<&str>, subkey: Option<&str>) -> Result<Vec<u8>, CliError> {
    if let Some(text) = subkey_hex {
        return crate::hex::decode(text).ok_or_else(|| CliError::InvalidValue {
            kind: "subkey-hex",
            value: text.to_owned(),
        });
    }
    Ok(subkey
        .map(|text| text.as_bytes().to_vec())
        .unwrap_or_default())
}

/// Parses a bucket field in `field_id=type:value` form.
///
/// # Errors
///
/// Returns [`CliError`] when the syntax or value is invalid.
fn bucket_field(input: &str) -> Result<FieldValue, CliError> {
    let (target, specification) = input
        .split_once('=')
        .ok_or_else(|| CliError::InvalidField(input.to_owned()))?;
    let field_id = target
        .parse::<u32>()
        .map_err(|_| CliError::InvalidField(input.to_owned()))?;
    let kind = parse_typed_value(specification)?;

    Ok(FieldValue {
        field_id,
        value: Some(Value { kind: Some(kind) }),
    })
}
