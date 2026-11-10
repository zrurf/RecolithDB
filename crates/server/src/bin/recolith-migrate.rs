//! Offline schema migration for large buckets.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use recolith_backend_fjall::FjallBackend;
use recolith_common::{BucketId, SchemaVersion, Timestamp};
use recolith_server::{
    DEFAULT_MIGRATION_BATCH, FieldMapping, MigrationConversion, MigrationSource, OfflineMigration,
    OfflineMigrationError, SchemaMigration,
};
use recolith_storage::Value;
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(
    name = "recolith-migrate",
    version,
    about = "Offline bucket schema migration"
)]
struct Args {
    /// Fjall data directory. The server must be stopped.
    #[arg(long)]
    data_dir: PathBuf,
    /// Bucket identifier.
    #[arg(long)]
    bucket_id: u64,
    /// Target schema version.
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
    /// Records examined per batch.
    #[arg(long, default_value_t = DEFAULT_MIGRATION_BATCH)]
    batch_size: usize,
}

#[derive(Debug, Error)]
enum MigrateError {
    #[error(transparent)]
    Offline(#[from] OfflineMigrationError),
    #[error(transparent)]
    Backend(#[from] recolith_backend::BackendError),
    #[error("invalid argument: {0}")]
    Invalid(String),
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(stats) => {
            println!(
                "migrated: {}, skipped: {}, batches: {}",
                stats.migrated, stats.skipped, stats.batches
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("recolith-migrate: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<recolith_server::MigrationStats, MigrateError> {
    let backend = FjallBackend::open(&args.data_dir)?;
    let migration = migration(&args.mappings, &args.conversions, &args.defaults)?;
    let migrator = OfflineMigration::open(
        backend,
        BucketId::new(args.bucket_id),
        SchemaVersion::new(args.schema_version),
        migration,
    )?;

    Ok(migrator.migrate(args.batch_size)?)
}

fn migration(
    mappings: &[String],
    conversions: &[String],
    defaults: &[String],
) -> Result<SchemaMigration, MigrateError> {
    let mut output = Vec::new();
    for mapping in mappings {
        let (to, from) = mapping
            .split_once('=')
            .ok_or_else(|| invalid(mapping, "to=from"))?;
        output.push(FieldMapping {
            to: recolith_common::FieldId::new(parse_u32(to, mapping)?),
            source: MigrationSource::Field(recolith_common::FieldId::new(parse_u32(
                from, mapping,
            )?)),
            conversion: MigrationConversion::None,
        });
    }
    for conversion in conversions {
        let (to, rest) = conversion
            .split_once('=')
            .ok_or_else(|| invalid(conversion, "to=from:conversion"))?;
        let (from, kind) = rest
            .split_once(':')
            .ok_or_else(|| invalid(conversion, "to=from:conversion"))?;
        output.push(FieldMapping {
            to: recolith_common::FieldId::new(parse_u32(to, conversion)?),
            source: MigrationSource::Field(recolith_common::FieldId::new(parse_u32(
                from, conversion,
            )?)),
            conversion: conversion_kind(kind)?,
        });
    }
    for default in defaults {
        let (to, specification) = default
            .split_once('=')
            .ok_or_else(|| invalid(default, "to=type:value"))?;
        output.push(FieldMapping {
            to: recolith_common::FieldId::new(parse_u32(to, default)?),
            source: MigrationSource::Default(parse_value(specification)?),
            conversion: MigrationConversion::None,
        });
    }

    Ok(SchemaMigration { mappings: output })
}

fn conversion_kind(value: &str) -> Result<MigrationConversion, MigrateError> {
    match value {
        "none" => Ok(MigrationConversion::None),
        "i32-i64" => Ok(MigrationConversion::I32ToI64),
        "u32-u64" => Ok(MigrationConversion::U32ToU64),
        "u32-i64" => Ok(MigrationConversion::U32ToI64),
        _ => Err(MigrateError::Invalid(format!(
            "unknown conversion `{value}`; expected none, i32-i64, u32-u64, or u32-i64"
        ))),
    }
}

fn parse_value(specification: &str) -> Result<Value, MigrateError> {
    let (kind, raw) = specification
        .split_once(':')
        .ok_or_else(|| MigrateError::Invalid(format!("invalid value `{specification}`")))?;
    let invalid_value = || MigrateError::Invalid(format!("invalid {kind} value `{raw}`"));
    match kind.trim().to_ascii_lowercase().as_str() {
        "bool" => Ok(Value::Bool(raw.parse().map_err(|_| invalid_value())?)),
        "i32" => Ok(Value::I32(raw.parse().map_err(|_| invalid_value())?)),
        "i64" => Ok(Value::I64(raw.parse().map_err(|_| invalid_value())?)),
        "u32" => Ok(Value::U32(raw.parse().map_err(|_| invalid_value())?)),
        "u64" => Ok(Value::U64(raw.parse().map_err(|_| invalid_value())?)),
        "f32" => Ok(Value::F32(raw.parse().map_err(|_| invalid_value())?)),
        "f64" => Ok(Value::F64(raw.parse().map_err(|_| invalid_value())?)),
        "string" | "str" => Ok(Value::String(raw.to_owned())),
        "bytes" | "hex" => Ok(Value::Bytes(decode_hex(raw).ok_or_else(invalid_value)?)),
        "timestamp" => Ok(Value::Timestamp(Timestamp::from_micros(
            raw.parse().map_err(|_| invalid_value())?,
        ))),
        "duration" => Ok(Value::Duration(raw.parse().map_err(|_| invalid_value())?)),
        _ => Err(MigrateError::Invalid(format!(
            "unknown value type `{kind}`"
        ))),
    }
}

fn parse_u32(value: &str, original: &str) -> Result<u32, MigrateError> {
    value
        .parse()
        .map_err(|_| MigrateError::Invalid(format!("invalid migration `{original}`")))
}

fn invalid(value: &str, expected: &str) -> MigrateError {
    MigrateError::Invalid(format!("invalid migration `{value}`; expected {expected}"))
}

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    let input = input.strip_prefix("0x").unwrap_or(input);
    if !input.is_ascii() || !input.len().is_multiple_of(2) {
        return None;
    }

    (0..input.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&input[offset..offset + 2], 16).ok())
        .collect()
}
