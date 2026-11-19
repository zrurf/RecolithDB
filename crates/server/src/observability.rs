//! Slow-operation logging helpers.

use std::time::Duration;

use recolith_protocol::messages::opcode;

/// Returns whether slow-operation logging is enabled for a threshold.
#[must_use]
pub const fn slow_query_enabled(threshold_ms: u64) -> bool {
    threshold_ms > 0
}

/// Returns whether one opcode participates in slow-operation logging.
#[must_use]
pub fn slow_log_opcode(opcode: u16) -> bool {
    matches!(
        opcode,
        opcode::DOC_GET
            | opcode::DOC_PUT
            | opcode::DOC_DELETE
            | opcode::QUERY
            | opcode::EXPLAIN
            | opcode::AGGREGATE
            | opcode::BACKUP
    )
}

/// Returns whether one operation should be logged as slow.
#[must_use]
pub fn should_log_slow(threshold_ms: u64, opcode: u16, elapsed: Duration) -> bool {
    slow_query_enabled(threshold_ms)
        && slow_log_opcode(opcode)
        && elapsed >= Duration::from_millis(threshold_ms)
}

/// Returns a stable lowercase name for an opcode.
#[must_use]
pub fn opcode_name(opcode: u16) -> &'static str {
    match opcode {
        opcode::PING => "ping",
        opcode::DOC_GET => "doc_get",
        opcode::DOC_PUT => "doc_put",
        opcode::DOC_DELETE => "doc_delete",
        opcode::QUERY => "query",
        opcode::QUERY_STREAM => "query_stream",
        opcode::QUERY_CHUNK => "query_chunk",
        opcode::EXPLAIN => "explain",
        opcode::AGGREGATE => "aggregate",
        opcode::BACKUP => "backup",
        opcode::BATCH => "batch",
        opcode::DESCRIBE_COLLECTION => "describe_collection",
        opcode::CATALOG_APPLY => "catalog_apply",
        opcode::CATALOG_DROP => "catalog_drop",
        opcode::CATALOG_ALTER => "catalog_alter",
        opcode::HELLO => "hello",
        opcode::GOAWAY => "goaway",
        _ => "unknown",
    }
}

/// Formats bytes as lowercase hexadecimal.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
