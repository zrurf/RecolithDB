use std::time::Duration;

use recolith_protocol::messages::opcode;
use recolith_server::observability::{hex, opcode_name, should_log_slow, slow_log_opcode};

#[test]
fn only_data_operations_participate_in_slow_logging() {
    for operation in [
        opcode::DOC_GET,
        opcode::DOC_PUT,
        opcode::DOC_DELETE,
        opcode::QUERY,
        opcode::EXPLAIN,
        opcode::AGGREGATE,
        opcode::BACKUP,
    ] {
        assert!(slow_log_opcode(operation));
    }
    for operation in [
        opcode::PING,
        opcode::DESCRIBE_COLLECTION,
        opcode::CATALOG_APPLY,
        opcode::CATALOG_DROP,
        opcode::CATALOG_ALTER,
        opcode::QUERY_STREAM,
        opcode::BATCH,
        opcode::HELLO,
    ] {
        assert!(!slow_log_opcode(operation));
    }
}

#[test]
fn logs_operations_at_or_above_the_threshold() {
    assert!(should_log_slow(
        200,
        opcode::QUERY,
        Duration::from_millis(200)
    ));
    assert!(should_log_slow(
        200,
        opcode::QUERY,
        Duration::from_millis(201)
    ));
    assert!(!should_log_slow(
        200,
        opcode::QUERY,
        Duration::from_millis(199)
    ));
    assert!(!should_log_slow(0, opcode::QUERY, Duration::from_secs(60)));
    assert!(!should_log_slow(200, opcode::PING, Duration::from_secs(60)));
}

#[test]
fn names_opcodes_stably() {
    assert_eq!(opcode_name(opcode::DOC_DELETE), "doc_delete");
    assert_eq!(opcode_name(opcode::EXPLAIN), "explain");
    assert_eq!(opcode_name(opcode::AGGREGATE), "aggregate");
    assert_eq!(opcode_name(opcode::BACKUP), "backup");
    assert_eq!(opcode_name(9_999), "unknown");
}

#[test]
fn formats_hexadecimal_bytes() {
    assert_eq!(hex(&[]), "");
    assert_eq!(hex(&[0x01, 0xAB]), "01ab");
}
