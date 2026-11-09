use recolith_protocol::{
    BatchLimits, ProtocolError, SubFrame, decode_batch, encode_batch, is_batchable_opcode,
    messages::opcode,
};

fn limits() -> BatchLimits {
    BatchLimits {
        max_subframes: 8,
        max_bytes: 4096,
    }
}

#[test]
fn roundtrips_batch_subframes() {
    let subframes = [
        SubFrame::new(opcode::DOC_GET, 1, b"first"),
        SubFrame::new(opcode::DOC_PUT, 2, b"second"),
    ];
    let encoded = encode_batch(&subframes, limits()).expect("encode");
    let decoded = decode_batch(&encoded, limits()).expect("decode");

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].opcode(), opcode::DOC_GET);
    assert_eq!(decoded[0].request_id(), 1);
    assert_eq!(decoded[0].payload(), b"first");
    assert_eq!(decoded[1].opcode(), opcode::DOC_PUT);
    assert_eq!(decoded[1].request_id(), 2);
    assert_eq!(decoded[1].payload(), b"second");
}

#[test]
fn allows_only_unary_request_opcodes() {
    assert!(is_batchable_opcode(opcode::PING));
    assert!(is_batchable_opcode(opcode::DOC_GET));
    assert!(is_batchable_opcode(opcode::DOC_PUT));
    assert!(is_batchable_opcode(opcode::QUERY));
    assert!(!is_batchable_opcode(opcode::HELLO));
    assert!(!is_batchable_opcode(opcode::BATCH));
    assert!(!is_batchable_opcode(opcode::ERROR));
}

#[test]
fn rejects_empty_batch() {
    assert!(matches!(
        encode_batch(&[], limits()),
        Err(ProtocolError::EmptyBatch)
    ));
    assert!(matches!(
        decode_batch(&[0, 0], limits()),
        Err(ProtocolError::EmptyBatch)
    ));
}

#[test]
fn enforces_subframe_limit() {
    let subframes = [
        SubFrame::new(opcode::PING, 1, b""),
        SubFrame::new(opcode::PING, 2, b""),
    ];
    let encoded = encode_batch(&subframes, BatchLimits::default()).expect("encode");
    let small = BatchLimits {
        max_subframes: 1,
        max_bytes: 4096,
    };

    assert!(matches!(
        encode_batch(&subframes, small),
        Err(ProtocolError::BatchTooMany { .. })
    ));
    assert!(matches!(
        decode_batch(&encoded, small),
        Err(ProtocolError::BatchTooMany { .. })
    ));
}

#[test]
fn enforces_byte_limit() {
    let subframes = [SubFrame::new(opcode::PING, 1, b"0123456789")];
    let small = BatchLimits {
        max_subframes: 8,
        max_bytes: 20,
    };

    assert!(matches!(
        encode_batch(&subframes, small),
        Err(ProtocolError::BatchTooLarge { .. })
    ));
}

#[test]
fn rejects_forbidden_opcode() {
    let subframes = [SubFrame::new(opcode::HELLO, 1, b"")];
    assert!(matches!(
        encode_batch(&subframes, limits()),
        Err(ProtocolError::BatchForbiddenOpcode(opcode::HELLO))
    ));
}

#[test]
fn rejects_duplicate_request_ids() {
    let subframes = [
        SubFrame::new(opcode::PING, 1, b""),
        SubFrame::new(opcode::PING, 1, b""),
    ];
    assert!(matches!(
        encode_batch(&subframes, limits()),
        Err(ProtocolError::DuplicateRequestId(1))
    ));
}

#[test]
fn rejects_zero_request_id() {
    let subframes = [SubFrame::new(opcode::PING, 0, b"")];
    assert!(matches!(
        encode_batch(&subframes, limits()),
        Err(ProtocolError::ZeroRequestId)
    ));
}

#[test]
fn rejects_truncated_batch() {
    let subframes = [SubFrame::new(opcode::PING, 1, b"payload")];
    let mut encoded = encode_batch(&subframes, limits()).expect("encode");
    encoded.pop();

    assert!(matches!(
        decode_batch(&encoded, limits()),
        Err(ProtocolError::BatchTruncated)
    ));
}

#[test]
fn rejects_trailing_bytes() {
    let subframes = [SubFrame::new(opcode::PING, 1, b"")];
    let mut encoded = encode_batch(&subframes, limits()).expect("encode");
    encoded.push(0);

    assert!(matches!(
        decode_batch(&encoded, limits()),
        Err(ProtocolError::BatchTrailingBytes)
    ));
}

#[test]
fn rejects_non_zero_subframe_flags() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&1_u16.to_le_bytes());
    encoded.extend_from_slice(&opcode::PING.to_le_bytes());
    encoded.extend_from_slice(&1_u16.to_le_bytes());
    encoded.extend_from_slice(&1_u64.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());

    assert!(matches!(
        decode_batch(&encoded, limits()),
        Err(ProtocolError::BatchSubframeFlags(1))
    ));
}
