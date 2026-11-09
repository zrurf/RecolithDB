use recolith_protocol::frame::{
    DEFAULT_MAX_DECOMPRESSED_SIZE, DEFAULT_MAX_FRAME_SIZE, FLAG_COMPRESSED, FrameCodec,
    FrameHeader, HEADER_LEN, PayloadEncoding,
};
use recolith_protocol::messages::{opcode, status};
use recolith_protocol::{CompressionAlgorithm, ProtocolError};

#[test]
fn roundtrips_header_and_payload() {
    let codec = FrameCodec::plain().expect("codec");
    let header = FrameHeader::new(opcode::HELLO, 7)
        .as_response(status::OK)
        .with_encoding(PayloadEncoding::Protobuf);
    let payload = b"hello";

    let frame = codec.encode(&header, payload).expect("encode");
    let decoded = FrameHeader::decode(&frame).expect("header");

    assert_eq!(decoded.opcode(), opcode::HELLO);
    assert_eq!(decoded.request_id(), 7);
    assert!(decoded.is_response());
    assert_eq!(decoded.status(), status::OK);
    assert_eq!(decoded.encoding(), Some(PayloadEncoding::Protobuf));
    assert_eq!(decoded.payload_len() as usize, payload.len());
    assert_eq!(
        codec
            .decode(&decoded, &frame[HEADER_LEN..])
            .expect("payload"),
        payload
    );
}

#[test]
fn rejects_invalid_magic() {
    let codec = FrameCodec::plain().expect("codec");
    let header = FrameHeader::new(opcode::PING, 1);
    let mut frame = codec.encode(&header, b"x").expect("encode");
    frame[0] ^= 0xFF;

    assert!(matches!(
        FrameHeader::decode(&frame),
        Err(ProtocolError::InvalidMagic)
    ));
}

#[test]
fn rejects_header_checksum_mismatch() {
    let codec = FrameCodec::plain().expect("codec");
    let header = FrameHeader::new(opcode::PING, 1);
    let mut frame = codec.encode(&header, b"x").expect("encode");
    frame[6] ^= 0x01;

    assert!(matches!(
        FrameHeader::decode(&frame),
        Err(ProtocolError::HeaderChecksumMismatch { .. })
    ));
}

#[test]
fn rejects_payload_checksum_mismatch() {
    let codec = FrameCodec::plain().expect("codec");
    let header = FrameHeader::new(opcode::PING, 1);
    let mut frame = codec.encode(&header, b"payload").expect("encode");
    frame[HEADER_LEN] ^= 0xFF;
    let decoded = FrameHeader::decode(&frame).expect("header");

    assert!(matches!(
        codec.decode(&decoded, &frame[HEADER_LEN..]),
        Err(ProtocolError::PayloadChecksumMismatch { .. })
    ));
}

#[test]
fn rejects_truncated_header() {
    assert!(matches!(
        FrameHeader::decode(&[0_u8; HEADER_LEN - 1]),
        Err(ProtocolError::FrameTooShort { .. })
    ));
}

#[test]
fn rejects_reserved_flags() {
    let codec = FrameCodec::plain().expect("codec");
    let header = FrameHeader::new(opcode::PING, 1).with_flags(0x8000);

    assert!(matches!(
        codec.encode(&header, b"x"),
        Err(ProtocolError::ReservedFlags(_))
    ));
}

#[test]
fn enforces_frame_size_limit() {
    let codec = FrameCodec::new(CompressionAlgorithm::None, 0, 4, 4).expect("codec");
    let header = FrameHeader::new(opcode::DOC_PUT, 1);

    assert!(matches!(
        codec.encode(&header, b"12345"),
        Err(ProtocolError::PayloadTooLarge { .. })
    ));
}

#[test]
fn clears_compressed_flag_for_plain_codec() {
    let codec = FrameCodec::plain().expect("codec");
    let header = FrameHeader::new(opcode::PING, 1);
    let frame = codec
        .encode(&header.with_flags(FLAG_COMPRESSED), b"x")
        .expect("encode");
    let decoded = FrameHeader::decode(&frame).expect("header");

    assert!(!decoded.is_compressed());
}

#[test]
fn default_limits_are_exposed() {
    assert_eq!(DEFAULT_MAX_FRAME_SIZE, 16 * 1024 * 1024);
    assert_eq!(DEFAULT_MAX_DECOMPRESSED_SIZE, 16 * 1024 * 1024);
}
