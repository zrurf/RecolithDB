use recolith_protocol::frame::{
    DEFAULT_MAX_DECOMPRESSED_SIZE, DEFAULT_MAX_FRAME_SIZE, FrameCodec, FrameHeader, HEADER_LEN,
};
use recolith_protocol::messages::opcode;
use recolith_protocol::{CompressionAlgorithm, ProtocolError, compressor};

fn payload() -> Vec<u8> {
    (0..4096).map(|index| (index % 7) as u8).collect()
}

#[test]
fn roundtrips_zstd_frame() {
    roundtrip(CompressionAlgorithm::Zstd);
}

#[test]
fn roundtrips_lz4_frame() {
    roundtrip(CompressionAlgorithm::Lz4);
}

fn roundtrip(algorithm: CompressionAlgorithm) {
    let codec = FrameCodec::new(
        algorithm,
        16,
        DEFAULT_MAX_FRAME_SIZE,
        DEFAULT_MAX_DECOMPRESSED_SIZE,
    )
    .expect("codec");
    let header = FrameHeader::new(opcode::DOC_GET, 11);
    let payload = payload();
    let frame = codec.encode(&header, &payload).expect("encode");
    let decoded_header = FrameHeader::decode(&frame).expect("header");

    assert!(decoded_header.is_compressed());
    assert_eq!(frame[HEADER_LEN], algorithm.as_wire());
    assert_eq!(
        codec
            .decode(&decoded_header, &frame[HEADER_LEN..])
            .expect("decode"),
        payload
    );
}

#[test]
fn skips_compression_below_threshold() {
    let codec = FrameCodec::new(
        CompressionAlgorithm::Zstd,
        1024,
        DEFAULT_MAX_FRAME_SIZE,
        DEFAULT_MAX_DECOMPRESSED_SIZE,
    )
    .expect("codec");
    let header = FrameHeader::new(opcode::PING, 1);
    let payload = b"small payload";
    let frame = codec.encode(&header, payload).expect("encode");
    let decoded_header = FrameHeader::decode(&frame).expect("header");

    assert!(!decoded_header.is_compressed());
    assert_eq!(
        codec
            .decode(&decoded_header, &frame[HEADER_LEN..])
            .expect("decode"),
        payload
    );
}

#[test]
fn rejects_algorithm_mismatch() {
    let zstd = FrameCodec::new(
        CompressionAlgorithm::Zstd,
        1,
        DEFAULT_MAX_FRAME_SIZE,
        DEFAULT_MAX_DECOMPRESSED_SIZE,
    )
    .expect("zstd");
    let lz4 = FrameCodec::new(
        CompressionAlgorithm::Lz4,
        1,
        DEFAULT_MAX_FRAME_SIZE,
        DEFAULT_MAX_DECOMPRESSED_SIZE,
    )
    .expect("lz4");
    let header = FrameHeader::new(opcode::DOC_GET, 1);
    let frame = zstd.encode(&header, &payload()).expect("encode");
    let decoded_header = FrameHeader::decode(&frame).expect("header");

    assert!(matches!(
        lz4.decode(&decoded_header, &frame[HEADER_LEN..]),
        Err(ProtocolError::UnsupportedCompression(1))
    ));
}

#[test]
fn enforces_decompressed_size_limit() {
    let encoder = FrameCodec::new(
        CompressionAlgorithm::Zstd,
        1,
        DEFAULT_MAX_FRAME_SIZE,
        DEFAULT_MAX_DECOMPRESSED_SIZE,
    )
    .expect("encoder");
    let decoder = FrameCodec::new(CompressionAlgorithm::Zstd, 1, DEFAULT_MAX_FRAME_SIZE, 16)
        .expect("decoder");
    let header = FrameHeader::new(opcode::DOC_GET, 1);
    let frame = encoder.encode(&header, &payload()).expect("encode");
    let decoded_header = FrameHeader::decode(&frame).expect("header");

    assert!(matches!(
        decoder.decode(&decoded_header, &frame[HEADER_LEN..]),
        Err(ProtocolError::DecompressionLimit { .. })
    ));
}

#[test]
fn compressor_trait_roundtrips_directly() {
    for algorithm in [CompressionAlgorithm::Zstd, CompressionAlgorithm::Lz4] {
        let codec = compressor(algorithm)
            .expect("codec")
            .expect("algorithm has a codec");
        let input = payload();
        let compressed = codec.compress(&input).expect("compress");
        let output = codec
            .decompress(&compressed, input.len(), DEFAULT_MAX_DECOMPRESSED_SIZE)
            .expect("decompress");

        assert_eq!(output, input);
    }
}
