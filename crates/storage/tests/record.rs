mod frame {
    use recolith_common::{CommitTs, SchemaId, SchemaVersion};
    use recolith_storage::{HEADER_LEN, RecordHeader, decode_record, encode_record};

    fn header() -> RecordHeader {
        RecordHeader::new(SchemaId::new(9), SchemaVersion::new(2), CommitTs::new(42))
    }

    #[test]
    fn roundtrips_record_frame() {
        let encoded = encode_record(&header(), b"payload").expect("encode");
        let decoded = decode_record(&encoded).expect("decode");

        assert_eq!(encoded.len(), HEADER_LEN + 7);
        assert_eq!(decoded.payload(), b"payload");
        assert_eq!(decoded.header().schema_id(), SchemaId::new(9));
        assert_eq!(decoded.header().commit_ts(), CommitTs::new(42));
    }

    #[test]
    fn rejects_corrupt_payload() {
        let mut encoded = encode_record(&header(), b"payload").expect("encode");
        encoded[HEADER_LEN] ^= 0xFF;

        assert!(decode_record(&encoded).is_err());
    }

    #[test]
    fn rejects_truncated_record() {
        assert!(decode_record(&[0_u8; HEADER_LEN - 1]).is_err());
    }
}

mod checksum {
    use recolith_storage::crc32c;

    #[test]
    fn matches_standard_crc32c_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }
}
