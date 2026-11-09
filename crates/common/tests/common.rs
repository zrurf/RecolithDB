mod bytes {
    use recolith_common::bytes::{get_uvarint, put_uvarint};

    #[test]
    fn roundtrips_varints() {
        for value in [0, 1, 127, 128, 300, u64::MAX] {
            let mut encoded = Vec::new();
            put_uvarint(value, &mut encoded);

            let (decoded, consumed) = get_uvarint(&encoded).expect("valid varint");

            assert_eq!(decoded, value);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn rejects_truncated_varint() {
        assert!(get_uvarint(&[0x80]).is_err());
    }

    #[test]
    fn rejects_overflowing_varint() {
        let encoded = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02];

        assert!(get_uvarint(&encoded).is_err());
    }
}

mod ids {
    use recolith_common::KeyspaceId;

    #[test]
    fn identifiers_are_typed_and_formattable() {
        let identifier = KeyspaceId::new(42);

        assert_eq!(identifier.get(), 42);
        assert_eq!(identifier.to_string(), "42");
        assert_eq!(KeyspaceId::from(42), identifier);
    }
}

mod uri {
    use recolith_common::Uri;

    #[test]
    fn accepts_recolith_uris() {
        let uri = Uri::parse("recolith://cluster/game/collections/player").expect("valid URI");

        assert_eq!(uri.as_str(), "recolith://cluster/game/collections/player");
    }

    #[test]
    fn rejects_other_schemes() {
        assert!(Uri::parse("https://example.com").is_err());
    }
}
