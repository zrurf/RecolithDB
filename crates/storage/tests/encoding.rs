mod record_key {
    use recolith_common::{BucketId, FragmentId, PartitionId};
    use recolith_storage::{
        decode_record_key, decode_record_key_head, encode_bucket_prefix, encode_document_prefix,
        encode_index_key, encode_record_key,
    };

    #[test]
    fn record_key_extends_document_prefix() {
        let prefix = encode_document_prefix(BucketId::new(7), PartitionId::new(3), b"alex");
        let key = encode_record_key(
            BucketId::new(7),
            PartitionId::new(3),
            b"alex",
            FragmentId::new(2),
            b"slot-1",
        );

        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn decodes_record_key_prefix() {
        let key = encode_record_key(
            BucketId::new(7),
            PartitionId::new(3),
            b"alex",
            FragmentId::new(2),
            b"",
        );
        let head = decode_record_key_head(&key).expect("head");

        assert_eq!(head.bucket(), BucketId::new(7));
        assert_eq!(head.partition(), PartitionId::new(3));
        assert!(decode_record_key_head(b"short").is_err());
    }

    #[test]
    fn decodes_complete_record_keys_and_rejects_index_keys() {
        let key = encode_record_key(
            BucketId::new(7),
            PartitionId::new(3),
            b"alex",
            FragmentId::new(2),
            b"slot-1",
        );
        let decoded = decode_record_key(&key).expect("record key");

        assert_eq!(decoded.head().bucket(), BucketId::new(7));
        assert_eq!(decoded.head().partition(), PartitionId::new(3));
        assert_eq!(decoded.document_key(), b"alex");
        assert_eq!(decoded.fragment(), FragmentId::new(2));
        assert_eq!(decoded.subkey(), b"slot-1");

        let index = encode_index_key(
            BucketId::new(7),
            PartitionId::new(3),
            recolith_common::IndexId::new(4),
            b"level:10",
            &key,
        );
        assert!(decode_record_key(&index).is_err());
        assert!(decode_record_key(&key[..key.len() - 1]).is_err());
    }

    #[test]
    fn bucket_prefix_is_big_endian() {
        assert_eq!(
            encode_bucket_prefix(BucketId::new(1)),
            vec![0, 0, 0, 0, 0, 0, 0, 1]
        );
    }
}

mod document_key {
    use recolith_model::{DocumentKeySchema, KeyField, KeyType, KeyValue};
    use recolith_storage::{MAX_DOCUMENT_KEY_LEN, StorageError, encode_document_key};

    fn schema() -> DocumentKeySchema {
        DocumentKeySchema::new(vec![
            KeyField::new("player_id", KeyType::U64),
            KeyField::new("slot", KeyType::U64),
        ])
    }

    #[test]
    fn encodes_composite_key_components_in_order() {
        let key =
            encode_document_key(&schema(), &[KeyValue::U64(7), KeyValue::U64(3)]).expect("key");

        assert_eq!(key, vec![0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 3]);
    }

    #[test]
    fn composite_string_components_are_unambiguous() {
        let string_schema = DocumentKeySchema::new(vec![
            KeyField::new("first", KeyType::String),
            KeyField::new("second", KeyType::String),
        ]);
        let first = encode_document_key(
            &string_schema,
            &[
                KeyValue::String("a".to_owned()),
                KeyValue::String("bc".to_owned()),
            ],
        )
        .expect("first");
        let second = encode_document_key(
            &string_schema,
            &[
                KeyValue::String("ab".to_owned()),
                KeyValue::String("c".to_owned()),
            ],
        )
        .expect("second");

        assert_ne!(first, second);
    }

    #[test]
    fn signed_keys_order_by_value() {
        let signed = DocumentKeySchema::new(vec![KeyField::new("id", KeyType::I64)]);
        let negative = encode_document_key(&signed, &[KeyValue::I64(-1)]).expect("negative");
        let zero = encode_document_key(&signed, &[KeyValue::I64(0)]).expect("zero");

        assert!(negative < zero);
    }

    #[test]
    fn rejects_type_and_count_mismatches() {
        assert!(matches!(
            encode_document_key(&schema(), &[KeyValue::U64(7)]),
            Err(StorageError::InvalidKey(_))
        ));
        assert!(matches!(
            encode_document_key(
                &schema(),
                &[KeyValue::String("seven".to_owned()), KeyValue::U64(3)]
            ),
            Err(StorageError::InvalidKey(_))
        ));
    }

    #[test]
    fn rejects_oversized_key() {
        let bytes_schema = DocumentKeySchema::new(vec![KeyField::new("blob", KeyType::Bytes)]);

        assert!(matches!(
            encode_document_key(
                &bytes_schema,
                &[KeyValue::Bytes(vec![b'x'; MAX_DOCUMENT_KEY_LEN])]
            ),
            Err(StorageError::InvalidKey(_))
        ));
    }
}

mod index_key {
    use recolith_common::{BucketId, IndexId, PartitionId};
    use recolith_storage::{encode_index_key, encode_index_prefix};

    #[test]
    fn index_key_extends_partition_scoped_prefix() {
        let prefix = encode_index_prefix(BucketId::new(2), PartitionId::new(3), IndexId::new(4));
        let key = encode_index_key(
            BucketId::new(2),
            PartitionId::new(3),
            IndexId::new(4),
            b"level:10",
            b"player:1",
        );

        assert!(key.starts_with(&prefix));
        assert!(key.ends_with(b"player:1"));
        assert_eq!(&prefix[..12], &key[..12]);
    }
}

mod index_value {
    use recolith_common::Timestamp;
    use recolith_storage::{StorageError, Value, encode_index_value};

    #[test]
    fn encodes_unsigned_integers_big_endian() {
        assert_eq!(
            encode_index_value(&Value::U32(1)).expect("encode"),
            vec![0x04, 0, 0, 0, 1]
        );
    }

    #[test]
    fn signed_values_order_after_bias() {
        let negative = encode_index_value(&Value::I32(-1)).expect("negative");
        let zero = encode_index_value(&Value::I32(0)).expect("zero");
        let positive = encode_index_value(&Value::I32(1)).expect("positive");

        assert!(negative < zero);
        assert!(zero < positive);
    }

    #[test]
    fn strings_preserve_order_with_embedded_nul() {
        let empty = encode_index_value(&Value::String(String::new())).expect("empty");
        let a = encode_index_value(&Value::String("a".to_owned())).expect("a");
        let a_nul = encode_index_value(&Value::String("a\0".to_owned())).expect("a nul");
        let ab = encode_index_value(&Value::String("ab".to_owned())).expect("ab");

        assert!(empty < a);
        assert!(a < a_nul);
        assert!(a_nul < ab);
    }

    #[test]
    fn encodes_timestamps_and_durations() {
        assert_eq!(
            encode_index_value(&Value::Timestamp(Timestamp::from_micros(7))).expect("timestamp"),
            vec![0x08, 0, 0, 0, 0, 0, 0, 0, 7]
        );
        assert_eq!(
            encode_index_value(&Value::Duration(7)).expect("duration"),
            vec![0x09, 0, 0, 0, 0, 0, 0, 0, 7]
        );
    }

    #[test]
    fn rejects_non_scalar_values() {
        assert!(matches!(
            encode_index_value(&Value::F64(1.0)),
            Err(StorageError::UnindexableValue("f64"))
        ));
        assert!(matches!(
            encode_index_value(&Value::List(Vec::new())),
            Err(StorageError::UnindexableValue("list"))
        ));
    }
}
