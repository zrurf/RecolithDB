use recolith_model::{
    DocumentKeyError, DocumentKeySchema, KeyField, KeyType, KeyValue, MAX_DOCUMENT_KEY_LEN,
};

fn schema(fields: &[(&str, KeyType)]) -> DocumentKeySchema {
    DocumentKeySchema::new(
        fields
            .iter()
            .map(|(name, ty)| KeyField::new(*name, *ty))
            .collect(),
    )
}

#[test]
fn encodes_fixed_width_components_in_order() {
    let key = schema(&[("player_id", KeyType::U64), ("slot", KeyType::U64)])
        .encode(&[KeyValue::U64(7), KeyValue::U64(3)])
        .expect("key");

    assert_eq!(key, vec![0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 3]);
}

#[test]
fn string_components_are_unambiguous() {
    let schema = schema(&[("first", KeyType::String), ("second", KeyType::String)]);
    let first = schema
        .encode(&[
            KeyValue::String("a".to_owned()),
            KeyValue::String("bc".to_owned()),
        ])
        .expect("first");
    let second = schema
        .encode(&[
            KeyValue::String("ab".to_owned()),
            KeyValue::String("c".to_owned()),
        ])
        .expect("second");

    assert_ne!(first, second);
}

#[test]
fn signed_keys_order_by_value() {
    let schema = schema(&[("id", KeyType::I64)]);
    let negative = schema.encode(&[KeyValue::I64(-1)]).expect("negative");
    let zero = schema.encode(&[KeyValue::I64(0)]).expect("zero");

    assert!(negative < zero);
}

#[test]
fn rejects_count_and_type_mismatches() {
    let schema = schema(&[("player_id", KeyType::U64)]);
    assert_eq!(
        schema.encode(&[]),
        Err(DocumentKeyError::CountMismatch {
            expected: 1,
            actual: 0
        })
    );
    assert!(matches!(
        schema.encode(&[KeyValue::String("1".to_owned())]),
        Err(DocumentKeyError::TypeMismatch { .. })
    ));
}

#[test]
fn rejects_oversized_keys() {
    let schema = schema(&[("blob", KeyType::Bytes)]);
    assert!(matches!(
        schema.encode(&[KeyValue::Bytes(vec![1; MAX_DOCUMENT_KEY_LEN])]),
        Err(DocumentKeyError::TooLong { .. })
    ));
}
