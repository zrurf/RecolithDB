use recolith_common::{FieldId, SchemaId, SchemaVersion};
use recolith_schema::{EnumSchema, Field, RecordSchema, Type};
use recolith_storage::{RecordData, StorageError, Value, decode_fields, encode_fields};

fn schema() -> RecordSchema {
    RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "player",
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, true),
            Field::new(
                FieldId::new(3),
                "tags",
                Type::List(Box::new(Type::String)),
                true,
            ),
            Field::new(
                FieldId::new(4),
                "attributes",
                Type::Map(Box::new(Type::String), Box::new(Type::U32)),
                true,
            ),
            Field::new(
                FieldId::new(5),
                "status",
                Type::Enum(EnumSchema::new(
                    "status",
                    vec!["Active".to_owned(), "Retired".to_owned()],
                )),
                true,
            ),
        ],
    )
}

#[test]
fn roundtrips_scalar_list_map_and_enum() {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("alex".to_owned()));
    data.insert(FieldId::new(2), Value::U32(42));
    data.insert(
        FieldId::new(3),
        Value::List(vec![
            Value::String("mage".to_owned()),
            Value::String("vip".to_owned()),
        ]),
    );
    data.insert(
        FieldId::new(4),
        Value::Map(vec![(Value::String("str".to_owned()), Value::U32(10))]),
    );
    data.insert(FieldId::new(5), Value::Enum(1));

    let encoded = encode_fields(&schema(), &data).expect("encode");
    let decoded = decode_fields(&schema(), &encoded).expect("decode");

    assert_eq!(decoded, data);
}

#[test]
fn omits_absent_optional_fields() {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("alex".to_owned()));

    let encoded = encode_fields(&schema(), &data).expect("encode");
    let decoded = decode_fields(&schema(), &encoded).expect("decode");

    assert_eq!(decoded, data);
}

#[test]
fn rejects_missing_required_field() {
    assert!(matches!(
        encode_fields(&schema(), &RecordData::new()),
        Err(StorageError::MissingField(_))
    ));
}

#[test]
fn rejects_type_mismatch() {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::U32(1));

    assert!(matches!(
        encode_fields(&schema(), &data),
        Err(StorageError::TypeMismatch { .. })
    ));
}

#[test]
fn preserves_unknown_field_entries() {
    let mut writer_fields = schema().fields().to_vec();
    writer_fields.push(Field::new(
        FieldId::new(99),
        "future_field",
        Type::String,
        true,
    ));
    let writer = RecordSchema::new(
        SchemaId::new(1),
        SchemaVersion::new(1),
        "player",
        writer_fields,
    );
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("alex".to_owned()));
    data.insert(FieldId::new(99), Value::String("unknown".to_owned()));

    let encoded = encode_fields(&writer, &data).expect("encode");
    let decoded = decode_fields(&schema(), &encoded).expect("decode");

    assert!(decoded.unknown().contains_key(&FieldId::new(99)));
    assert_eq!(
        encode_fields(&schema(), &decoded).expect("re-encode"),
        encoded
    );
}

#[test]
fn roundtrips_nested_record() {
    let nested = RecordSchema::new(
        SchemaId::new(2),
        SchemaVersion::new(1),
        "stats",
        vec![Field::new(FieldId::new(1), "power", Type::U32, false)],
    );
    let outer = RecordSchema::new(
        SchemaId::new(3),
        SchemaVersion::new(1),
        "player",
        vec![Field::new(
            FieldId::new(1),
            "stats",
            Type::Record(Box::new(nested)),
            false,
        )],
    );

    let mut stats = RecordData::new();
    stats.insert(FieldId::new(1), Value::U32(99));
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::Record(Box::new(stats)));

    let encoded = encode_fields(&outer, &data).expect("encode");
    let decoded = decode_fields(&outer, &encoded).expect("decode");

    assert_eq!(decoded, data);
}
