use recolith_cli::{CliError, parse_parameter};
use recolith_protocol::messages::{Value, value};

#[test]
fn parses_scalar_parameters() {
    assert_eq!(
        parse_parameter("active=bool:true").expect("bool").value,
        Some(dynamic_value(value::Kind::BoolValue(true)))
    );
    assert_eq!(
        parse_parameter("id=u64:42").expect("u64").value,
        Some(dynamic_value(value::Kind::U64Value(42)))
    );
    assert_eq!(
        parse_parameter("name=string:alex").expect("string").value,
        Some(dynamic_value(value::Kind::StringValue("alex".to_owned())))
    );
    assert_eq!(
        parse_parameter("data=bytes:0x0102").expect("bytes").value,
        Some(dynamic_value(value::Kind::BytesValue(vec![1, 2])))
    );
    assert_eq!(
        parse_parameter("at=timestamp:123")
            .expect("timestamp")
            .value,
        Some(dynamic_value(value::Kind::TimestampMicros(123)))
    );
}

#[test]
fn rejects_malformed_parameters() {
    assert!(matches!(
        parse_parameter("id"),
        Err(CliError::InvalidParameter(_))
    ));
    assert!(matches!(
        parse_parameter("=u64:1"),
        Err(CliError::InvalidParameter(_))
    ));
    assert!(matches!(
        parse_parameter("id=u64:not-a-number"),
        Err(CliError::InvalidValue { .. })
    ));
    assert!(matches!(
        parse_parameter("id=unknown:1"),
        Err(CliError::InvalidParameter(_))
    ));
    assert!(matches!(
        parse_parameter("data=bytes:xyz"),
        Err(CliError::InvalidValue { .. })
    ));
}

fn dynamic_value(kind: value::Kind) -> Value {
    Value { kind: Some(kind) }
}
