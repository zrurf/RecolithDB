use prost::Message;
use recolith_protocol::messages::{
    AuthMethod, DocumentPutRequest, FieldValue, FragmentPayload, HelloRequest, NamedParameter,
    QueryRequest, RequestMeta, Value, compression_bit, feature, value,
};

#[test]
fn roundtrips_hello_request() {
    let hello = HelloRequest {
        protocol_min_major: 1,
        protocol_min_minor: 0,
        protocol_max_major: 1,
        protocol_max_minor: 0,
        client_id: "test-client".to_owned(),
        client_version: "0.1.0".to_owned(),
        auth_method: AuthMethod::Bearer as i32,
        auth_token: b"secret".to_vec(),
        features: feature::BATCH | feature::DOCUMENT_READ | feature::DOCUMENT_WRITE,
        max_frame_size: 16 * 1024 * 1024,
        max_batch_subframes: 64,
        max_batch_bytes: 1024 * 1024,
        compression_algorithms: compression_bit::ZSTD | compression_bit::LZ4,
        compression_threshold: 512,
        encodings: 1,
    };

    let encoded = hello.encode_to_vec();
    let decoded = HelloRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, hello);
}

#[test]
fn roundtrips_document_put_request() {
    let put = DocumentPutRequest {
        meta: Some(RequestMeta {
            deadline_unix_micros: 99,
            catalog_generation: 3,
            trace_id: b"trace".to_vec(),
            idempotency_key: b"idem-1".to_vec(),
        }),
        collection_id: 7,
        partition: 2,
        document_key: b"player:42".to_vec(),
        fragments: vec![FragmentPayload {
            fragment_id: 1,
            subkey: Vec::new(),
            fields: vec![FieldValue {
                field_id: 2,
                value: Some(Value {
                    kind: Some(value::Kind::StringValue("alex".to_owned())),
                }),
            }],
            if_match: Some(41),
        }],
    };

    let encoded = put.encode_to_vec();
    let decoded = DocumentPutRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, put);
}

#[test]
fn roundtrips_query_parameters() {
    let query = QueryRequest {
        meta: Some(RequestMeta::default()),
        partition: 0,
        rql: "SELECT profile.name FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile"
            .to_owned(),
        parameters: vec![NamedParameter {
            name: "id".to_owned(),
            value: Some(Value {
                kind: Some(value::Kind::U64Value(42)),
            }),
        }],
        limit: 10,
    };

    let encoded = query.encode_to_vec();
    let decoded = QueryRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, query);
}
