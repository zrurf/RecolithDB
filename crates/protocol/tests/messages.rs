use prost::Message;
use recolith_protocol::messages::{
    AggregateColumn, AggregateFunction, AggregateRequest, AggregateResponse, AggregateRow,
    AuthMethod, BucketDefinition, BucketPolicy, CatalogAlterRequest, CatalogApplyRequest,
    CatalogDropRequest, CatalogIndexTarget, CollectionDefinition, CollectionMetadata,
    DescribeCollectionRequest, DescribeCollectionResponse, DocumentPutRequest, ExplainDrive,
    ExplainFieldSelection, ExplainFilter, ExplainFilterOp, ExplainIndex, ExplainKeyFilter,
    ExplainOrder, ExplainRequest, ExplainResponse, ExplainScanPlan, FieldConversion, FieldMapping,
    FieldValue, FragmentDefinition, FragmentMetadata, FragmentPayload, HelloRequest,
    IndexDefinition, IndexKind, KeyFieldDefinition, KeyFieldMetadata, KeyFieldType,
    KeyspaceDefinition, NamedParameter, QueryRequest, QueryStreamRequest, RequestMeta,
    SchemaDefinition, SchemaField, SchemaList, SchemaMigration, SchemaScalarType, SchemaType,
    SetBucketSchema, Value, catalog_alter_request, catalog_drop_request, compression_bit,
    describe_collection_request, explain_response, feature, field_mapping, schema_type, value,
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
        cursor: b"next-page".to_vec(),
    };

    let encoded = query.encode_to_vec();
    let decoded = QueryRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, query);
}

#[test]
fn roundtrips_query_stream_request() {
    let request = QueryStreamRequest {
        meta: Some(RequestMeta::default()),
        partition: 2,
        rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
              INCLUDE FRAGMENTS profile"
            .to_owned(),
        parameters: Vec::new(),
        limit: 100,
        chunk_size: 10,
    };

    let encoded = request.encode_to_vec();
    let decoded = QueryStreamRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, request);
}

#[test]
fn roundtrips_collection_metadata() {
    let string_type = SchemaType {
        kind: Some(schema_type::Kind::Scalar(SchemaScalarType::String as i32)),
    };
    let response = DescribeCollectionResponse {
        catalog_generation: 7,
        collection: Some(CollectionMetadata {
            id: 1,
            keyspace_id: 1,
            keyspace: "game".to_owned(),
            name: "player".to_owned(),
            affinity_group: "player".to_owned(),
            key_fields: vec![KeyFieldMetadata {
                name: "player_id".to_owned(),
                r#type: KeyFieldType::U64 as i32,
            }],
            fragments: vec![FragmentMetadata {
                id: 1,
                name: "profile".to_owned(),
                cardinality: recolith_protocol::messages::FragmentCardinality::One as i32,
                bucket_id: 1,
                schema_id: 1,
                schema_version: 1,
                fields: vec![
                    SchemaField {
                        id: 1,
                        name: "name".to_owned(),
                        optional: false,
                        r#type: Some(string_type.clone()),
                    },
                    SchemaField {
                        id: 2,
                        name: "tags".to_owned(),
                        optional: true,
                        r#type: Some(SchemaType {
                            kind: Some(schema_type::Kind::List(Box::new(SchemaList {
                                element: Some(Box::new(string_type)),
                            }))),
                        }),
                    },
                ],
            }],
            indexes: Vec::new(),
        }),
    };

    let encoded = response.encode_to_vec();
    let decoded = DescribeCollectionResponse::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, response);
}

#[test]
fn roundtrips_catalog_apply_request() {
    let request = CatalogApplyRequest {
        meta: Some(RequestMeta {
            catalog_generation: 7,
            ..Default::default()
        }),
        keyspaces: vec![KeyspaceDefinition {
            id: 1,
            name: "game".to_owned(),
        }],
        schemas: vec![SchemaDefinition {
            id: 1,
            version: 1,
            idl: "record profile { 1: name: string; }".to_owned(),
        }],
        buckets: vec![BucketDefinition {
            id: 1,
            keyspace_id: 1,
            name: "player_profile".to_owned(),
            partitions: 16,
            policy: BucketPolicy::Strict as i32,
            schema_id: 1,
            schema_version: 1,
        }],
        collections: vec![CollectionDefinition {
            id: 1,
            keyspace_id: 1,
            name: "player".to_owned(),
            affinity_group: "player".to_owned(),
            key_fields: vec![KeyFieldDefinition {
                name: "player_id".to_owned(),
                r#type: KeyFieldType::U64 as i32,
            }],
            fragments: vec![FragmentDefinition {
                id: 1,
                name: "profile".to_owned(),
                bucket_id: 1,
                cardinality: recolith_protocol::messages::FragmentCardinality::One as i32,
            }],
        }],
        indexes: vec![IndexDefinition {
            id: 1,
            bucket_id: 1,
            name: "by-level".to_owned(),
            kind: IndexKind::Range as i32,
            fields: vec![2],
        }],
    };

    let encoded = request.encode_to_vec();
    let decoded = CatalogApplyRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, request);
}

#[test]
fn roundtrips_catalog_drop_request() {
    let request = CatalogDropRequest {
        meta: Some(RequestMeta {
            catalog_generation: 9,
            ..Default::default()
        }),
        purge: true,
        target: Some(catalog_drop_request::Target::Index(CatalogIndexTarget {
            bucket_id: 1,
            index_id: 2,
        })),
    };

    let encoded = request.encode_to_vec();
    let decoded = CatalogDropRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, request);
}

#[test]
fn roundtrips_catalog_alter_request() {
    let request = CatalogAlterRequest {
        meta: Some(RequestMeta {
            catalog_generation: 4,
            ..Default::default()
        }),
        change: Some(catalog_alter_request::Change::BucketSchema(
            SetBucketSchema {
                bucket_id: 1,
                schema_version: 3,
                migration: Some(SchemaMigration {
                    mappings: vec![FieldMapping {
                        to_field: 3,
                        source: Some(field_mapping::Source::DefaultValue(Value {
                            kind: Some(value::Kind::StringValue("default".to_owned())),
                        })),
                        conversion: FieldConversion::None as i32,
                    }],
                }),
            },
        )),
    };

    let encoded = request.encode_to_vec();
    let decoded = CatalogAlterRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, request);
}

#[test]
fn roundtrips_explain_request_and_response() {
    let request = ExplainRequest {
        meta: Some(RequestMeta {
            catalog_generation: 5,
            ..Default::default()
        }),
        partition: 3,
        rql: "SELECT profile.name FROM player WHERE profile.level >= 10 \
              INCLUDE FRAGMENTS profile ORDER BY profile.level DESC"
            .to_owned(),
        parameters: Vec::new(),
    };
    let encoded = request.encode_to_vec();
    let decoded = ExplainRequest::decode(encoded.as_slice()).expect("decode request");
    assert_eq!(decoded, request);

    let response = ExplainResponse {
        catalog_generation: 7,
        plan: Some(explain_response::Plan::Scan(ExplainScanPlan {
            collection_id: 1,
            partition: 3,
            fragment_ids: vec![1],
            drives: vec![ExplainDrive {
                bucket_id: 1,
                fragment_id: 1,
                indexes: vec![ExplainIndex {
                    index_id: 2,
                    field_ids: vec![2],
                    kind: IndexKind::Range as i32,
                }],
                bucket_scan: false,
            }],
            filters: vec![ExplainFilter {
                fragment_id: 1,
                field_id: 2,
                op: ExplainFilterOp::Ge as i32,
            }],
            projections: vec![ExplainFieldSelection {
                fragment_id: 1,
                field_id: 1,
            }],
            order: Some(ExplainOrder {
                fragment_id: 1,
                field_id: 2,
                descending: true,
                index_id: Some(2),
            }),
            key_filters: vec![ExplainKeyFilter {
                field: 0,
                op: ExplainFilterOp::Eq as i32,
            }],
            candidate_limit: 10_000,
        })),
    };
    let encoded = response.encode_to_vec();
    let decoded = ExplainResponse::decode(encoded.as_slice()).expect("decode response");
    assert_eq!(decoded, response);
}

#[test]
fn roundtrips_aggregate_request_and_response() {
    let request = AggregateRequest {
        meta: Some(RequestMeta {
            catalog_generation: 3,
            ..Default::default()
        }),
        partition: 2,
        rql: "SELECT COUNT(*), SUM(profile.level) FROM player \
              WHERE profile.level >= 10 INCLUDE FRAGMENTS profile GROUP BY profile.level"
            .to_owned(),
        parameters: Vec::new(),
        limit: 25,
    };
    let encoded = request.encode_to_vec();
    let decoded = AggregateRequest::decode(encoded.as_slice()).expect("decode request");
    assert_eq!(decoded, request);

    let response = AggregateResponse {
        catalog_generation: 4,
        columns: vec![
            AggregateColumn {
                function: AggregateFunction::Count as i32,
                fragment_id: None,
                field_id: None,
            },
            AggregateColumn {
                function: AggregateFunction::Sum as i32,
                fragment_id: Some(1),
                field_id: Some(2),
            },
        ],
        group_fragment_id: Some(1),
        group_field_id: Some(2),
        rows: vec![AggregateRow {
            group: Some(Value {
                kind: Some(value::Kind::U32Value(10)),
            }),
            values: vec![
                Value {
                    kind: Some(value::Kind::U64Value(2)),
                },
                Value {
                    kind: Some(value::Kind::U64Value(30)),
                },
            ],
        }],
        truncated: false,
    };
    let encoded = response.encode_to_vec();
    let decoded = AggregateResponse::decode(encoded.as_slice()).expect("decode response");
    assert_eq!(decoded, response);
}

#[test]
fn roundtrips_describe_collection_request() {
    let request = DescribeCollectionRequest {
        meta: Some(RequestMeta {
            catalog_generation: 7,
            ..Default::default()
        }),
        keyspace: "game".to_owned(),
        target: Some(describe_collection_request::Target::CollectionName(
            "player".to_owned(),
        )),
    };

    let encoded = request.encode_to_vec();
    let decoded = DescribeCollectionRequest::decode(encoded.as_slice()).expect("decode");

    assert_eq!(decoded, request);
}
