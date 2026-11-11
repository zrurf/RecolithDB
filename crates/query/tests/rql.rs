mod parser {
    use recolith_query::{RqlAggregateFunction, parse_aggregate_query};
    use recolith_query::{RqlError, RqlLiteral, RqlOp, RqlOperand, parse_document_query};

    const DOCUMENT_EXAMPLE: &str = "\
SELECT profile.name, equipment[slot = 1].item_id
FROM player
WHERE player_id = :id
  AND profile.level >= 10
INCLUDE FRAGMENTS profile, equipment
LIMIT 10;";

    #[test]
    fn parses_document_query_example() {
        let query = parse_document_query(DOCUMENT_EXAMPLE).expect("parse");

        assert_eq!(query.collection(), "player");
        assert_eq!(query.fragments(), ["profile", "equipment"]);
        assert_eq!(query.limit(), Some(10));

        let projections = query.projections();
        assert_eq!(projections.len(), 2);
        assert_eq!(projections[0].fragment(), Some("profile"));
        assert_eq!(projections[0].field(), "name");
        assert_eq!(projections[0].selector(), None);

        let selector = projections[1].selector().expect("selector");
        assert_eq!(projections[1].fragment(), Some("equipment"));
        assert_eq!(projections[1].field(), "item_id");
        assert_eq!(selector.field(), "slot");
        assert_eq!(selector.op(), RqlOp::Eq);
        assert_eq!(
            selector.operand(),
            &RqlOperand::Literal(RqlLiteral::Integer(1))
        );

        let predicates = query.predicates();
        assert_eq!(predicates.len(), 2);
        assert_eq!(predicates[0].path().fragment(), None);
        assert_eq!(predicates[0].path().field(), "player_id");
        assert_eq!(predicates[0].op(), RqlOp::Eq);
        assert_eq!(
            predicates[0].operand(),
            &RqlOperand::Parameter("id".to_owned())
        );
        assert_eq!(predicates[1].path().fragment(), Some("profile"));
        assert_eq!(predicates[1].path().field(), "level");
        assert_eq!(predicates[1].op(), RqlOp::Ge);
        assert_eq!(
            predicates[1].operand(),
            &RqlOperand::Literal(RqlLiteral::Integer(10))
        );
    }

    #[test]
    fn parses_literals_and_escapes() {
        let query = parse_document_query(
            "SELECT items.name FROM player \
         WHERE player_id = :id \
           AND items.label = 'it\\'s' \
           AND items.visible = true \
           AND items.weight = 1.5 \
           AND items.count = 18446744073709551615 \
         INCLUDE FRAGMENTS items;",
        )
        .expect("parse");

        let operands: Vec<&RqlOperand> = query
            .predicates()
            .iter()
            .map(|predicate| predicate.operand())
            .collect();
        assert_eq!(
            operands[1],
            &RqlOperand::Literal(RqlLiteral::String("it's".to_owned()))
        );
        assert_eq!(operands[2], &RqlOperand::Literal(RqlLiteral::Bool(true)));
        assert_eq!(operands[3], &RqlOperand::Literal(RqlLiteral::Float(1.5)));
        assert_eq!(
            operands[4],
            &RqlOperand::Literal(RqlLiteral::Unsigned(u64::MAX))
        );
    }

    #[test]
    fn parses_order_by_direction_and_limit() {
        let query = parse_document_query(
            "SELECT profile.level FROM player WHERE player_id = :id \
         INCLUDE FRAGMENTS profile ORDER BY profile.level DESC LIMIT 5;",
        )
        .expect("parse");

        let order = query.order().expect("order");
        assert_eq!(order.path().fragment(), Some("profile"));
        assert_eq!(order.path().field(), "level");
        assert!(order.descending());
        assert_eq!(query.limit(), Some(5));
    }

    #[test]
    fn defaults_order_to_ascending() {
        let query = parse_document_query(
            "SELECT profile.level FROM player WHERE player_id = :id \
         INCLUDE FRAGMENTS profile ORDER BY profile.level",
        )
        .expect("parse");

        let order = query.order().expect("order");
        assert!(!order.descending());
    }

    #[test]
    fn rejects_limit_before_order() {
        let result = parse_document_query(
            "SELECT profile.level FROM player WHERE player_id = :id \
         INCLUDE FRAGMENTS profile LIMIT 5 ORDER BY profile.level",
        );

        assert!(matches!(result, Err(RqlError::Syntax { .. })));
    }

    #[test]
    fn keywords_are_case_insensitive_and_comments_are_ignored() {
        let query = parse_document_query(
            "// player profile projection\n\
         select profile.name\n\
         from player\n\
         where player_id = :id\n\
         include fragments profile",
        )
        .expect("parse");

        assert_eq!(query.collection(), "player");
        assert_eq!(query.limit(), None);
    }

    #[test]
    fn rejects_missing_include_clause() {
        let result = parse_document_query("SELECT profile.name FROM player WHERE player_id = :id");

        assert!(matches!(result, Err(RqlError::Syntax { .. })));
    }

    #[test]
    fn rejects_projection_without_field() {
        let result = parse_document_query(
            "SELECT profile FROM player WHERE player_id = :id INCLUDE FRAGMENTS profile",
        );

        assert!(matches!(result, Err(RqlError::Invalid(_))));
    }

    #[test]
    fn rejects_unknown_string_escape() {
        let result = parse_document_query(
            r"SELECT profile.name FROM player WHERE profile.name = 'a\q' INCLUDE FRAGMENTS profile",
        );

        assert!(matches!(result, Err(RqlError::Invalid(_))));
    }

    #[test]
    fn rejects_negative_limit() {
        let result = parse_document_query(
            "SELECT profile.name FROM player WHERE player_id = :id \
         INCLUDE FRAGMENTS profile LIMIT -1",
        );

        assert!(matches!(result, Err(RqlError::Invalid(_))));
    }

    #[test]
    fn syntax_errors_report_position() {
        let error = parse_document_query("SELECT FROM player").expect_err("syntax error");
        let RqlError::Syntax { line, column, .. } = error else {
            panic!("expected syntax error");
        };

        assert_eq!(line, 1);
        assert!(column > 1);
    }
    #[test]
    fn parses_aggregate_query_with_grouping() {
        let query = parse_aggregate_query(
            "SELECT COUNT(*), SUM(profile.level), MIN(profile.name), \
             MAX(profile.level), AVG(profile.level) \
             FROM player WHERE profile.level >= 10 \
             INCLUDE FRAGMENTS profile GROUP BY profile.level LIMIT 5",
        )
        .expect("parse aggregate");

        assert_eq!(query.collection(), "player");
        assert_eq!(query.fragments(), ["profile"]);
        assert_eq!(query.limit(), Some(5));
        assert_eq!(query.group_by().expect("group").field(), "level");
        let aggregates = query.aggregates();
        assert_eq!(aggregates.len(), 5);
        assert_eq!(aggregates[0].function(), RqlAggregateFunction::Count);
        assert!(aggregates[0].path().is_none());
        assert_eq!(aggregates[1].function(), RqlAggregateFunction::Sum);
        assert_eq!(aggregates[1].path().expect("path").field(), "level");
        assert_eq!(aggregates[2].function(), RqlAggregateFunction::Min);
        assert_eq!(aggregates[3].function(), RqlAggregateFunction::Max);
        assert_eq!(aggregates[4].function(), RqlAggregateFunction::Avg);
    }

    #[test]
    fn parses_global_aggregate_without_group() {
        let query = parse_aggregate_query(
            "SELECT COUNT(*) FROM player WHERE profile.level >= 10 \
             INCLUDE FRAGMENTS profile",
        )
        .expect("parse aggregate");

        assert!(query.group_by().is_none());
        assert_eq!(query.limit(), None);
    }

    #[test]
    fn document_parser_rejects_aggregate_projection() {
        let result = parse_document_query(
            "SELECT COUNT(*) FROM player WHERE profile.level >= 10 \
             INCLUDE FRAGMENTS profile",
        );

        assert!(matches!(result, Err(RqlError::Syntax { .. })));
    }

    #[test]
    fn aggregate_parser_rejects_order_by_and_bad_grouping() {
        assert!(matches!(
            parse_aggregate_query(
                "SELECT COUNT(*) FROM player WHERE profile.level >= 10 \
                 INCLUDE FRAGMENTS profile ORDER BY profile.level"
            ),
            Err(RqlError::Syntax { .. })
        ));
        assert!(matches!(
            parse_aggregate_query(
                "SELECT COUNT(*) FROM player WHERE profile.level >= 10 \
                 INCLUDE FRAGMENTS profile GROUP profile.level"
            ),
            Err(RqlError::Syntax { .. })
        ));
    }
}

mod bind {
    use recolith_catalog::Catalog;
    use recolith_common::{
        BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, PartitionId, SchemaId,
        SchemaVersion,
    };
    use recolith_model::{
        Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality,
        KeyField, KeyType, Keyspace,
    };
    use recolith_schema::{Field, RecordSchema, Type};
    use recolith_storage::{Value, encode_document_key};

    use recolith_query::ir::FilterOp;
    use recolith_query::{AggregateFunction, RqlLiteral, parse_document_query};
    use recolith_query::{
        BindParameters, BoundQuery, RqlBindError, bind_aggregate_query, bind_document_query,
        bind_query, parse_aggregate_query,
    };

    fn profile_schema() -> RecordSchema {
        RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "profile",
            vec![
                Field::new(FieldId::new(1), "player_id", Type::U64, false),
                Field::new(FieldId::new(2), "name", Type::String, false),
                Field::new(FieldId::new(3), "level", Type::U32, false),
                Field::new(
                    FieldId::new(4),
                    "tags",
                    Type::List(Box::new(Type::String)),
                    true,
                ),
            ],
        )
    }

    fn item_schema() -> RecordSchema {
        RecordSchema::new(
            SchemaId::new(2),
            SchemaVersion::new(1),
            "item",
            vec![
                Field::new(FieldId::new(1), "slot", Type::U32, false),
                Field::new(FieldId::new(2), "item_id", Type::U64, false),
            ],
        )
    }

    fn key_schema() -> DocumentKeySchema {
        DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)])
    }

    fn catalog() -> Catalog {
        let catalog = Catalog::new();
        catalog
            .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
            .expect("keyspace");
        catalog.register_schema(profile_schema()).expect("profile");
        catalog.register_schema(item_schema()).expect("item");
        catalog
            .create_bucket(
                KeyspaceId::new(1),
                Bucket::new(
                    BucketId::new(1),
                    "player_profile",
                    16,
                    BucketPolicy::Strict,
                    SchemaId::new(1),
                    SchemaVersion::new(1),
                ),
            )
            .expect("profile bucket");
        catalog
            .create_bucket(
                KeyspaceId::new(1),
                Bucket::new(
                    BucketId::new(2),
                    "player_items",
                    16,
                    BucketPolicy::Strict,
                    SchemaId::new(2),
                    SchemaVersion::new(1),
                ),
            )
            .expect("item bucket");
        catalog
            .create_collection(Collection::new(
                CollectionId::new(1),
                KeyspaceId::new(1),
                "player",
                "player",
                key_schema(),
                vec![
                    Fragment::new(
                        FragmentId::new(1),
                        "profile",
                        BucketId::new(1),
                        FragmentCardinality::One,
                    ),
                    Fragment::new(
                        FragmentId::new(2),
                        "items",
                        BucketId::new(2),
                        FragmentCardinality::Many,
                    ),
                ],
            ))
            .expect("collection");

        catalog
    }

    const QUERY: &str = "\
SELECT profile.name, items[slot = 1].item_id
FROM player
WHERE player_id = :id AND profile.level >= 10
INCLUDE FRAGMENTS profile, items";

    #[test]
    fn binds_collection_paths_and_document_key() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(QUERY).expect("parse");
        let mut parameters = BindParameters::new();
        parameters.insert("id", RqlLiteral::Integer(42));

        let query = bind_document_query(&snapshot, &parsed, PartitionId::new(3), &parameters)
            .expect("bind");

        assert_eq!(query.collection_id(), CollectionId::new(1));
        assert_eq!(query.partition(), PartitionId::new(3));
        assert_eq!(
            query.document_key(),
            encode_document_key(&key_schema(), &[recolith_model::KeyValue::U64(42)]).expect("key")
        );
        assert_eq!(query.fragments(), &[FragmentId::new(1), FragmentId::new(2)]);
        assert_eq!(query.projection().len(), 2);
        assert_eq!(query.projection()[0].fragment(), FragmentId::new(1));
        assert_eq!(query.projection()[0].field(), FieldId::new(2));
        assert_eq!(query.projection()[1].fragment(), FragmentId::new(2));
        assert_eq!(query.projection()[1].field(), FieldId::new(2));

        assert_eq!(query.filters().len(), 2);
        assert_eq!(query.filters()[0].fragment(), FragmentId::new(2));
        assert_eq!(query.filters()[0].field(), FieldId::new(1));
        assert_eq!(query.filters()[0].op(), FilterOp::Eq);
        assert_eq!(query.filters()[0].value(), &Value::U32(1));
        assert_eq!(query.filters()[1].fragment(), FragmentId::new(1));
        assert_eq!(query.filters()[1].field(), FieldId::new(3));
        assert_eq!(query.filters()[1].op(), FilterOp::Ge);
        assert_eq!(query.filters()[1].value(), &Value::U32(10));
    }

    #[test]
    fn binds_missing_key_fields_as_scan_queries() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE profile.level = 1 INCLUDE FRAGMENTS profile",
        )
        .expect("parse");

        let bound = bind_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect("scan");

        assert!(matches!(bound, BoundQuery::Scan(_)));
    }

    #[test]
    fn binds_scan_ordering() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE profile.level >= 1 \
         INCLUDE FRAGMENTS profile ORDER BY profile.level DESC",
        )
        .expect("parse");

        let bound = bind_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect("scan");

        let BoundQuery::Scan(scan) = bound else {
            panic!("expected scan");
        };
        let order = scan.order().expect("order");
        assert_eq!(order.fragment(), FragmentId::new(1));
        assert_eq!(order.field(), FieldId::new(3));
        assert!(order.descending());
    }

    #[test]
    fn rejects_order_by_key_field() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE profile.level >= 1 \
         INCLUDE FRAGMENTS profile ORDER BY player_id",
        )
        .expect("parse");

        let error = bind_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("key ordering");

        assert!(matches!(error, RqlBindError::InvalidOrder(_)));
    }

    #[test]
    fn rejects_order_by_unsortable_field() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE profile.level >= 1 \
         INCLUDE FRAGMENTS profile ORDER BY profile.tags",
        )
        .expect("parse");

        let error = bind_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("list ordering");

        assert!(matches!(error, RqlBindError::UnsortableField(field) if field == "profile.tags"));
    }

    #[test]
    fn rejects_order_field_outside_include_list() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE profile.level >= 1 \
         INCLUDE FRAGMENTS profile ORDER BY items.item_id",
        )
        .expect("parse");

        let error = bind_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("fragment not included");

        assert!(matches!(error, RqlBindError::FragmentNotIncluded(name) if name == "items"));
    }

    #[test]
    fn binds_aggregate_expressions_grouping_and_point_keys() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_aggregate_query(
            "SELECT COUNT(*), SUM(profile.level), AVG(profile.level) \
             FROM player WHERE player_id = 1 \
             INCLUDE FRAGMENTS profile GROUP BY profile.level LIMIT 5",
        )
        .expect("parse aggregate");
        let query = bind_aggregate_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect("bind aggregate");

        assert_eq!(query.collection_id(), CollectionId::new(1));
        assert_eq!(query.fragments(), &[FragmentId::new(1)]);
        assert_eq!(
            query.document_key(),
            Some(
                encode_document_key(&key_schema(), &[recolith_model::KeyValue::U64(1)])
                    .expect("key")
                    .as_slice()
            )
        );
        let group = query.group().expect("group");
        assert_eq!(group.fragment(), FragmentId::new(1));
        assert_eq!(group.field(), FieldId::new(3));
        let aggregates = query.aggregates();
        assert_eq!(aggregates[0].function(), AggregateFunction::Count);
        assert_eq!(aggregates[0].fragment(), None);
        assert_eq!(aggregates[1].function(), AggregateFunction::Sum);
        assert_eq!(aggregates[1].fragment(), Some(FragmentId::new(1)));
        assert_eq!(aggregates[1].field(), Some(FieldId::new(3)));
        assert_eq!(aggregates[2].function(), AggregateFunction::Avg);
    }

    #[test]
    fn rejects_invalid_aggregate_fields_and_groups() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");

        let sum_name = parse_aggregate_query(
            "SELECT SUM(profile.name) FROM player WHERE profile.level >= 1 \
             INCLUDE FRAGMENTS profile",
        )
        .expect("parse");
        let error = bind_aggregate_query(
            &snapshot,
            &sum_name,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("sum string");
        assert!(matches!(
            error,
            RqlBindError::InvalidAggregateType { function, .. } if function == "SUM"
        ));

        let group_list = parse_aggregate_query(
            "SELECT COUNT(*) FROM player WHERE profile.level >= 1 \
             INCLUDE FRAGMENTS profile GROUP BY profile.tags",
        )
        .expect("parse");
        let error = bind_aggregate_query(
            &snapshot,
            &group_list,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("group list");
        assert!(matches!(error, RqlBindError::UnsortableGroup(field) if field == "profile.tags"));

        let group_key = parse_aggregate_query(
            "SELECT COUNT(*) FROM player WHERE profile.level >= 1 \
             INCLUDE FRAGMENTS profile GROUP BY player_id",
        )
        .expect("parse");
        let error = bind_aggregate_query(
            &snapshot,
            &group_key,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("group key");
        assert!(matches!(error, RqlBindError::InvalidAggregate(_)));
    }

    #[test]
    fn rejects_aggregate_fragment_outside_include_list() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_aggregate_query(
            "SELECT SUM(items.item_id) FROM player WHERE profile.level >= 1 \
             INCLUDE FRAGMENTS profile",
        )
        .expect("parse");

        let error = bind_aggregate_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("fragment not included");

        assert!(matches!(error, RqlBindError::FragmentNotIncluded(name) if name == "items"));
    }

    #[test]
    fn rejects_missing_parameters() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(QUERY).expect("parse");

        let error = bind_document_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("missing parameter");

        assert!(matches!(error, RqlBindError::MissingParameter(name) if name == "id"));
    }

    #[test]
    fn rejects_fragment_outside_include_list() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE player_id = 1 AND items.item_id = 2 \
         INCLUDE FRAGMENTS profile",
        )
        .expect("parse");

        let error = bind_document_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("not included");

        assert!(matches!(error, RqlBindError::FragmentNotIncluded(name) if name == "items"));
    }

    #[test]
    fn rejects_key_type_mismatch() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE player_id = 'alex' INCLUDE FRAGMENTS profile",
        )
        .expect("parse");

        let error = bind_document_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("type mismatch");

        assert!(
            matches!(error, RqlBindError::InvalidLiteral { field, .. } if field == "player_id")
        );
    }

    #[test]
    fn binds_duplicate_key_predicates_as_scan_queries() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE player_id = 1 AND player_id = 2 \
         INCLUDE FRAGMENTS profile",
        )
        .expect("parse");

        let bound = bind_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect("scan");

        let BoundQuery::Scan(scan) = bound else {
            panic!("expected scan");
        };
        assert_eq!(scan.key_filters().len(), 2);
    }
}
