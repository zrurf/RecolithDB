mod parser {
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
    use recolith_query::{BindParameters, RqlBindError, bind_document_query};
    use recolith_query::{RqlLiteral, parse_document_query};

    fn profile_schema() -> RecordSchema {
        RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "profile",
            vec![
                Field::new(FieldId::new(1), "player_id", Type::U64, false),
                Field::new(FieldId::new(2), "name", Type::String, false),
                Field::new(FieldId::new(3), "level", Type::U32, false),
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
    fn requires_every_document_key_field() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE profile.level = 1 INCLUDE FRAGMENTS profile",
        )
        .expect("parse");

        let error = bind_document_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("missing key");

        assert!(matches!(error, RqlBindError::MissingKeyField(name) if name == "player_id"));
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
    fn rejects_duplicate_key_predicates() {
        let catalog = catalog();
        let snapshot = catalog.snapshot().expect("snapshot");
        let parsed = parse_document_query(
            "SELECT profile.name FROM player WHERE player_id = 1 AND player_id = 2 \
         INCLUDE FRAGMENTS profile",
        )
        .expect("parse");

        let error = bind_document_query(
            &snapshot,
            &parsed,
            PartitionId::new(0),
            &BindParameters::new(),
        )
        .expect_err("duplicate key");

        assert!(matches!(error, RqlBindError::DuplicateKeyField(field) if field == "player_id"));
    }
}
