mod compat {
    use recolith_common::{FieldId, SchemaId, SchemaVersion};
    use recolith_schema::{
        Compatibility, EnumSchema, Field, RecordSchema, Type, check_compatibility,
    };

    fn record(fields: Vec<Field>) -> RecordSchema {
        RecordSchema::new(SchemaId::new(1), SchemaVersion::new(1), "player", fields)
    }

    #[test]
    fn adding_optional_field_is_compatible() {
        let old = record(vec![Field::new(
            FieldId::new(1),
            "name",
            Type::String,
            false,
        )]);
        let new = record(vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, true),
        ]);

        assert_eq!(
            check_compatibility(&old, &new).expect("valid schemas"),
            Compatibility::Compatible
        );
    }

    #[test]
    fn adding_required_field_requires_migration() {
        let old = record(vec![Field::new(
            FieldId::new(1),
            "name",
            Type::String,
            false,
        )]);
        let new = record(vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U32, false),
        ]);

        assert_eq!(
            check_compatibility(&old, &new).expect("valid schemas"),
            Compatibility::RequiresMigration
        );
    }

    #[test]
    fn changing_type_requires_migration() {
        let old = record(vec![Field::new(FieldId::new(1), "level", Type::U32, false)]);
        let new = record(vec![Field::new(FieldId::new(1), "level", Type::U64, false)]);

        assert_eq!(
            check_compatibility(&old, &new).expect("valid schemas"),
            Compatibility::RequiresMigration
        );
    }

    #[test]
    fn appending_enum_variant_is_compatible() {
        let old = record(vec![Field::new(
            FieldId::new(1),
            "status",
            Type::Enum(EnumSchema::new("status", vec!["Active".to_owned()])),
            false,
        )]);
        let new = record(vec![Field::new(
            FieldId::new(1),
            "status",
            Type::Enum(EnumSchema::new(
                "status",
                vec!["Active".to_owned(), "Retired".to_owned()],
            )),
            false,
        )]);

        assert_eq!(
            check_compatibility(&old, &new).expect("valid schemas"),
            Compatibility::Compatible
        );
    }

    #[test]
    fn making_optional_field_required_requires_migration() {
        let old = record(vec![Field::new(
            FieldId::new(1),
            "name",
            Type::String,
            true,
        )]);
        let new = record(vec![Field::new(
            FieldId::new(1),
            "name",
            Type::String,
            false,
        )]);

        assert_eq!(
            check_compatibility(&old, &new).expect("valid schemas"),
            Compatibility::RequiresMigration
        );
    }
}

mod idl {
    use recolith_common::{FieldId, SchemaId, SchemaVersion};
    use recolith_schema::{Type, parse_record};

    fn ids() -> (SchemaId, SchemaVersion) {
        (SchemaId::new(1), SchemaVersion::new(1))
    }

    #[test]
    fn parses_scalar_and_optional_fields() {
        let input = r"
            record Player {
                name: string @1;
                optional level: u32 @2;
            }
        ";
        let (id, version) = ids();
        let record = parse_record(input, id, version).expect("valid IDL");

        assert_eq!(record.name(), "Player");
        assert_eq!(record.fields().len(), 2);
        assert_eq!(record.fields()[0].id(), FieldId::new(1));
        assert!(!record.fields()[0].optional());
        assert!(record.fields()[1].optional());
    }

    #[test]
    fn parses_list_map_and_enum_types() {
        let input = r"
            record Player {
                tags: list<string> @1;
                attributes: map<string, u32> @2;
                status: enum Status { Active, Retired } @3;
            }
        ";
        let (id, version) = ids();
        let record = parse_record(input, id, version).expect("valid IDL");

        assert!(matches!(record.fields()[0].ty(), Type::List(_)));
        assert!(matches!(record.fields()[1].ty(), Type::Map(_, _)));
        assert!(matches!(record.fields()[2].ty(), Type::Enum(_)));
    }

    #[test]
    fn rejects_duplicate_field_ids() {
        let input = r"
            record Player {
                name: string @1;
                level: u32 @1;
            }
        ";
        let (id, version) = ids();

        assert!(parse_record(input, id, version).is_err());
    }

    #[test]
    fn rejects_missing_semicolon() {
        let input = r"
            record Player {
                name: string @1
            }
        ";
        let (id, version) = ids();

        assert!(parse_record(input, id, version).is_err());
    }
}
