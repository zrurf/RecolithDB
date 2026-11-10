use recolith_common::FieldId;
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{
    FieldMapping, MigrationConversion, MigrationError, MigrationSource, SchemaMigration,
    migrate_record,
};
use recolith_storage::{RecordData, Value};

fn schema(version: u32, fields: Vec<Field>) -> RecordSchema {
    RecordSchema::new(
        recolith_common::SchemaId::new(1),
        recolith_common::SchemaVersion::new(version),
        "profile",
        fields,
    )
}

fn old_data() -> RecordData {
    let mut data = RecordData::new();
    data.insert(FieldId::new(1), Value::String("alex".to_owned()));
    data.insert(FieldId::new(2), Value::U32(10));
    data
}

#[test]
fn maps_fields_with_conversion_and_defaults() {
    let target = schema(
        2,
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U64, false),
            Field::new(FieldId::new(3), "nickname", Type::String, true),
        ],
    );
    let migration = SchemaMigration {
        mappings: vec![
            FieldMapping {
                to: FieldId::new(2),
                source: MigrationSource::Field(FieldId::new(2)),
                conversion: MigrationConversion::U32ToU64,
            },
            FieldMapping {
                to: FieldId::new(3),
                source: MigrationSource::Default(Value::String("al".to_owned())),
                conversion: MigrationConversion::None,
            },
        ],
    };

    let migrated = migrate_record(&old_data(), &target, &migration).expect("migrate");

    assert_eq!(
        migrated.get(FieldId::new(1)),
        Some(&Value::String("alex".to_owned()))
    );
    assert_eq!(migrated.get(FieldId::new(2)), Some(&Value::U64(10)));
    assert_eq!(
        migrated.get(FieldId::new(3)),
        Some(&Value::String("al".to_owned()))
    );
}

#[test]
fn drops_unmapped_fields_and_rejects_invalid_mappings() {
    let target = schema(
        2,
        vec![
            Field::new(FieldId::new(1), "name", Type::String, false),
            Field::new(FieldId::new(2), "level", Type::U64, false),
            Field::new(FieldId::new(3), "title", Type::String, false),
        ],
    );
    let missing = SchemaMigration {
        mappings: vec![FieldMapping {
            to: FieldId::new(2),
            source: MigrationSource::Field(FieldId::new(2)),
            conversion: MigrationConversion::U32ToU64,
        }],
    };
    assert!(matches!(
        migrate_record(&old_data(), &target, &missing),
        Err(MigrationError::MissingRequired(field)) if field == FieldId::new(3)
    ));

    let duplicate = SchemaMigration {
        mappings: vec![
            FieldMapping {
                to: FieldId::new(2),
                source: MigrationSource::Field(FieldId::new(2)),
                conversion: MigrationConversion::U32ToU64,
            },
            FieldMapping {
                to: FieldId::new(2),
                source: MigrationSource::Default(Value::U64(1)),
                conversion: MigrationConversion::None,
            },
            FieldMapping {
                to: FieldId::new(3),
                source: MigrationSource::Default(Value::String("title".to_owned())),
                conversion: MigrationConversion::None,
            },
        ],
    };
    assert!(matches!(
        migrate_record(&old_data(), &target, &duplicate),
        Err(MigrationError::DuplicateTarget(field)) if field == FieldId::new(2)
    ));

    let invalid = SchemaMigration {
        mappings: vec![
            FieldMapping {
                to: FieldId::new(2),
                source: MigrationSource::Field(FieldId::new(1)),
                conversion: MigrationConversion::U32ToU64,
            },
            FieldMapping {
                to: FieldId::new(3),
                source: MigrationSource::Default(Value::String("title".to_owned())),
                conversion: MigrationConversion::None,
            },
        ],
    };
    assert!(matches!(
        migrate_record(&old_data(), &target, &invalid),
        Err(MigrationError::InvalidConversion(_, field)) if field == FieldId::new(2)
    ));

    let renamed = SchemaMigration {
        mappings: vec![
            FieldMapping {
                to: FieldId::new(2),
                source: MigrationSource::Field(FieldId::new(2)),
                conversion: MigrationConversion::U32ToU64,
            },
            FieldMapping {
                to: FieldId::new(3),
                source: MigrationSource::Field(FieldId::new(99)),
                conversion: MigrationConversion::None,
            },
        ],
    };
    assert!(matches!(
        migrate_record(&old_data(), &target, &renamed),
        Err(MigrationError::MissingSource(field)) if field == FieldId::new(99)
    ));
}
