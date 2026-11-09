//! Test fixtures shared by integration tests.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, Keyspace,
};
use recolith_schema::{Field, RecordSchema, Type};

/// Returns the directory containing TLS fixtures.
#[must_use]
pub fn tls_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/tls")
}

/// Returns the test certificate authority.
#[must_use]
pub fn tls_ca_certificate() -> PathBuf {
    tls_dir().join("ca.pem")
}

/// Returns the test server certificate chain.
#[must_use]
pub fn tls_server_certificate() -> PathBuf {
    tls_dir().join("server.pem")
}

/// Returns the test server private key.
#[must_use]
pub fn tls_server_key() -> PathBuf {
    tls_dir().join("server.key")
}

/// Builds the shared single-collection game catalog.
#[must_use]
pub fn game_catalog() -> Catalog {
    let catalog = Catalog::new();
    catalog
        .create_keyspace(Keyspace::new(KeyspaceId::new(1), "game"))
        .expect("keyspace");
    catalog
        .register_schema(RecordSchema::new(
            SchemaId::new(1),
            SchemaVersion::new(1),
            "profile",
            vec![
                Field::new(FieldId::new(1), "name", Type::String, false),
                Field::new(FieldId::new(2), "level", Type::U32, false),
            ],
        ))
        .expect("schema");
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
        .expect("bucket");
    catalog
        .create_collection(Collection::new(
            CollectionId::new(1),
            KeyspaceId::new(1),
            "player",
            "player",
            DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
            vec![Fragment::new(
                FragmentId::new(1),
                "profile",
                BucketId::new(1),
                FragmentCardinality::One,
            )],
        ))
        .expect("collection");

    catalog
}
