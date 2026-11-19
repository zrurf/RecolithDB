use std::sync::Arc;

use recolith_backend::StorageBackend;
use recolith_backend_memory::MemoryBackend;
use recolith_catalog::Catalog;
use recolith_common::{
    BucketId, CollectionId, FieldId, FragmentId, KeyspaceId, PartitionId, SchemaId, SchemaVersion,
};
use recolith_model::{
    Bucket, BucketPolicy, Collection, DocumentKeySchema, Fragment, FragmentCardinality, KeyField,
    KeyType, Keyspace,
};
use recolith_protocol::messages::{
    WorkflowDefinition, WorkflowIncrementField, WorkflowPutDocument, WorkflowState, WorkflowStep,
    WorkflowStepStatus, value, workflow_step,
};
use recolith_schema::{Field, RecordSchema, Type};
use recolith_server::{
    DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest, WorkflowError,
};
use recolith_storage::{Value, encode_document_key};

fn catalog() -> Catalog {
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
                Field::new(FieldId::new(3), "gold", Type::U64, false),
                Field::new(FieldId::new(4), "email", Type::String, true).with_unique(true),
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

fn store() -> (DocumentStore<MemoryBackend>, Arc<MemoryBackend>, Vec<u8>) {
    let backend = Arc::new(MemoryBackend::new());
    let key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[recolith_model::KeyValue::U64(1)],
    )
    .expect("key");

    (
        DocumentStore::new(Arc::new(catalog()), backend.clone()),
        backend,
        key,
    )
}

fn profile(name: &str, gold: u64) -> recolith_storage::RecordData {
    let mut data = recolith_storage::RecordData::new();
    data.insert(FieldId::new(1), Value::String(name.to_owned()));
    data.insert(FieldId::new(2), Value::U32(10));
    data.insert(FieldId::new(3), Value::U64(gold));

    data
}

fn profile_with_email(name: &str, email: &str, gold: u64) -> recolith_storage::RecordData {
    let mut data = profile(name, gold);
    data.insert(FieldId::new(4), Value::String(email.to_owned()));

    data
}

fn fragment_payload(
    fragment_id: u32,
    fields: Vec<recolith_protocol::messages::FieldValue>,
) -> recolith_protocol::messages::FragmentPayload {
    recolith_protocol::messages::FragmentPayload {
        fragment_id,
        subkey: Vec::new(),
        fields,
        if_match: None,
    }
}

fn put_step(key: &[u8], name: &str, gold: u64) -> WorkflowStep {
    WorkflowStep {
        kind: Some(workflow_step::Kind::PutDocument(WorkflowPutDocument {
            collection_id: 1,
            partition: 0,
            document_key: key.to_vec(),
            fragments: vec![fragment_payload(
                1,
                vec![
                    recolith_protocol::messages::FieldValue {
                        field_id: 1,
                        value: Some(recolith_protocol::messages::Value {
                            kind: Some(value::Kind::StringValue(name.to_owned())),
                        }),
                    },
                    recolith_protocol::messages::FieldValue {
                        field_id: 2,
                        value: Some(recolith_protocol::messages::Value {
                            kind: Some(value::Kind::U32Value(10)),
                        }),
                    },
                    recolith_protocol::messages::FieldValue {
                        field_id: 3,
                        value: Some(recolith_protocol::messages::Value {
                            kind: Some(value::Kind::U64Value(gold)),
                        }),
                    },
                ],
            )],
            ttl_seconds: None,
        })),
    }
}

fn tcc_put_step(key: &[u8], email: &str) -> WorkflowStep {
    WorkflowStep {
        kind: Some(workflow_step::Kind::PutDocument(WorkflowPutDocument {
            collection_id: 1,
            partition: 0,
            document_key: key.to_vec(),
            fragments: vec![fragment_payload(
                1,
                vec![
                    recolith_protocol::messages::FieldValue {
                        field_id: 1,
                        value: Some(recolith_protocol::messages::Value {
                            kind: Some(value::Kind::StringValue("player".to_owned())),
                        }),
                    },
                    recolith_protocol::messages::FieldValue {
                        field_id: 2,
                        value: Some(recolith_protocol::messages::Value {
                            kind: Some(value::Kind::U32Value(1)),
                        }),
                    },
                    recolith_protocol::messages::FieldValue {
                        field_id: 3,
                        value: Some(recolith_protocol::messages::Value {
                            kind: Some(value::Kind::U64Value(0)),
                        }),
                    },
                    recolith_protocol::messages::FieldValue {
                        field_id: 4,
                        value: Some(recolith_protocol::messages::Value {
                            kind: Some(value::Kind::StringValue(email.to_owned())),
                        }),
                    },
                ],
            )],
            ttl_seconds: None,
        })),
    }
}

fn increment_step(key: &[u8], delta: i64) -> WorkflowStep {
    WorkflowStep {
        kind: Some(workflow_step::Kind::IncrementField(
            WorkflowIncrementField {
                collection_id: 1,
                partition: 0,
                document_key: key.to_vec(),
                fragment_id: 1,
                field_id: 3,
                delta,
            },
        )),
    }
}

fn delete_step(key: &[u8]) -> WorkflowStep {
    WorkflowStep {
        kind: Some(workflow_step::Kind::DeleteDocument(
            recolith_protocol::messages::WorkflowDeleteDocument {
                collection_id: 1,
                partition: 0,
                document_key: key.to_vec(),
            },
        )),
    }
}

fn claim_step(key: &[u8], name: &str, ttl: u64) -> WorkflowStep {
    WorkflowStep {
        kind: Some(workflow_step::Kind::ClaimReservation(
            recolith_protocol::messages::WorkflowClaimReservation {
                schema_id: 1,
                field_id: 4,
                value: Some(recolith_protocol::messages::Value {
                    kind: Some(value::Kind::StringValue(name.to_owned())),
                }),
                collection_id: 1,
                partition: 0,
                document_key: key.to_vec(),
                ttl_seconds: ttl,
            },
        )),
    }
}

fn confirm_step(key: &[u8], name: &str) -> WorkflowStep {
    WorkflowStep {
        kind: Some(workflow_step::Kind::ConfirmReservation(
            recolith_protocol::messages::WorkflowConfirmReservation {
                schema_id: 1,
                field_id: 4,
                value: Some(recolith_protocol::messages::Value {
                    kind: Some(value::Kind::StringValue(name.to_owned())),
                }),
                collection_id: 1,
                partition: 0,
                document_key: key.to_vec(),
            },
        )),
    }
}

fn steps(items: Vec<WorkflowStep>) -> WorkflowDefinition {
    WorkflowDefinition { steps: items }
}

fn fragment_data(store: &DocumentStore<MemoryBackend>, key: &[u8]) -> recolith_storage::RecordData {
    let request = recolith_query::DocumentReadRequest::new(
        CollectionId::new(1),
        key.to_vec(),
        PartitionId::new(0),
    );
    let fragments = store.read(&request).expect("read");
    fragments[&FragmentId::new(1)][0].data().clone()
}

fn reservation_exists(backend: &MemoryBackend, name: &str) -> bool {
    use recolith_backend::Snapshot;

    let normalized = name.trim().to_lowercase().into_bytes();
    let mut key = b"\0recolith/reservation/".to_vec();
    key.extend_from_slice(&1_u64.to_be_bytes());
    key.extend_from_slice(&4_u32.to_be_bytes());
    key.extend_from_slice(&normalized);
    backend
        .snapshot()
        .expect("snapshot")
        .get(&key)
        .expect("get")
        .is_some()
}

#[test]
fn runs_saga_steps_to_completion() {
    let (store, _backend, key) = store();
    let record = store
        .start_workflow(
            "saga-happy",
            &steps(vec![put_step(&key, "alex", 100), increment_step(&key, 50)]),
        )
        .expect("start");

    assert_eq!(
        record.state(),
        WorkflowState::Completed,
        "error: {}",
        record.error
    );
    assert_eq!(record.steps.len(), 2);
    assert!(record.steps[0].status() == WorkflowStepStatus::Succeeded);
    assert!(record.steps[1].status() == WorkflowStepStatus::Succeeded);
    let data = fragment_data(&store, &key);
    assert_eq!(
        data.get(FieldId::new(1)),
        Some(&Value::String("alex".to_owned()))
    );
    assert_eq!(data.get(FieldId::new(3)), Some(&Value::U64(150)));
}

#[test]
fn compensates_put_steps_when_a_later_step_fails() {
    let (store, backend, key) = store();
    let record = store
        .start_workflow(
            "saga-compensate",
            &steps(vec![put_step(&key, "alex", 100), increment_step(&key, 50)]),
        )
        .expect("first workflow");
    assert_eq!(
        record.state(),
        WorkflowState::Completed,
        "error: {}",
        record.error
    );

    // Second workflow: create a new document, then fail by incrementing a
    // missing document.
    let other_key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[recolith_model::KeyValue::U64(2)],
    )
    .expect("key");
    let missing_key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[recolith_model::KeyValue::U64(3)],
    )
    .expect("key");
    let record = store
        .start_workflow(
            "saga-failure",
            &steps(vec![
                put_step(&other_key, "ben", 10),
                increment_step(&missing_key, 5),
            ]),
        )
        .expect("start");

    assert_eq!(record.state(), WorkflowState::Compensated);
    assert!(record.steps[0].status() == WorkflowStepStatus::Compensated);
    assert!(record.steps[1].status() == WorkflowStepStatus::Failed);
    assert!(
        store
            .read(&recolith_query::DocumentReadRequest::new(
                CollectionId::new(1),
                other_key,
                PartitionId::new(0),
            ))
            .expect("read")
            .is_empty()
    );
    let _ = backend;
}

#[test]
fn dead_letters_when_a_succeeded_step_is_not_compensable() {
    let (store, _backend, key) = store();
    store
        .write(
            &DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
                .with_fragment(FragmentWriteRequest::new(
                    FragmentId::new(1),
                    Vec::new(),
                    profile("alex", 0),
                )),
        )
        .expect("seed");

    // Delete the document, then fail by incrementing a missing document.
    let record = store
        .start_workflow(
            "saga-dead-letter",
            &steps(vec![delete_step(&key), increment_step(&key, 5)]),
        )
        .expect("start");

    assert_eq!(record.state(), WorkflowState::Failed);
    assert!(record.steps[0].status() == WorkflowStepStatus::Succeeded);
    assert!(!record.error.is_empty());
}

#[test]
fn resubmission_is_idempotent() {
    let (store, _backend, key) = store();
    let first = store
        .start_workflow("saga-idem", &steps(vec![put_step(&key, "alex", 100)]))
        .expect("first");
    let second = store
        .start_workflow("saga-idem", &steps(vec![put_step(&key, "alex", 100)]))
        .expect("second");

    assert_eq!(first.state(), WorkflowState::Completed);
    assert_eq!(second.state(), WorkflowState::Completed);
    let data = fragment_data(&store, &key);
    assert_eq!(data.get(FieldId::new(3)), Some(&Value::U64(100)));
}

#[test]
fn resume_leaves_terminal_workflows_alone() {
    let (store, _backend, key) = store();
    store
        .start_workflow("saga-resume", &steps(vec![put_step(&key, "alex", 100)]))
        .expect("start");
    let resumed = store.resume_workflows(100).expect("resume");
    assert_eq!(resumed, 0);
}

#[test]
fn runs_tcc_claim_put_confirm() {
    let (store, backend, key) = store();
    let record = store
        .start_workflow(
            "tcc-claim",
            &steps(vec![
                claim_step(&key, "alex@example.com", 3_600),
                tcc_put_step(&key, "alex@example.com"),
                confirm_step(&key, "alex@example.com"),
            ]),
        )
        .expect("start");

    assert_eq!(
        record.state(),
        WorkflowState::Completed,
        "error: {}",
        record.error
    );
    assert!(reservation_exists(&backend, "alex@example.com"));

    // Another owner cannot claim the committed value; the workflow compensates.
    let other_key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[recolith_model::KeyValue::U64(2)],
    )
    .expect("key");
    let duplicate = store
        .start_workflow(
            "tcc-duplicate",
            &steps(vec![claim_step(&other_key, "alex@example.com", 3_600)]),
        )
        .expect("start");
    assert_eq!(duplicate.state(), WorkflowState::Compensated);
    assert!(!duplicate.error.is_empty());
}

#[test]
fn releases_claim_when_the_write_fails() {
    let (store, backend, key) = store();
    let missing_key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[recolith_model::KeyValue::U64(3)],
    )
    .expect("key");
    let record = store
        .start_workflow(
            "tcc-cancel",
            &steps(vec![
                claim_step(&key, "alex@example.com", 3_600),
                tcc_put_step(&key, "alex@example.com"),
                increment_step(&missing_key, 5),
            ]),
        )
        .expect("start");

    assert_eq!(record.state(), WorkflowState::Compensated);
    assert!(!reservation_exists(&backend, "alex@example.com"));
}

#[test]
fn rejects_writes_to_unique_fields_without_a_reservation() {
    let (store, _backend, key) = store();
    let write = DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
        .with_fragment(FragmentWriteRequest::new(
            FragmentId::new(1),
            Vec::new(),
            profile_with_email("alex", "alex@example.com", 0),
        ));

    let error = store.write(&write).expect_err("rejected");
    assert!(matches!(
        error,
        DocumentError::UniqueReservationRequired { field } if field == "email"
    ));
}

#[test]
fn accepts_writes_with_a_committed_reservation_and_releases_on_delete() {
    let (store, backend, key) = store();
    store
        .start_workflow(
            "tcc-accept",
            &steps(vec![
                claim_step(&key, "alex@example.com", 3_600),
                tcc_put_step(&key, "alex@example.com"),
                confirm_step(&key, "alex@example.com"),
            ]),
        )
        .expect("tcc");

    // Rewriting the same unique value stays allowed.
    store
        .write(
            &DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
                .with_fragment(FragmentWriteRequest::new(
                    FragmentId::new(1),
                    Vec::new(),
                    profile_with_email("alex", "alex@example.com", 10),
                )),
        )
        .expect("same value");
    assert!(reservation_exists(&backend, "alex@example.com"));

    // Clearing the unique field releases the reservation.
    store
        .write(
            &DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
                .with_fragment(FragmentWriteRequest::new(
                    FragmentId::new(1),
                    Vec::new(),
                    profile("alex", 10),
                )),
        )
        .expect("clear email");
    assert!(!reservation_exists(&backend, "alex@example.com"));

    // Re-adding the value without a reservation is rejected.
    let rename = DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
        .with_fragment(FragmentWriteRequest::new(
            FragmentId::new(1),
            Vec::new(),
            profile_with_email("alex", "alex@example.com", 10),
        ));
    assert!(matches!(
        store.write(&rename),
        Err(DocumentError::UniqueReservationRequired { field }) if field == "email"
    ));

    // Deleting the document releases the reservation.
    store
        .delete_document(CollectionId::new(1), PartitionId::new(0), &key)
        .expect("delete");
    assert!(!reservation_exists(&backend, "alex@example.com"));
}

#[test]
fn sweeps_expired_reservations() {
    let (store, backend, key) = store();
    store
        .start_workflow(
            "tcc-expire",
            &steps(vec![claim_step(&key, "alex@example.com", 3_600)]),
        )
        .expect("claim");
    assert!(reservation_exists(&backend, "alex@example.com"));

    // A reconciler pass far in the future releases expired claims whose
    // owner document does not hold the value.
    let stats = store
        .reconcile_reservations(u64::MAX, 100)
        .expect("reconcile");
    assert_eq!(stats.released, 1);
    assert_eq!(stats.confirmed, 0);
    assert!(!reservation_exists(&backend, "alex@example.com"));
}

#[test]
fn reconciler_confirms_claims_the_owner_document_caught_up_with() {
    let (store, _backend, key) = store();
    // Claim the email, then write the doc through the normal path (the write
    // is allowed because the claim exists), and let the TTL lapse without a
    // confirm step.
    store
        .start_workflow(
            "tcc-reconcile",
            &steps(vec![
                claim_step(&key, "alex@example.com", 3_600),
                tcc_put_step(&key, "alex@example.com"),
            ]),
        )
        .expect("start");

    // Force the claim to look expired.
    let stats = store
        .reconcile_reservations(u64::MAX, 100)
        .expect("reconcile");
    assert_eq!(stats.checked, 1);
    assert_eq!(stats.confirmed, 1);
    assert_eq!(stats.released, 0);
}

#[test]
fn resolves_failed_workflows_by_retry_and_abandon() {
    use recolith_protocol::messages::WorkflowResolveAction;

    let (store, _backend, key) = store();
    // Claim + put + increment missing doc: claim succeeds, put succeeds,
    // increment fails. Confirm is never reached, so the claim gets released
    // during compensation and the retry path can re-claim.
    let record = store
        .start_workflow(
            "resolve-retry",
            &steps(vec![
                claim_step(&key, "alex@example.com", 3_600),
                tcc_put_step(&key, "alex@example.com"),
                increment_step(&missing_document_key(3), 5),
            ]),
        )
        .expect("start");
    assert_eq!(record.state(), WorkflowState::Compensated);

    // A failed workflow in Compensated state cannot be resolved.
    let error = store
        .resolve_workflow("resolve-retry", WorkflowResolveAction::Retry)
        .expect_err("compensated is not resolvable");
    assert!(matches!(error, WorkflowError::InvalidWorkflow(_)));

    // Failed during step execution: dead-letter via delete then increment.
    store
        .write(
            &DocumentWrite::new(CollectionId::new(1), key.clone(), PartitionId::new(0))
                .with_fragment(FragmentWriteRequest::new(
                    FragmentId::new(1),
                    Vec::new(),
                    profile("alex", 0),
                )),
        )
        .expect("seed");
    let failed = store
        .start_workflow(
            "resolve-failed",
            &steps(vec![delete_step(&key), increment_step(&key, 5)]),
        )
        .expect("start");
    assert_eq!(failed.state(), WorkflowState::Failed);

    let resumed = store
        .resolve_workflow("resolve-failed", WorkflowResolveAction::Retry)
        .expect("retry");
    assert_eq!(resumed.state(), WorkflowState::Failed);
    assert_eq!(resumed.steps[1].status(), WorkflowStepStatus::Failed);
    assert!(!resumed.error.is_empty());

    let abandoned = store
        .resolve_workflow("resolve-failed", WorkflowResolveAction::Abandon)
        .expect("abandon");
    assert_eq!(abandoned.state(), WorkflowState::Abandoned);
}

#[test]
fn lists_workflows_with_pagination() {
    let (store, _backend, key) = store();
    for (index, name) in ["wf-a", "wf-b", "wf-c"].into_iter().enumerate() {
        store
            .start_workflow(
                name,
                &steps(vec![put_step(&key, &format!("player-{index}"), 0)]),
            )
            .expect("start");
    }

    let all = store.list_workflows("", 10).expect("list all");
    assert_eq!(all.len(), 3);
    let ids: Vec<_> = all.iter().map(|record| record.id.as_str()).collect();
    assert_eq!(ids, ["wf-a", "wf-b", "wf-c"]);

    let page = store.list_workflows("wf-b", 10).expect("list after");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id, "wf-c");

    let empty = store.list_workflows("", 0).expect("zero limit");
    assert!(empty.is_empty());
}

fn missing_document_key(player: u64) -> Vec<u8> {
    encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[recolith_model::KeyValue::U64(player)],
    )
    .expect("key")
}

#[test]
fn step_timeout_marks_and_undoes_the_slow_step() {
    let backend = Arc::new(MemoryBackend::new());
    let key = encode_document_key(
        &DocumentKeySchema::new(vec![KeyField::new("player_id", KeyType::U64)]),
        &[recolith_model::KeyValue::U64(1)],
    )
    .expect("key");
    let store = DocumentStore::new(Arc::new(catalog()), backend.clone())
        .with_workflow_step_timeout(std::time::Duration::from_nanos(1));
    let record = store
        .start_workflow(
            "saga-timeout",
            &steps(vec![put_step(&key, "alex", 100), increment_step(&key, 50)]),
        )
        .expect("start");

    assert_eq!(record.state(), WorkflowState::Compensated);
    assert!(
        record.error.contains("exceeded its"),
        "unexpected error: {}",
        record.error
    );
    assert_eq!(record.steps[0].status(), WorkflowStepStatus::Compensated);
    assert_eq!(record.steps[1].status(), WorkflowStepStatus::Pending);

    // The timed-out step applied its effect before the boundary check, so the
    // immediate compensation must have removed it.
    let request = recolith_query::DocumentReadRequest::new(
        CollectionId::new(1),
        key.to_vec(),
        PartitionId::new(0),
    );
    let fragments = store.read(&request).expect("read");
    assert!(
        fragments.get(&FragmentId::new(1)).is_none_or(Vec::is_empty),
        "timed-out step effect must be undone"
    );
}

#[test]
fn submit_then_run_reaches_completion() {
    let (store, _backend, key) = store();
    let record = store
        .submit_workflow("async-split", &steps(vec![put_step(&key, "alex", 100)]))
        .expect("submit");
    assert_eq!(record.state(), WorkflowState::Pending);

    let record = store.run_workflow("async-split").expect("run");
    assert_eq!(record.state(), WorkflowState::Completed);
    assert_eq!(record.steps[0].status(), WorkflowStepStatus::Succeeded);

    // Resubmitting an existing identifier is a no-op.
    let record = store
        .submit_workflow("async-split", &steps(vec![put_step(&key, "alex", 100)]))
        .expect("resubmit");
    assert_eq!(record.state(), WorkflowState::Completed);
}
