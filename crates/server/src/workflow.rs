//! Durable Saga workflows with synchronous, bounded execution.

use std::ops::Bound;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost::Message;
use recolith_backend::{
    BackendError, KeyRange, ScanOptions, Snapshot, StorageBackend, Transaction, TxnOptions,
};
use recolith_common::{CollectionId, FieldId, FragmentId, PartitionId, SchemaId};
use recolith_model::FragmentCardinality;
use recolith_protocol::messages::{
    WorkflowDefinition, WorkflowRecord, WorkflowResolveAction, WorkflowState, WorkflowStep,
    WorkflowStepState, WorkflowStepStatus, workflow_step,
};
use recolith_storage::{Value, crc32c};
use sha2::{Digest, Sha256};

use crate::document::{
    DocumentError, DocumentStore, DocumentWrite, FragmentWriteRequest, Idempotency,
};
use crate::net::convert;

const WORKFLOW_PREFIX: &[u8] = b"\0recolith/workflow/";
const WORKFLOW_STEP_PREFIX: &[u8] = b"\0recolith/workflow-step/";
pub const MAX_WORKFLOW_STEPS: usize = 16;
pub const MAX_WORKFLOW_ID_LEN: usize = 256;

/// Workflow execution failure.
#[derive(Debug)]
pub enum WorkflowError {
    /// The workflow identifier or definition is invalid.
    InvalidWorkflow(String),
    /// A step exceeded its configured timeout.
    StepTimeout {
        /// Zero-based step index.
        index: u32,
        /// Configured timeout.
        timeout: Duration,
    },
    /// Execution, compensation, or persistence failed.
    Execution(DocumentError),
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWorkflow(message) => write!(formatter, "invalid workflow: {message}"),
            Self::StepTimeout { index, timeout } => {
                write!(
                    formatter,
                    "workflow step {index} exceeded its {timeout:?} timeout"
                )
            }
            Self::Execution(error) => write!(formatter, "workflow execution failed: {error}"),
        }
    }
}

impl From<DocumentError> for WorkflowError {
    fn from(error: DocumentError) -> Self {
        Self::Execution(error)
    }
}

impl From<BackendError> for WorkflowError {
    fn from(error: BackendError) -> Self {
        Self::Execution(DocumentError::Storage(
            recolith_storage::StorageError::InvalidValue(error.to_string()),
        ))
    }
}

fn workflow_key(id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(WORKFLOW_PREFIX.len() + 2 + id.len());
    key.extend_from_slice(WORKFLOW_PREFIX);
    key.extend_from_slice(&(id.len() as u16).to_be_bytes());
    key.extend_from_slice(id.as_bytes());
    key
}

fn step_dedup_key(id: &str, index: u32, phase: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(WORKFLOW_STEP_PREFIX.len() + 2 + id.len() + 5);
    key.extend_from_slice(WORKFLOW_STEP_PREFIX);
    key.extend_from_slice(&(id.len() as u16).to_be_bytes());
    key.extend_from_slice(id.as_bytes());
    key.extend_from_slice(&index.to_be_bytes());
    key.push(phase);
    key
}

fn step_hash(step: &WorkflowStep) -> [u8; 32] {
    Sha256::digest(step.encode_to_vec()).into()
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
        })
}

fn compensable(step: &WorkflowStep) -> bool {
    match step.kind.as_ref() {
        Some(workflow_step::Kind::PutDocument(_))
        | Some(workflow_step::Kind::IncrementField(_))
        | Some(workflow_step::Kind::ClaimReservation(_)) => true,
        Some(workflow_step::Kind::DeleteDocument(_))
        | Some(workflow_step::Kind::ConfirmReservation(_)) => false,
        None => false,
    }
}

pub(crate) fn step_target_collection(step: &WorkflowStep) -> Option<CollectionId> {
    match step.kind.as_ref()? {
        workflow_step::Kind::PutDocument(step) => Some(CollectionId::new(step.collection_id)),
        workflow_step::Kind::DeleteDocument(step) => Some(CollectionId::new(step.collection_id)),
        workflow_step::Kind::IncrementField(step) => Some(CollectionId::new(step.collection_id)),
        workflow_step::Kind::ClaimReservation(step) => Some(CollectionId::new(step.collection_id)),
        workflow_step::Kind::ConfirmReservation(step) => {
            Some(CollectionId::new(step.collection_id))
        }
    }
}

fn step_target_partition(step: &WorkflowStep) -> Option<u32> {
    match step.kind.as_ref()? {
        workflow_step::Kind::PutDocument(step) => Some(step.partition),
        workflow_step::Kind::DeleteDocument(step) => Some(step.partition),
        workflow_step::Kind::IncrementField(step) => Some(step.partition),
        workflow_step::Kind::ClaimReservation(step) => Some(step.partition),
        workflow_step::Kind::ConfirmReservation(step) => Some(step.partition),
    }
}

fn value_to_bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::I64(value) => i128::from(*value).to_be_bytes().to_vec(),
        Value::U64(value) => i128::from(*value).to_be_bytes().to_vec(),
        _ => Vec::new(),
    }
}

fn save_workflow<B: StorageBackend>(
    backend: &B,
    record: &WorkflowRecord,
) -> Result<(), DocumentError> {
    let mut payload = record.encode_to_vec();
    let checksum = crc32c(&payload);
    payload.extend_from_slice(&checksum.to_be_bytes());
    let mut transaction = backend.begin(TxnOptions::default())?;
    transaction.put(&workflow_key(&record.id), payload);
    transaction.commit()?;

    Ok(())
}

fn decode_workflow_value(value: &[u8]) -> Option<WorkflowRecord> {
    if value.len() < 4 {
        return None;
    }
    let (body, checksum) = value.split_at(value.len() - 4);
    let expected = u32::from_be_bytes(checksum.try_into().expect("four bytes"));
    if crc32c(body) != expected {
        return None;
    }
    WorkflowRecord::decode(body).ok()
}

fn load_workflow<B: StorageBackend>(
    backend: &B,
    id: &str,
) -> Result<Option<WorkflowRecord>, DocumentError> {
    let Some(payload) = backend.snapshot()?.get(&workflow_key(id))? else {
        return Ok(None);
    };
    let Some(record) = decode_workflow_value(&payload) else {
        return Err(DocumentError::InvalidRequest(
            "malformed workflow record".to_owned(),
        ));
    };

    Ok(Some(record))
}

fn workflow_id_from_key(key: &[u8]) -> Option<String> {
    let rest = key.strip_prefix(WORKFLOW_PREFIX)?;
    if rest.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes(rest[..2].try_into().expect("two bytes")) as usize;
    let id = rest.get(2..2 + len)?;
    String::from_utf8(id.to_vec()).ok()
}

impl<B: StorageBackend> DocumentStore<B> {
    /// Submits a workflow and runs it to a terminal state.
    ///
    /// Resubmitting an existing identifier returns the current state without
    /// re-executing steps.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError`] for invalid definitions or execution failures.
    pub fn start_workflow(
        &self,
        id: &str,
        definition: &WorkflowDefinition,
    ) -> Result<WorkflowRecord, WorkflowError> {
        let mut record = self.submit_workflow(id, definition)?;
        self.advance_workflow(&mut record)?;

        Ok(record)
    }

    /// Validates a workflow definition, persists its record as `Pending`, and
    /// returns without executing any step.
    ///
    /// Resubmitting an existing identifier returns the current state without
    /// re-executing steps.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError`] for invalid definitions or backend failures.
    pub fn submit_workflow(
        &self,
        id: &str,
        definition: &WorkflowDefinition,
    ) -> Result<WorkflowRecord, WorkflowError> {
        if let Some(existing) = self.backend().snapshot()?.get(&workflow_key(id))? {
            return decode_workflow_value(&existing).map_or_else(
                || {
                    Err(WorkflowError::InvalidWorkflow(
                        "existing workflow record is malformed".to_owned(),
                    ))
                },
                Ok,
            );
        }
        let record = self.build_workflow_record(id, definition)?;
        save_workflow(self.backend(), &record)?;

        Ok(record)
    }

    /// Loads a workflow and advances it to a terminal state.
    ///
    /// Used by the asynchronous runner after [`DocumentStore::submit_workflow`].
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError`] when the record is unknown or execution fails.
    pub fn run_workflow(&self, id: &str) -> Result<WorkflowRecord, WorkflowError> {
        let mut record = load_workflow(self.backend(), id)?.ok_or(
            WorkflowError::InvalidWorkflow(format!("workflow {id} does not exist")),
        )?;
        self.advance_workflow(&mut record)?;

        Ok(record)
    }

    /// Returns one workflow record.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] when the record is malformed or the backend fails.
    pub fn workflow_record(&self, id: &str) -> Result<Option<WorkflowRecord>, DocumentError> {
        load_workflow(self.backend(), id)
    }

    /// Cancels a workflow by compensating succeeded steps.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError`] for unknown workflows or compensation failures.
    pub fn cancel_workflow(&self, id: &str) -> Result<WorkflowRecord, WorkflowError> {
        let mut record = load_workflow(self.backend(), id)?.ok_or(
            WorkflowError::InvalidWorkflow(format!("workflow {id} does not exist")),
        )?;
        match record.state() {
            WorkflowState::Pending => {
                record.error = "cancelled".to_owned();
                record.set_state(WorkflowState::Compensated);
                record.updated_at_unix_micros = now_micros();
                save_workflow(self.backend(), &record)?;
            }
            WorkflowState::Running | WorkflowState::Compensating => {
                record.error = "cancelled".to_owned();
                record.set_state(WorkflowState::Compensating);
                record.updated_at_unix_micros = now_micros();
                save_workflow(self.backend(), &record)?;
                self.advance_workflow(&mut record)?;
            }
            WorkflowState::Completed
            | WorkflowState::Compensated
            | WorkflowState::Failed
            | WorkflowState::Abandoned
            | WorkflowState::Unspecified => {}
        }

        Ok(record)
    }

    /// Resolves a failed workflow by retrying from the failed step or
    /// abandoning it.
    ///
    /// Retry is only available when the failure happened during step
    /// execution; workflows that dead-lettered during compensation can only
    /// be abandoned. Abandon compensates anything still compensable.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError`] for unknown workflows, non-failed workflows,
    /// or compensation failures.
    pub fn resolve_workflow(
        &self,
        id: &str,
        action: WorkflowResolveAction,
    ) -> Result<WorkflowRecord, WorkflowError> {
        let mut record = load_workflow(self.backend(), id)?.ok_or(
            WorkflowError::InvalidWorkflow(format!("workflow {id} does not exist")),
        )?;
        if record.state() != WorkflowState::Failed {
            return Err(WorkflowError::InvalidWorkflow(format!(
                "workflow {id} is not in the failed state"
            )));
        }

        match action {
            WorkflowResolveAction::Retry => {
                let index = record.current_step as usize;
                let retryable = record
                    .steps
                    .get(index)
                    .is_some_and(|step| step.status() == WorkflowStepStatus::Failed);
                if !retryable {
                    return Err(WorkflowError::InvalidWorkflow(
                        "workflow has no failed step to retry; use abandon".to_owned(),
                    ));
                }
                record.error = String::new();
                record.steps[index].status = WorkflowStepStatus::Pending as i32;
                record.set_state(WorkflowState::Running);
                record.updated_at_unix_micros = now_micros();
                save_workflow(self.backend(), &record)?;
                self.advance_workflow(&mut record)?;
            }
            WorkflowResolveAction::Abandon => {
                record.error = format!("abandoned: {}", record.error);
                record.set_state(WorkflowState::Compensating);
                record.updated_at_unix_micros = now_micros();
                save_workflow(self.backend(), &record)?;
                self.advance_workflow(&mut record)?;
                record.set_state(WorkflowState::Abandoned);
                record.updated_at_unix_micros = now_micros();
                save_workflow(self.backend(), &record)?;
            }
            WorkflowResolveAction::Unspecified => {
                return Err(WorkflowError::InvalidWorkflow(
                    "resolve action must be retry or abandon".to_owned(),
                ));
            }
        }

        Ok(record)
    }

    /// Lists workflows ordered by identifier.
    ///
    /// # Errors
    ///
    /// Returns [`DocumentError`] when the backend fails.
    pub fn list_workflows(
        &self,
        start_after: &str,
        limit: usize,
    ) -> Result<Vec<WorkflowRecord>, DocumentError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let snapshot = self.backend().snapshot()?;
        let end = KeyRange::prefix(WORKFLOW_PREFIX).end_bound().clone();
        let start = if start_after.is_empty() {
            Bound::Included(WORKFLOW_PREFIX.to_vec())
        } else {
            Bound::Excluded(workflow_key(start_after))
        };
        let entries = snapshot.scan(
            &KeyRange::new(start, end),
            ScanOptions {
                limit: Some(limit),
                reverse: false,
            },
        )?;

        Ok(entries
            .into_iter()
            .filter_map(|(key, value)| {
                let id = workflow_id_from_key(&key)?;
                Some((id, decode_workflow_value(&value)?))
            })
            .map(|(_, record)| record)
            .collect())
    }

    /// Resumes interrupted workflows in bounded batches.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError`] when scanning or resuming fails.
    pub fn resume_workflows(&self, limit: usize) -> Result<u32, WorkflowError> {
        let snapshot = self.backend().snapshot()?;
        let entries = snapshot.scan(
            &KeyRange::prefix(WORKFLOW_PREFIX),
            ScanOptions {
                limit: Some(limit),
                reverse: false,
            },
        )?;
        let mut resumed = 0_u32;
        for (key, value) in entries {
            let Some(id) = workflow_id_from_key(&key) else {
                continue;
            };
            let mut record = decode_workflow_value(&value).ok_or_else(|| {
                WorkflowError::InvalidWorkflow(format!("workflow {id} record is malformed"))
            })?;
            if !matches!(
                record.state(),
                WorkflowState::Pending | WorkflowState::Running | WorkflowState::Compensating
            ) {
                continue;
            }
            self.advance_workflow(&mut record)?;
            resumed = resumed.saturating_add(1);
        }

        Ok(resumed)
    }

    fn build_workflow_record(
        &self,
        id: &str,
        definition: &WorkflowDefinition,
    ) -> Result<WorkflowRecord, WorkflowError> {
        if id.is_empty() || id.len() > MAX_WORKFLOW_ID_LEN {
            return Err(WorkflowError::InvalidWorkflow(
                "workflow id length is invalid".to_owned(),
            ));
        }
        let steps = definition.steps.clone();
        if steps.is_empty() || steps.len() > MAX_WORKFLOW_STEPS {
            return Err(WorkflowError::InvalidWorkflow(format!(
                "workflow must contain 1 to {MAX_WORKFLOW_STEPS} steps"
            )));
        }
        self.validate_steps(id, &steps)?;
        let now = now_micros();

        Ok(WorkflowRecord {
            id: id.to_owned(),
            state: WorkflowState::Pending as i32,
            current_step: 0,
            steps: steps
                .into_iter()
                .map(|step| WorkflowStepState {
                    step: Some(step),
                    status: WorkflowStepStatus::Pending as i32,
                    before_value: None,
                    after_value: None,
                })
                .collect(),
            error: String::new(),
            created_at_unix_micros: now,
            updated_at_unix_micros: now,
        })
    }

    fn validate_steps(&self, id: &str, steps: &[WorkflowStep]) -> Result<(), WorkflowError> {
        let snapshot = self.catalog_snapshot()?;
        for (index, step) in steps.iter().enumerate() {
            let Some(collection_id) = step_target_collection(step) else {
                return Err(WorkflowError::InvalidWorkflow(format!(
                    "workflow {id} step {index} has no operation"
                )));
            };
            let partition = step_target_partition(step).unwrap_or_default();
            let collection = snapshot
                .collections
                .get(&collection_id)
                .ok_or(DocumentError::UnknownCollection(collection_id))?;

            match step.kind.as_ref().expect("kind is present") {
                workflow_step::Kind::PutDocument(step) => {
                    if step.document_key.is_empty()
                        || step.document_key.len() > recolith_storage::MAX_DOCUMENT_KEY_LEN
                    {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} has an invalid document key"
                        )));
                    }
                    if step.fragments.is_empty() {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} must write a fragment"
                        )));
                    }
                    for fragment in &step.fragments {
                        let target = collection
                            .fragments()
                            .iter()
                            .find(|candidate| candidate.id().get() == fragment.fragment_id)
                            .ok_or(DocumentError::UnknownFragment(FragmentId::new(
                                fragment.fragment_id,
                            )))?;
                        let bucket = snapshot
                            .buckets
                            .get(&target.bucket_id())
                            .ok_or(DocumentError::MissingMetadata("bucket"))?;
                        if partition >= bucket.partition_count() {
                            return Err(WorkflowError::InvalidWorkflow(format!(
                                "workflow {id} step {index} partition is out of range"
                            )));
                        }
                        match target.cardinality() {
                            FragmentCardinality::One if !fragment.subkey.is_empty() => {
                                return Err(WorkflowError::InvalidWorkflow(format!(
                                    "workflow {id} step {index} one fragment requires an empty subkey"
                                )));
                            }
                            FragmentCardinality::Many if fragment.subkey.is_empty() => {
                                return Err(WorkflowError::InvalidWorkflow(format!(
                                    "workflow {id} step {index} many fragment requires a subkey"
                                )));
                            }
                            _ => {}
                        }
                    }
                }
                workflow_step::Kind::DeleteDocument(step) => {
                    if step.document_key.is_empty()
                        || step.document_key.len() > recolith_storage::MAX_DOCUMENT_KEY_LEN
                    {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} has an invalid document key"
                        )));
                    }
                    for fragment in collection.fragments() {
                        let bucket = snapshot
                            .buckets
                            .get(&fragment.bucket_id())
                            .ok_or(DocumentError::MissingMetadata("bucket"))?;
                        if partition >= bucket.partition_count() {
                            return Err(WorkflowError::InvalidWorkflow(format!(
                                "workflow {id} step {index} partition is out of range"
                            )));
                        }
                    }
                }
                workflow_step::Kind::IncrementField(step) => {
                    if step.delta == 0 {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} delta must not be zero"
                        )));
                    }
                    if step.document_key.is_empty()
                        || step.document_key.len() > recolith_storage::MAX_DOCUMENT_KEY_LEN
                    {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} has an invalid document key"
                        )));
                    }
                    let fragment = collection
                        .fragments()
                        .iter()
                        .find(|candidate| candidate.id().get() == step.fragment_id)
                        .ok_or(DocumentError::UnknownFragment(FragmentId::new(
                            step.fragment_id,
                        )))?;
                    if fragment.cardinality() != FragmentCardinality::One {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} requires a one fragment"
                        )));
                    }
                    let bucket = snapshot
                        .buckets
                        .get(&fragment.bucket_id())
                        .ok_or(DocumentError::MissingMetadata("bucket"))?;
                    if partition >= bucket.partition_count() {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} partition is out of range"
                        )));
                    }
                    let schema = snapshot
                        .schemas
                        .schemas
                        .get(&(bucket.schema_id(), bucket.schema_version()))
                        .ok_or(DocumentError::MissingMetadata("schema"))?;
                    let field = schema
                        .field(FieldId::new(step.field_id))
                        .ok_or(DocumentError::MissingMetadata("field"))?;
                    if !matches!(
                        field.ty(),
                        recolith_schema::Type::I64 | recolith_schema::Type::U64
                    ) {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} requires an i64 or u64 counter field"
                        )));
                    }
                }
                workflow_step::Kind::ClaimReservation(step_claim) => {
                    Self::validate_reservation_step(
                        id,
                        index,
                        &snapshot,
                        step_claim.schema_id,
                        step_claim.field_id,
                        step_claim.value.as_ref(),
                        &step_claim.document_key,
                    )?;
                    if step_claim.ttl_seconds == 0 {
                        return Err(WorkflowError::InvalidWorkflow(format!(
                            "workflow {id} step {index} claim requires a positive TTL"
                        )));
                    }
                }
                workflow_step::Kind::ConfirmReservation(step_confirm) => {
                    Self::validate_reservation_step(
                        id,
                        index,
                        &snapshot,
                        step_confirm.schema_id,
                        step_confirm.field_id,
                        step_confirm.value.as_ref(),
                        &step_confirm.document_key,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn validate_reservation_step(
        id: &str,
        index: usize,
        snapshot: &recolith_catalog::CatalogSnapshot,
        schema_id: u64,
        field_id: u32,
        value: Option<&recolith_protocol::messages::Value>,
        document_key: &[u8],
    ) -> Result<(), WorkflowError> {
        if document_key.is_empty() || document_key.len() > recolith_storage::MAX_DOCUMENT_KEY_LEN {
            return Err(WorkflowError::InvalidWorkflow(format!(
                "workflow {id} step {index} has an invalid document key"
            )));
        }
        let Some(value) = value else {
            return Err(WorkflowError::InvalidWorkflow(format!(
                "workflow {id} step {index} has no reservation value"
            )));
        };
        let normalized = convert::value_from_wire(value)
            .map_err(|error| WorkflowError::InvalidWorkflow(error.to_string()))
            .and_then(|storage_value| {
                crate::document::normalized_unique_value(&storage_value)
                    .map_err(WorkflowError::Execution)
            })?;
        if normalized.is_empty() {
            return Err(WorkflowError::InvalidWorkflow(format!(
                "workflow {id} step {index} reserves an empty value"
            )));
        }
        let unique = snapshot
            .schemas
            .schemas
            .iter()
            .any(|((schema, _), record)| {
                schema.get() == schema_id
                    && record
                        .field(FieldId::new(field_id))
                        .is_some_and(|field| field.unique())
            });
        if !unique {
            return Err(WorkflowError::InvalidWorkflow(format!(
                "workflow {id} step {index} field is not globally unique"
            )));
        }

        Ok(())
    }

    fn advance_workflow(&self, record: &mut WorkflowRecord) -> Result<(), WorkflowError> {
        match record.state() {
            WorkflowState::Pending => {
                record.set_state(WorkflowState::Running);
                record.updated_at_unix_micros = now_micros();
                save_workflow(self.backend(), record)?;
            }
            WorkflowState::Completed
            | WorkflowState::Compensated
            | WorkflowState::Failed
            | WorkflowState::Abandoned
            | WorkflowState::Unspecified => return Ok(()),
            WorkflowState::Running | WorkflowState::Compensating => {}
        }

        if record.state() == WorkflowState::Running {
            let mut failure: Option<String> = None;
            while record.current_step as usize <= record.steps.len() {
                if record.current_step as usize == record.steps.len() {
                    record.set_state(WorkflowState::Completed);
                    record.updated_at_unix_micros = now_micros();
                    save_workflow(self.backend(), record)?;
                    return Ok(());
                }
                let index = record.current_step as usize;
                let succeeded = record.steps[index].status() == WorkflowStepStatus::Succeeded;
                if succeeded {
                    record.current_step += 1;
                    continue;
                }
                // Another writer (cancellation, resolution) may have changed
                // the persisted state; yield to it instead of racing.
                if let Some(persisted) = self.workflow_record(&record.id)?
                    && persisted.state() != WorkflowState::Running
                {
                    *record = persisted;
                    return Ok(());
                }
                let Some(step) = record.steps[index].step.clone() else {
                    return Err(WorkflowError::InvalidWorkflow(
                        "workflow step is missing".to_owned(),
                    ));
                };
                let started = Instant::now();
                match self.execute_workflow_step(record.id.as_str(), record.current_step, &step) {
                    Ok(step_state) => {
                        record.steps[index] = step_state;
                        if self
                            .workflow_step_timeout()
                            .is_some_and(|timeout| started.elapsed() > timeout)
                        {
                            // The step applied its effect but ran past the
                            // budget: mark it failed, undo it when possible,
                            // and take the normal failure path.
                            let timeout = self.workflow_step_timeout().expect("checked above");
                            record.steps[index].status = WorkflowStepStatus::Failed as i32;
                            record.current_step = index as u32;
                            if compensable(&step) {
                                match self.compensate_workflow_step(
                                    record.id.as_str(),
                                    index as u32,
                                    &step,
                                ) {
                                    Ok(()) => {
                                        record.steps[index].status =
                                            WorkflowStepStatus::Compensated as i32;
                                    }
                                    Err(error) => {
                                        failure = Some(format!(
                                            "step {index} timeout compensation failed: {error}"
                                        ));
                                    }
                                }
                            }
                            if failure.is_none() {
                                failure = Some(
                                    WorkflowError::StepTimeout {
                                        index: index as u32,
                                        timeout,
                                    }
                                    .to_string(),
                                );
                            }
                            break;
                        }
                        record.steps[index].status = WorkflowStepStatus::Succeeded as i32;
                        record.current_step += 1;
                        record.updated_at_unix_micros = now_micros();
                        save_workflow(self.backend(), record)?;
                    }
                    Err(error) => {
                        record.steps[index].status = WorkflowStepStatus::Failed as i32;
                        record.current_step = index as u32;
                        failure = Some(error.to_string());
                        break;
                    }
                }
            }

            if let Some(error) = failure {
                let non_compensable = record.steps.iter().any(|step_state| {
                    step_state.status() == WorkflowStepStatus::Succeeded
                        && step_state
                            .step
                            .as_ref()
                            .is_some_and(|step| !compensable(step))
                });
                if non_compensable {
                    record.set_state(WorkflowState::Failed);
                    record.error = error;
                    record.updated_at_unix_micros = now_micros();
                    save_workflow(self.backend(), record)?;
                    return Ok(());
                }
                record.set_state(WorkflowState::Compensating);
                record.error = error;
                record.updated_at_unix_micros = now_micros();
                save_workflow(self.backend(), record)?;
            }
        }

        if record.state() == WorkflowState::Compensating {
            for index in (0..record.steps.len()).rev() {
                let succeeded = record.steps[index].status() == WorkflowStepStatus::Succeeded;
                if !succeeded {
                    continue;
                }
                let Some(step) = record.steps[index].step.clone() else {
                    continue;
                };
                if !compensable(&step) {
                    continue;
                }
                let started = Instant::now();
                let outcome =
                    self.compensate_workflow_step(record.id.as_str(), index as u32, &step);
                if outcome.is_ok()
                    && self
                        .workflow_step_timeout()
                        .is_some_and(|timeout| started.elapsed() > timeout)
                {
                    record.set_state(WorkflowState::Failed);
                    record.error = format!(
                        "compensation step {index} exceeded its {:?} timeout",
                        self.workflow_step_timeout().expect("checked above")
                    );
                    record.updated_at_unix_micros = now_micros();
                    save_workflow(self.backend(), record)?;
                    return Ok(());
                }
                match outcome {
                    Ok(()) => {
                        record.steps[index].status = WorkflowStepStatus::Compensated as i32;
                        record.updated_at_unix_micros = now_micros();
                        save_workflow(self.backend(), record)?;
                    }
                    Err(error) => {
                        record.set_state(WorkflowState::Failed);
                        record.error = format!("compensation failed: {error}");
                        record.updated_at_unix_micros = now_micros();
                        save_workflow(self.backend(), record)?;
                        return Ok(());
                    }
                }
            }
            record.set_state(WorkflowState::Compensated);
            record.updated_at_unix_micros = now_micros();
            save_workflow(self.backend(), record)?;
        }

        Ok(())
    }

    fn execute_workflow_step(
        &self,
        id: &str,
        index: u32,
        step: &WorkflowStep,
    ) -> Result<WorkflowStepState, WorkflowError> {
        let dedup_key = step_dedup_key(id, index, 0);
        let hash = step_hash(step);
        match step.kind.as_ref().expect("kind is present") {
            workflow_step::Kind::PutDocument(step_put) => {
                let mut write = DocumentWrite::new(
                    CollectionId::new(step_put.collection_id),
                    step_put.document_key.clone(),
                    PartitionId::new(step_put.partition),
                );
                for fragment in &step_put.fragments {
                    let data = convert::record_from_wire(&fragment.fields)
                        .map_err(|error| WorkflowError::InvalidWorkflow(error.to_string()))?;
                    write = write.with_fragment(FragmentWriteRequest::new(
                        FragmentId::new(fragment.fragment_id),
                        fragment.subkey.clone(),
                        data,
                    ));
                }
                write = write.with_ttl_seconds(step_put.ttl_seconds);
                write = write.with_idempotency(Idempotency::new(dedup_key, hash));
                self.write(&write)?;

                Ok(self.succeeded_step(step))
            }
            workflow_step::Kind::DeleteDocument(step_delete) => {
                self.delete_document(
                    CollectionId::new(step_delete.collection_id),
                    PartitionId::new(step_delete.partition),
                    &step_delete.document_key,
                )?;

                Ok(self.succeeded_step(step))
            }
            workflow_step::Kind::IncrementField(step_increment) => {
                let change = self.increment_field(
                    CollectionId::new(step_increment.collection_id),
                    PartitionId::new(step_increment.partition),
                    &step_increment.document_key,
                    FragmentId::new(step_increment.fragment_id),
                    FieldId::new(step_increment.field_id),
                    step_increment.delta,
                    Some((dedup_key.as_slice(), hash)),
                )?;
                let mut state = self.succeeded_step(step);
                state.before_value = Some(value_to_bytes(&change.before));
                state.after_value = Some(value_to_bytes(&change.after));

                Ok(state)
            }
            workflow_step::Kind::ClaimReservation(step_claim) => {
                let value = convert::value_from_wire(step_claim.value.as_ref().ok_or(
                    WorkflowError::InvalidWorkflow("reservation value is missing".to_owned()),
                )?)
                .map_err(|error| WorkflowError::InvalidWorkflow(error.to_string()))?;
                self.claim_reservation(
                    SchemaId::new(step_claim.schema_id),
                    FieldId::new(step_claim.field_id),
                    &value,
                    CollectionId::new(step_claim.collection_id),
                    PartitionId::new(step_claim.partition),
                    &step_claim.document_key,
                    step_claim.ttl_seconds,
                    Some((dedup_key.as_slice(), hash)),
                )?;

                Ok(self.succeeded_step(step))
            }
            workflow_step::Kind::ConfirmReservation(step_confirm) => {
                let value = convert::value_from_wire(step_confirm.value.as_ref().ok_or(
                    WorkflowError::InvalidWorkflow("reservation value is missing".to_owned()),
                )?)
                .map_err(|error| WorkflowError::InvalidWorkflow(error.to_string()))?;
                self.confirm_reservation(
                    SchemaId::new(step_confirm.schema_id),
                    FieldId::new(step_confirm.field_id),
                    &value,
                    CollectionId::new(step_confirm.collection_id),
                    PartitionId::new(step_confirm.partition),
                    &step_confirm.document_key,
                    Some((dedup_key.as_slice(), hash)),
                )?;

                Ok(self.succeeded_step(step))
            }
        }
    }

    fn succeeded_step(&self, step: &WorkflowStep) -> WorkflowStepState {
        WorkflowStepState {
            step: Some(step.clone()),
            status: WorkflowStepStatus::Succeeded as i32,
            before_value: None,
            after_value: None,
        }
    }

    fn compensate_workflow_step(
        &self,
        id: &str,
        index: u32,
        step: &WorkflowStep,
    ) -> Result<(), WorkflowError> {
        match step.kind.as_ref().expect("kind is present") {
            workflow_step::Kind::PutDocument(step) => {
                self.delete_document(
                    CollectionId::new(step.collection_id),
                    PartitionId::new(step.partition),
                    &step.document_key,
                )?;

                Ok(())
            }
            workflow_step::Kind::IncrementField(step_increment) => {
                let dedup_key = step_dedup_key(id, index, 1);
                let reverse_delta = step_increment.delta.checked_neg().ok_or_else(|| {
                    WorkflowError::InvalidWorkflow("counter delta cannot be reversed".to_owned())
                })?;
                self.increment_field(
                    CollectionId::new(step_increment.collection_id),
                    PartitionId::new(step_increment.partition),
                    &step_increment.document_key,
                    FragmentId::new(step_increment.fragment_id),
                    FieldId::new(step_increment.field_id),
                    reverse_delta,
                    Some((dedup_key.as_slice(), step_hash(step))),
                )?;

                Ok(())
            }
            workflow_step::Kind::ClaimReservation(step_claim) => {
                let value = convert::value_from_wire(step_claim.value.as_ref().ok_or(
                    WorkflowError::InvalidWorkflow("reservation value is missing".to_owned()),
                )?)
                .map_err(|error| WorkflowError::InvalidWorkflow(error.to_string()))?;
                self.release_reservation(
                    SchemaId::new(step_claim.schema_id),
                    FieldId::new(step_claim.field_id),
                    &value,
                    CollectionId::new(step_claim.collection_id),
                    PartitionId::new(step_claim.partition),
                    &step_claim.document_key,
                )?;

                Ok(())
            }
            workflow_step::Kind::ConfirmReservation(_) => Ok(()),
            workflow_step::Kind::DeleteDocument(_) => Ok(()),
        }
    }
}
