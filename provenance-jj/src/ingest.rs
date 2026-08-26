use std::collections::BTreeSet;

use jj_lib::object_id::ObjectId;
use provenance_core::{DefaultRules, Intent, ProcessError, Service, Transaction};
use thiserror::Error;

use crate::{
    COMMIT_KIND, JJ_NAMESPACE, JjAdapter, JjAdapterError, JjIdentity, JjModel, JjRepository,
    JjRuntime, JjRuntimeError, jj_commit_address, jj_operation, observe_operation_delta,
};

#[derive(Debug, Error)]
pub enum JjIngestError {
    #[error(transparent)]
    Repository(#[from] crate::JjRepositoryError),
    #[error("provenance ingestion failed: {0:?}")]
    Process(ProcessError<JjAdapterError, JjRuntimeError>),
}

/// Hydrates JJ operations that are not already present in the provenance
/// operation store, in root-to-current order.
///
/// Existing source anchors are skipped before adapter work. Each new
/// operation is observed as a delta: known commit ancestry is not reloaded
/// into the transaction, while the transaction remains a normal generic
/// provenance transaction processed by the kernel and runtime.
pub fn ingest_repository(
    service: &mut Service<JjModel, JjIdentity, DefaultRules, JjAdapter, JjRuntime>,
    repository: &JjRepository,
) -> Result<(), JjIngestError> {
    let ancestry = repository
        .operation_ancestry()
        .map_err(JjIngestError::Repository)?;
    let mut known_commits = observed_commit_ids(service);
    for operation in ancestry {
        let source = jj_operation(operation.id().hex());
        if service.state.has_source(&source) {
            continue;
        }
        let snapshot = repository
            .at_operation(operation.id())
            .map_err(JjIngestError::Repository)?;
        let transaction = observe_operation_delta(&snapshot, &known_commits)
            .map_err(|error| JjIngestError::Process(ProcessError::Adapter(error)))?;
        extend_observed_commit_ids(&mut known_commits, &transaction);
        service
            .process_transaction(transaction)
            .map_err(JjIngestError::Process)?;
    }
    Ok(())
}
fn extend_observed_commit_ids(
    known_commits: &mut BTreeSet<String>,
    transaction: &Transaction<JjModel>,
) {
    let commit_type = jj_commit_address(String::new()).entity_type();
    for intent in &transaction.intents {
        let Intent::ObserveEntity(observation) = intent else {
            continue;
        };
        let address = &observation.entity;
        if address.namespace == JJ_NAMESPACE.into()
            && address.kind == COMMIT_KIND.into()
            && address.entity_type() == commit_type
        {
            known_commits.insert(address.id.clone());
        }
    }
}

fn observed_commit_ids(
    service: &Service<JjModel, JjIdentity, DefaultRules, JjAdapter, JjRuntime>,
) -> BTreeSet<String> {
    let commit_type = jj_commit_address(String::new()).entity_type();
    service
        .state
        .projection
        .entities
        .iter()
        .filter(|(address, _)| {
            address.namespace == JJ_NAMESPACE.into()
                && address.kind == COMMIT_KIND.into()
                && address.entity_type() == commit_type
        })
        .map(|(address, _)| address.id.clone())
        .collect()
}
