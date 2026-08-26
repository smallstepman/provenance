use jj_lib::object_id::ObjectId;
use provenance_core::{DefaultRules, ProcessError, Service};
use thiserror::Error;

use crate::{
    JjAdapter, JjAdapterError, JjIdentity, JjModel, JjRepository, JjRuntime, JjRuntimeError,
    jj_operation,
};
#[derive(Debug, Error)]
pub enum JjIngestError {
    #[error(transparent)]
    Repository(#[from] crate::JjRepositoryError),
    #[error("provenance ingestion failed: {0}")]
    Process(#[source] ProcessError<JjAdapterError, JjRuntimeError>),
}

/// Hydrates JJ operations that are not already present in the provenance
/// operation store, in root-to-current order.
///
/// Existing source anchors are skipped before adapter work. This makes repeat
/// ingestion proportional to newly observed JJ operations rather than replaying
/// the entire ancestry through the adapter on every invocation.
pub fn ingest_repository(
    service: &mut Service<JjModel, JjIdentity, DefaultRules, JjAdapter, JjRuntime>,
    repository: &JjRepository,
) -> Result<(), JjIngestError> {
    let ancestry = repository
        .operation_ancestry()
        .map_err(JjIngestError::Repository)?;
    for operation in ancestry {
        let source = jj_operation(operation.id().hex());
        if service.state.has_source(&source) {
            continue;
        }
        let snapshot = repository
            .at_operation(operation.id())
            .map_err(JjIngestError::Repository)?;
        service.process(snapshot).map_err(JjIngestError::Process)?;
    }
    Ok(())
}
