use provenance_core::{DefaultRules, ProcessError, Service};

use crate::{
    JjAdapter, JjAdapterError, JjIdentity, JjModel, JjRepository, JjRuntime, JjRuntimeError,
};

#[derive(Debug)]
pub enum JjIngestError {
    Repository(crate::JjRepositoryError),
    Process(ProcessError<JjAdapterError, JjRuntimeError>),
}

/// Hydrates all JJ operations from the repository root to its current operation.
///
/// `observe_repository` remains a single-operation transaction constructor. This
/// helper supplies the source-parent transactions needed when a provenance state
/// is empty or has fallen behind the JJ operation log.
pub fn ingest_repository(
    service: &mut Service<JjModel, JjIdentity, DefaultRules, JjAdapter, JjRuntime>,
    repository: &JjRepository,
) -> Result<(), JjIngestError> {
    let ancestry = repository
        .operation_ancestry()
        .map_err(JjIngestError::Repository)?;
    for operation in ancestry {
        let snapshot = repository
            .at_operation(operation.id())
            .map_err(JjIngestError::Repository)?;
        service.process(snapshot).map_err(JjIngestError::Process)?;
    }
    Ok(())
}
