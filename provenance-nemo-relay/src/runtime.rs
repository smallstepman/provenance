use std::collections::BTreeSet;
use std::path::Path;

use provenance_core::{
    FinalizeAction, Operation, OperationId, PluginModel, PrepareRequirement, ProvenanceStore,
    PublishOutcome, Runtime, State,
};
use provenance_storage::{DirectoryProvenanceStore, StorageError};
use thiserror::Error;

/// Errors raised by the directory-backed Relay provenance runtime.
#[derive(Debug, Error)]
pub enum DirectoryRuntimeError {
    /// Authoritative operation/object storage failed.
    #[error(transparent)]
    Store(#[from] StorageError),
    /// Rebuilding the in-memory kernel projection failed.
    #[error(transparent)]
    Core(#[from] provenance_core::Error),
    /// Retention claims are intentionally not materialized by this generic
    /// runtime; a backend-specific runtime can add that policy later.
    #[error("agent Relay runtime does not implement physical retention")]
    RetentionUnsupported,
    /// A different immutable operation already occupies this identity.
    #[error("published provenance operation identity collision")]
    PublishCollision,
}

/// Filesystem runtime for the harness-neutral agent provenance graph.
///
/// Operations and object bytes are authoritative in the existing
/// `provenance-storage` directory format. The agent projection currently emits
/// no object or retention intents, but the runtime handles object preparation
/// and rejects retention explicitly rather than silently dropping policy.
pub struct DirectoryRuntime {
    store: DirectoryProvenanceStore<PluginModel>,
}

impl std::fmt::Debug for DirectoryRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectoryRuntime")
            .field("root", &self.store.root())
            .finish()
    }
}

impl DirectoryRuntime {
    /// Open or create the authoritative provenance directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DirectoryRuntimeError> {
        Ok(Self {
            store: DirectoryProvenanceStore::open(path)?,
        })
    }

    /// Rebuild kernel state from all operations reachable from persisted heads.
    pub fn load_state(&self) -> Result<State<PluginModel>, DirectoryRuntimeError> {
        let mut state = State::new();
        for operation in self.store.reachable_operations()? {
            state.operations.declare(operation.id.clone(), operation)?;
        }
        state.rebuild()?;
        Ok(state)
    }

    /// Return the durable store root.
    pub fn root(&self) -> &Path {
        self.store.root()
    }

    fn publish_operation(
        &mut self,
        operation: &Operation<PluginModel>,
    ) -> Result<(), DirectoryRuntimeError> {
        if let Some(existing) = self.store.get_operation(&operation.id)?
            && existing != *operation
        {
            return Err(DirectoryRuntimeError::PublishCollision);
        }

        self.store.put_operation(operation)?;
        let expected = self.store.heads()?;
        let mut next = expected.clone();
        for parent in &operation.parents {
            next.remove(parent);
        }
        next.insert(operation.id.clone());
        let _outcome: PublishOutcome = self.store.publish_heads(&expected, &next)?;
        Ok(())
    }
}

impl Runtime<PluginModel> for DirectoryRuntime {
    type Error = DirectoryRuntimeError;

    fn prepare(
        &mut self,
        requirement: &PrepareRequirement<PluginModel>,
    ) -> Result<(), Self::Error> {
        match requirement {
            PrepareRequirement::MaterializeObject(object) => {
                self.store.put_object(object)?;
                Ok(())
            }
            PrepareRequirement::Retention(_) => Err(DirectoryRuntimeError::RetentionUnsupported),
        }
    }

    fn publish(&mut self, operation: &Operation<PluginModel>) -> Result<(), Self::Error> {
        self.publish_operation(operation)
    }

    fn finalize(&mut self, action: &FinalizeAction<PluginModel>) -> Result<(), Self::Error> {
        match action {
            FinalizeAction::Retention(_) => Err(DirectoryRuntimeError::RetentionUnsupported),
        }
    }
}

/// Read the current durable head set, primarily for diagnostics and tests.
pub fn durable_heads(
    path: impl AsRef<Path>,
) -> Result<BTreeSet<OperationId<PluginModel>>, DirectoryRuntimeError> {
    let store = DirectoryProvenanceStore::<PluginModel>::open(path)?;
    Ok(store.heads()?)
}
