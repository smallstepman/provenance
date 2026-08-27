use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use jj_lib::repo::ReadonlyRepo;
use provenance_core::{
    FinalizeAction, Operation, OperationId, PrepareRequirement, ProvenanceStore, Runtime, State,
};
use provenance_storage::{DirectoryProvenanceStore, MemoryProvenanceStore, StorageError};
use thiserror::Error;

use crate::model::JjModel;
use crate::retention::{JjRetention, JjRetentionError};

#[derive(Debug, Error)]
pub enum JjRuntimeError {
    #[error(transparent)]
    Retention(#[from] JjRetentionError),
    #[error(transparent)]
    Store(#[from] StorageError),
    #[error(transparent)]
    Core(#[from] provenance_core::Error),
    #[error("provenance object materialization is not configured for provenance-jj")]
    ObjectMaterializationUnsupported,
    #[error("published provenance operation identity collision")]
    PublishCollision,
}

enum JjStore {
    Memory(MemoryProvenanceStore<JjModel>),
    Directory(DirectoryProvenanceStore<JjModel>),
}

impl JjStore {
    fn is_durable(&self) -> bool {
        matches!(self, Self::Directory(_))
    }

    fn reachable_operations(&self) -> Result<Vec<Operation<JjModel>>, StorageError> {
        match self {
            Self::Memory(store) => store.reachable_operations(),
            Self::Directory(store) => store.reachable_operations(),
        }
    }

    fn reachable_operation_count(&self) -> Result<usize, StorageError> {
        match self {
            Self::Memory(store) => store.reachable_operation_count(),
            Self::Directory(store) => store.reachable_operation_count(),
        }
    }
}

impl ProvenanceStore<JjModel> for JjStore {
    type Error = StorageError;

    fn get_operation(
        &self,
        id: &OperationId<JjModel>,
    ) -> Result<Option<Operation<JjModel>>, Self::Error> {
        match self {
            Self::Memory(store) => store.get_operation(id),
            Self::Directory(store) => store.get_operation(id),
        }
    }

    fn has_operation(&self, id: &OperationId<JjModel>) -> Result<bool, Self::Error> {
        match self {
            Self::Memory(store) => store.has_operation(id),
            Self::Directory(store) => store.has_operation(id),
        }
    }

    fn put_operation(&mut self, operation: &Operation<JjModel>) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store.put_operation(operation),
            Self::Directory(store) => store.put_operation(operation),
        }
    }

    fn get_object(
        &self,
        id: &provenance_core::ObjectId<JjModel>,
    ) -> Result<Option<provenance_core::Object<JjModel>>, Self::Error> {
        match self {
            Self::Memory(store) => store.get_object(id),
            Self::Directory(store) => store.get_object(id),
        }
    }

    fn put_object(&mut self, object: &provenance_core::Object<JjModel>) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store.put_object(object),
            Self::Directory(store) => store.put_object(object),
        }
    }

    fn heads(&self) -> Result<BTreeSet<OperationId<JjModel>>, Self::Error> {
        match self {
            Self::Memory(store) => store.heads(),
            Self::Directory(store) => store.heads(),
        }
    }

    fn publish_heads(
        &mut self,
        expected: &BTreeSet<OperationId<JjModel>>,
        next: &BTreeSet<OperationId<JjModel>>,
    ) -> Result<provenance_core::PublishOutcome, Self::Error> {
        match self {
            Self::Memory(store) => store.publish_heads(expected, next),
            Self::Directory(store) => store.publish_heads(expected, next),
        }
    }
}

/// Runtime boundary for JJ-backed retention and authoritative operation storage.
/// The durable backend stores immutable operation/object records and an
/// atomically replaced head manifest. Retention is reconciled from the
/// authoritative kernel projection after restart.
pub struct JjRuntime {
    retention: JjRetention,
    published: BTreeMap<OperationId<JjModel>, Operation<JjModel>>,
    store: JjStore,
}

impl std::fmt::Debug for JjRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JjRuntime")
            .field("published_operations", &self.published.len())
            .field("durable", &self.store.is_durable())
            .finish()
    }
}

impl JjRuntime {
    pub fn new(repo: Arc<ReadonlyRepo>) -> Self {
        Self {
            retention: JjRetention::new(repo),
            published: BTreeMap::new(),
            store: JjStore::Memory(MemoryProvenanceStore::new()),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    pub fn open(repo: Arc<ReadonlyRepo>, path: impl AsRef<Path>) -> Result<Self, JjRuntimeError> {
        let mut runtime = Self {
            retention: JjRetention::new(repo),
            published: BTreeMap::new(),
            store: JjStore::Directory(DirectoryProvenanceStore::open(path)?),
        };
        runtime.reload_cache()?;
        runtime.reconcile()?;
        Ok(runtime)
    }

    pub fn retention(&self) -> &JjRetention {
        &self.retention
    }

    pub fn heads(&self) -> Result<BTreeSet<OperationId<JjModel>>, JjRuntimeError> {
        Ok(self.store.heads()?)
    }

    pub fn load_state(&self) -> Result<State<JjModel>, JjRuntimeError> {
        let mut state = State::new();
        for operation in self.store.reachable_operations()? {
            state.operations.declare(operation.id.clone(), operation)?;
        }
        state.rebuild()?;
        Ok(state)
    }

    pub fn published_operation(&self, id: &OperationId<JjModel>) -> Option<&Operation<JjModel>> {
        self.published.get(id)
    }

    pub fn published_len(&self) -> Result<usize, JjRuntimeError> {
        Ok(self.store.reachable_operation_count()?)
    }

    /// Reconcile physical retention from authoritative provenance state.
    #[tracing::instrument(level = "debug", skip_all, err)]
    pub fn reconcile(&mut self) -> Result<(), JjRuntimeError> {
        let state = self.load_state()?;
        let requirements = state
            .projection
            .required_retention
            .iter()
            .map(|(resource, strength)| (resource.clone(), *strength));
        self.retention.reconcile(requirements)?;
        Ok(())
    }

    fn reload_cache(&mut self) -> Result<(), JjRuntimeError> {
        self.published.clear();
        for operation in self.store.reachable_operations()? {
            self.published.insert(operation.id.clone(), operation);
        }
        Ok(())
    }

    fn publish_operation(&mut self, operation: &Operation<JjModel>) -> Result<(), JjRuntimeError> {
        if let Some(existing) = self.store.get_operation(&operation.id)?
            && existing != *operation
        {
            return Err(JjRuntimeError::PublishCollision);
        }

        self.store.put_operation(operation)?;
        let expected = self.store.heads()?;
        let mut next = expected.clone();
        for parent in &operation.parents {
            next.remove(parent);
        }
        next.insert(operation.id.clone());
        self.store.publish_heads(&expected, &next)?;
        self.published
            .insert(operation.id.clone(), operation.clone());
        Ok(())
    }

    fn finalize_action(&mut self, action: &FinalizeAction<JjModel>) -> Result<(), JjRuntimeError> {
        match action {
            FinalizeAction::Retention(transition) => {
                self.retention.finalize(transition)?;
            }
        }
        Ok(())
    }
}

impl Runtime<JjModel> for JjRuntime {
    type Error = JjRuntimeError;

    fn prepare(&mut self, requirement: &PrepareRequirement<JjModel>) -> Result<(), Self::Error> {
        match requirement {
            PrepareRequirement::MaterializeObject(_) => {
                Err(JjRuntimeError::ObjectMaterializationUnsupported)
            }
            PrepareRequirement::Retention(transition) => {
                self.retention.prepare(transition).map_err(Into::into)
            }
        }
    }

    fn publish(&mut self, operation: &Operation<JjModel>) -> Result<(), Self::Error> {
        self.publish_operation(operation)
    }

    fn finalize(&mut self, action: &FinalizeAction<JjModel>) -> Result<(), Self::Error> {
        self.finalize_action(action)
    }
}
