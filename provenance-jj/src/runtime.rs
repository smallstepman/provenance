use std::collections::BTreeMap;
use std::sync::Arc;

use jj_lib::repo::ReadonlyRepo;
use provenance_core::{FinalizeAction, Operation, OperationId, PrepareRequirement, Runtime};
use thiserror::Error;

use crate::model::JjModel;
use crate::retention::{JjRetention, JjRetentionError};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JjRuntimeError {
    #[error(transparent)]
    Retention(#[from] JjRetentionError),
    #[error("provenance object materialization is not configured for provenance-jj")]
    ObjectMaterializationUnsupported,
    #[error("published provenance operation identity collision")]
    PublishCollision,
}

/// Runtime boundary for JJ-backed retention and in-process publication.
///
/// JJ remains the source system. The published operation map is only the
/// runtime's immutable publication sink; adapters never mutate it.
pub struct JjRuntime {
    retention: JjRetention,
    published: BTreeMap<OperationId<JjModel>, Operation<JjModel>>,
}

impl std::fmt::Debug for JjRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JjRuntime")
            .field("published_operations", &self.published.len())
            .finish()
    }
}

impl JjRuntime {
    pub fn new(repo: Arc<ReadonlyRepo>) -> Self {
        Self {
            retention: JjRetention::new(repo),
            published: BTreeMap::new(),
        }
    }

    pub fn retention(&self) -> &JjRetention {
        &self.retention
    }

    pub fn published_operation(&self, id: &OperationId<JjModel>) -> Option<&Operation<JjModel>> {
        self.published.get(id)
    }

    pub fn published_len(&self) -> usize {
        self.published.len()
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
        if let Some(existing) = self.published.get(&operation.id) {
            if existing == operation {
                return Ok(());
            }
            return Err(JjRuntimeError::PublishCollision);
        }
        self.published
            .insert(operation.id.clone(), operation.clone());
        Ok(())
    }

    fn finalize(&mut self, action: &FinalizeAction<JjModel>) -> Result<(), Self::Error> {
        match action {
            FinalizeAction::Retention(transition) => {
                self.retention.finalize(transition).map_err(Into::into)
            }
        }
    }
}
