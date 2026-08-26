//! Effectful integration boundaries and service orchestration.

use crate::protocol::{CommitPlan, FinalizeAction, Kernel, PrepareRequirement, Rules};
use crate::state::{Operation, State};
use crate::{Error, Transaction};
use provenance_data_model::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error as ThisError;

/// Pure plugin/integration boundary.
///
/// JJ adapter, issue adapter, docs adapter, agent adapter, etc. implement this.
///
/// A WASM host would convert WIT values into Adapter-native input or directly
/// into Transaction.
///
/// The kernel does not know WIT exists.
pub trait Adapter<M: Model> {
    type Input;
    type Error;
    fn transaction(&self, input: Self::Input) -> std::result::Result<Transaction<M>, Self::Error>;
    fn core_error(&self, error: Error) -> Self::Error;
}

pub trait AdapterExt<M: Model>: Adapter<M> {
    fn plan<I, R>(
        &self,
        kernel: &Kernel<M, I, R>,
        state: &State<M>,
        input: Self::Input,
    ) -> std::result::Result<CommitPlan<M>, Self::Error>
    where
        I: IdentityScheme<M>,
        R: Rules<M>,
    {
        let transaction = self.transaction(input)?;
        kernel
            .transact(state, transaction)
            .map_err(|error| self.core_error(error))
    }
}

impl<M, A> AdapterExt<M> for A
where
    M: Model,
    A: Adapter<M>,
{
}

/// Result of an immutable operation/head publication attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The operation and requested heads were newly published.
    Published,
    /// The exact operation was already present.
    AlreadyPresent,
    /// The operation was published and heads were merged with a concurrent writer.
    Merged,
}

/// Authoritative immutable provenance operation/object storage.

/// Implementations own persistence and concurrency. Derived projections may be
/// rebuilt from this interface and must not replace it as authority.
pub trait ProvenanceStore<M: Model> {
    type Error;
    fn get_operation(
        &self,
        id: &OperationId<M>,
    ) -> std::result::Result<Option<Operation<M>>, Self::Error>;
    fn has_operation(&self, id: &OperationId<M>) -> std::result::Result<bool, Self::Error>;
    fn put_operation(&mut self, operation: &Operation<M>) -> std::result::Result<(), Self::Error>;
    fn get_object(&self, id: &ObjectId<M>) -> std::result::Result<Option<Object<M>>, Self::Error>;
    fn put_object(&mut self, object: &Object<M>) -> std::result::Result<(), Self::Error>;
    fn heads(&self) -> std::result::Result<BTreeSet<OperationId<M>>, Self::Error>;
    fn publish_heads(
        &mut self,
        expected: &BTreeSet<OperationId<M>>,
        next: &BTreeSet<OperationId<M>>,
    ) -> std::result::Result<PublishOutcome, Self::Error>;
}

/// Effectful implementation contract.
///
/// This may be implemented by:
///   filesystem store
///   object store
///   VCS retention layer
///   distributed service
///   anything else
///
/// Core itself remains pure.
pub trait Runtime<M: Model> {
    type Error;
    /// Idempotently establish prerequisite.
    fn prepare(
        &mut self,
        requirement: &PrepareRequirement<M>,
    ) -> std::result::Result<(), Self::Error>;
    /// Atomically publish immutable provenance Operation. Same operation twice must be harmless.
    fn publish(&mut self, operation: &Operation<M>) -> std::result::Result<(), Self::Error>;
    /// Idempotent post-publication reconciliation.
    fn finalize(&mut self, action: &FinalizeAction<M>) -> std::result::Result<(), Self::Error>;
}

pub struct Service<M, I, R, A, RT>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
    A: Adapter<M>,
    RT: Runtime<M>,
{
    pub kernel: Kernel<M, I, R>,
    pub adapter: A,
    pub runtime: RT,
    pub state: State<M>,
}

#[derive(Debug, ThisError, Serialize, Deserialize)]
pub enum ProcessError<A, R> {
    #[error("adapter stage failed: {0}")]
    Adapter(#[source] A),
    #[error("prepare stage failed: {0}")]
    Prepare(#[source] R),
    #[error("publish stage failed: {0}")]
    Publish(#[source] R),
    /// IMPORTANT: Operation is already authoritative when this is returned. Caller state has already advanced.
    #[error("finalize stage failed: {0}")]
    Finalize(#[source] R),
}

impl<M, I, R, A, RT> Service<M, I, R, A, RT>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
    A: Adapter<M>,
    RT: Runtime<M>,
{
    pub fn process(
        &mut self,
        input: A::Input,
    ) -> std::result::Result<(), ProcessError<A::Error, RT::Error>> {
        let plan = self
            .adapter
            .plan(&self.kernel, &self.state, input)
            .map_err(ProcessError::Adapter)?;
        if plan.idempotent {
            return Ok(());
        }
        // State must NOT advance if this fails.
        for requirement in &plan.prepare {
            self.runtime
                .prepare(requirement)
                .map_err(ProcessError::Prepare)?;
        }

        // This is the commit point.
        self.runtime
            .publish(&plan.operation)
            .map_err(ProcessError::Publish)?;

        // Operation is authoritative from HERE onward.
        self.state = plan.next_state;

        // Failure does NOT roll back state. Reconciliation should retry these operations later.
        for action in &plan.finalize {
            self.runtime
                .finalize(action)
                .map_err(ProcessError::Finalize)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("disk full")]
    struct TestError;

    #[test]
    fn process_error_includes_stage_and_source() {
        let error: ProcessError<TestError, TestError> = ProcessError::Publish(TestError);

        assert_eq!(error.to_string(), "publish stage failed: disk full");
        assert_eq!(
            std::error::Error::source(&error)
                .expect("process error source")
                .to_string(),
            "disk full"
        );
    }

    #[test]
    fn core_error_has_contextual_display_message() {
        assert_eq!(
            Error::MissingSchemaDependency.to_string(),
            "schema dependency is missing"
        );
    }
}
