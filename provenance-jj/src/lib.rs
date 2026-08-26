//! Jujutsu-backed hydration for provenance-core.
use std::path::Path;

use provenance_core::{DefaultRules, Kernel, Service};
use thiserror::Error;

mod adapter;
mod authoritative;
mod codec;
mod identity;
mod ingest;
mod model;
mod query;
mod retention;
mod runtime;
mod schema;

pub use adapter::{
    JjAdapter, JjAdapterError, JjRepository, JjRepositoryError, JjRepositorySource,
    observe_operation_delta, observe_repository,
};
pub use authoritative::{DirectoryProvenanceStore, DirectoryStoreError};
pub use identity::{JjIdentity, observation_seed};
pub use ingest::{JjIngestError, ingest_repository};
pub use model::{
    CHANGE_KIND, COMMIT_KIND, JJ_NAMESPACE, JjModel, OPERATION_KIND, WORKSPACE_KIND, external,
    jj_address, jj_change, jj_commit_address, jj_operation, jj_workspace, string_value,
};
pub use query::{JjQueryError, compile_why, jj_commit, jj_entity, why_jj_commit};
pub use retention::{JjRetention, JjRetentionError, KEEP_REF_PREFIX, keep_ref_name};
pub use runtime::{JjRuntime, JjRuntimeError};
pub use schema::{JJ_SCHEMA_VERSION, WHY_QUERY_NAME, jj_schema, jj_schema_key, jj_why_query};

pub type JjService = Service<JjModel, JjIdentity, DefaultRules, JjAdapter, JjRuntime>;

#[derive(Debug, Error)]
pub enum JjServiceError {
    #[error(transparent)]
    Runtime(#[from] JjRuntimeError),
    #[error(transparent)]
    Core(#[from] provenance_core::Error),
}

pub fn open_service(
    repository: &JjRepository,
    store_path: impl AsRef<Path>,
) -> Result<JjService, JjServiceError> {
    let runtime = JjRuntime::open(repository.repo().clone(), store_path)?;
    let state = runtime.load_state()?;
    Ok(Service {
        kernel: Kernel::new(JjIdentity),
        adapter: JjAdapter,
        runtime,
        state,
    })
}
