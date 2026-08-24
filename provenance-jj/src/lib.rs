//! Jujutsu-backed hydration for provenance-core.
mod ingest;

mod adapter;
mod identity;
mod model;
mod query;
mod retention;
mod runtime;
mod schema;
pub use ingest::{JjIngestError, ingest_repository};

pub use adapter::{
    JjAdapter, JjAdapterError, JjRepository, JjRepositoryError, JjRepositorySource,
    observe_repository,
};
pub use identity::{JjIdentity, observation_seed};
pub use model::{
    CHANGE_KIND, COMMIT_KIND, JJ_NAMESPACE, JjModel, OPERATION_KIND, WORKSPACE_KIND, external,
    jj_address, jj_change, jj_commit_address, jj_operation, jj_workspace, string_value,
};
pub use query::{JjQueryError, compile_why, jj_commit, jj_entity, why_jj_commit};
pub use retention::{JjRetention, JjRetentionError, KEEP_REF_PREFIX, keep_ref_name};
pub use runtime::{JjRuntime, JjRuntimeError};
pub use schema::{JJ_SCHEMA_VERSION, WHY_QUERY_NAME, jj_schema, jj_schema_key, jj_why_query};
