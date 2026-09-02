use std::io;

use provenance_jj::{
    JjIngestError, JjPluginError, JjQueryError, JjRepositoryError, JjRuntimeError, JjServiceError,
};
use provenance_plugin::PluginHostError;
use thiserror::Error;

/// Errors returned by the `jj-prov` command-line boundary.
#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid JSON hook payload: {0}")]
    Json(#[from] serde_json::Error),
    #[error("could not construct JJ settings: {0}")]
    Settings(String),
    #[error(transparent)]
    Repository(#[from] JjRepositoryError),
    #[error(transparent)]
    Ingest(#[from] JjIngestError),
    #[error(transparent)]
    Runtime(#[from] JjRuntimeError),
    #[error(transparent)]
    Service(#[from] JjServiceError),
    #[error(transparent)]
    PluginHost(#[from] PluginHostError),
    #[error(transparent)]
    PluginIngest(#[from] JjPluginError),
    #[error(transparent)]
    Query(#[from] JjQueryError),
    #[error(transparent)]
    Core(#[from] provenance_core::Error),
    #[error("hook input error: {0}")]
    Hook(String),
}
