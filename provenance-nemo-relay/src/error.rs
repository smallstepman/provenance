use std::io;

use thiserror::Error;

/// Errors raised while projecting and committing a Relay event.
#[derive(Debug, Error)]
pub enum RelayBridgeError {
    /// The event could not be converted to the generic provenance request.
    #[error("ATOF projection failed: {0}")]
    Projection(#[from] ProjectionError),
    /// The local evidence spool could not be written.
    #[error("local ATOF spool failed: {0}")]
    Spool(#[from] SpoolError),
    /// The configured WASM projection plugin rejected or could not process the request.
    #[error("agent WASM plugin failed: {0}")]
    Plugin(#[from] provenance_plugin::PluginHostError),
    /// The provenance service rejected the resulting transaction.
    #[error("provenance transaction failed: {0}")]
    Processing(String),
    /// A JSONL import line was not a valid ATOF event.
    #[error("invalid ATOF JSONL at line {line}: {source}")]
    AtofLine {
        /// One-based line number in the imported stream.
        line: usize,
        /// JSON parser failure.
        #[source]
        source: serde_json::Error,
    },
    /// A JSONL reader failed before an event could be decoded.
    #[error("cannot read ATOF JSONL: {0}")]
    AtofRead(#[source] io::Error),
    /// The bridge mutex was poisoned after a panic in an earlier callback.
    #[error("provenance Relay bridge lock is poisoned")]
    LockPoisoned,
}

/// Errors raised by the bounded ATOF projection layer.
#[derive(Debug, Error)]
pub enum ProjectionError {
    /// JSON payload hashing failed.
    #[error("cannot serialize {field} payload: {source}")]
    Serialize {
        /// Payload name (`data` or `metadata`).
        field: &'static str,
        /// Serialization failure.
        #[source]
        source: serde_json::Error,
    },
    /// A configured identity component was empty.
    #[error("{field} must not be empty")]
    EmptyIdentity {
        /// Configuration field name.
        field: &'static str,
    },
}

/// Errors raised by the local append-only ATOF evidence spool.
#[derive(Debug, Error)]
pub enum SpoolError {
    /// Filesystem operation failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Event serialization failed.
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),
}
