//! Authoritative provenance storage.
//!
//! This crate provides in-memory and filesystem-backed implementations of the
//! storage contract from `provenance-core`. It does not depend on a particular
//! source adapter or repository implementation.

use std::collections::BTreeSet;

use provenance_core::{Model, Operation, OperationId, ProvenanceStore};
use thiserror::Error;

mod codec;
mod implementation;

pub use codec::CodecError;
pub use implementation::{DirectoryProvenanceStore, MemoryProvenanceStore};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("authoritative provenance filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("operation {0} has different immutable contents")]
    OperationCollision(String),
    #[error("object {0} has different immutable contents")]
    ObjectCollision(String),
    #[error("authoritative provenance operation {0} is missing")]
    MissingOperation(String),
    #[error("authoritative provenance path component is invalid: {0}")]
    InvalidComponent(String),
}

fn collect_reachable<M, S>(
    store: &S,
    id: &OperationId<M>,
    seen: &mut BTreeSet<OperationId<M>>,
    ordered: &mut Vec<Operation<M>>,
) -> Result<(), StorageError>
where
    M: Model,
    M::Id: AsRef<str>,
    S: ProvenanceStore<M, Error = StorageError>,
{
    if !seen.insert(id.clone()) {
        return Ok(());
    }
    let operation = store
        .get_operation(id)?
        .ok_or_else(|| StorageError::MissingOperation(id.raw().as_ref().to_owned()))?;
    for parent in &operation.parents {
        collect_reachable(store, parent, seen, ordered)?;
    }
    ordered.push(operation);
    Ok(())
}
