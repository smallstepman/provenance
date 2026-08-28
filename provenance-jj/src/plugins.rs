use std::collections::BTreeSet;

use provenance_core::{EntityAddress, TransactionProcessError};
use provenance_plugin::{PluginHostError, PluginManager, bindings};
use thiserror::Error;

use crate::{JjModel, JjRuntimeError, JjService};

#[derive(Debug, Error)]
pub enum JjPluginError {
    #[error(transparent)]
    Plugin(#[from] PluginHostError),
    #[error(transparent)]
    Process(#[from] TransactionProcessError<JjRuntimeError>),
    #[error("plugin transaction has no source anchor")]
    MissingSourceAnchor,
}

/// Observe and commit one plugin transaction into a JJ-backed service.
///
/// The plugin remains the owner of its source identity. This helper only
/// submits the resulting transaction through the normal core commit protocol.
pub fn ingest_plugin(
    service: &mut JjService,
    plugins: &PluginManager,
    plugin_id: &str,
    request: bindings::ObservationRequest,
) -> Result<(), JjPluginError> {
    ingest_plugin_with_causal_parents(service, plugins, plugin_id, request, std::iter::empty())
}

/// Observe and commit one plugin transaction, optionally attaching explicit
/// source-operation parents from the host context.
///
/// A causal parent is an operation-source address such as
/// `jj://operation/<id>`. It is distinct from a domain relation such as
/// `tracker://task/123 --implemented-by--> jj://commit/<id>`.
pub fn ingest_plugin_with_causal_parents<I>(
    service: &mut JjService,
    plugins: &PluginManager,
    plugin_id: &str,
    request: bindings::ObservationRequest,
    causal_parents: I,
) -> Result<(), JjPluginError>
where
    I: IntoIterator<Item = EntityAddress<JjModel>>,
{
    let mut transaction = plugins.observe(plugin_id, request)?;
    let causal_parents = causal_parents.into_iter().collect::<BTreeSet<_>>();
    let source = transaction
        .source
        .as_mut()
        .ok_or(JjPluginError::MissingSourceAnchor)?;
    source.parents.extend(causal_parents);
    service.process_transaction(transaction)?;
    Ok(())
}
