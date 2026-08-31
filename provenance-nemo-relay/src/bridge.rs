use std::fmt::Display;
use std::io::BufRead;

use provenance_core::{
    Adapter, EntityAddress, IdentityScheme, PluginModel, Rules, Runtime, Service, Transaction,
};
use provenance_plugin::{PluginHostError, PluginManager, WasmPluginAdapter};

use crate::error::RelayBridgeError;
use crate::projector::RelayProjector;
use crate::spool::LocalEventSpool;

/// A sink for transactions returned by the generic agent WASM plugin.
pub trait TransactionProcessor: Send {
    /// Process a transaction through the authoritative provenance service.
    fn process_transaction(&mut self, transaction: Transaction<PluginModel>) -> Result<(), String>;

    /// Report whether an immutable source operation is already authoritative.
    ///
    /// Custom processors may use the default conservative answer and rely on
    /// this bridge instance's in-memory event history.
    fn has_source(&self, _source: &EntityAddress<PluginModel>) -> bool {
        false
    }
}

impl<I, R, A, RT> TransactionProcessor for Service<PluginModel, I, R, A, RT>
where
    I: IdentityScheme<PluginModel>,
    R: Rules<PluginModel>,
    A: Adapter<PluginModel> + Send,
    RT: Runtime<PluginModel> + Send,
    RT::Error: Display,
{
    fn process_transaction(&mut self, transaction: Transaction<PluginModel>) -> Result<(), String> {
        Service::process_transaction(self, transaction).map_err(|error| error.to_string())
    }
    fn has_source(&self, source: &EntityAddress<PluginModel>) -> bool {
        self.state.projection.source_to_operation.contains(source)
    }
}

/// Result of one subscriber delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    /// The event was projected and committed.
    Ingested { event_key: String },
    /// The event was valid telemetry/evidence but not provenance-relevant.
    Skipped { event_key: String },
    /// The same event or source fact was already committed.
    Duplicate { event_key: String },
}

/// Stateful bridge from NeMo Relay ATOF events to a provenance service.
pub struct RelayBridge<P: TransactionProcessor> {
    adapter: WasmPluginAdapter,
    projector: RelayProjector,
    processor: P,
    spool: Option<LocalEventSpool>,
    seen_events: std::collections::BTreeSet<String>,
}

impl<P: TransactionProcessor> std::fmt::Debug for RelayBridge<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayBridge")
            .field("projector", &self.projector)
            .field("seen_events", &self.seen_events.len())
            .field("spool_enabled", &self.spool.is_some())
            .finish_non_exhaustive()
    }
}

impl<P: TransactionProcessor> RelayBridge<P> {
    /// Create a bridge from a registered agent WASM adapter.
    pub fn new(
        adapter: WasmPluginAdapter,
        projector: RelayProjector,
        processor: P,
        spool: Option<LocalEventSpool>,
    ) -> Self {
        Self {
            adapter,
            projector,
            processor,
            spool,
            seen_events: std::collections::BTreeSet::new(),
        }
    }

    /// Create a bridge using one plugin registered in a manager.
    pub fn from_manager(
        manager: &PluginManager,
        plugin_id: &str,
        projector: RelayProjector,
        processor: P,
        spool: Option<LocalEventSpool>,
    ) -> Result<Self, PluginHostError> {
        Ok(Self::new(
            manager.adapter(plugin_id)?,
            projector,
            processor,
            spool,
        ))
    }

    /// Process one ATOF event.
    pub fn ingest(
        &mut self,
        event: &nemo_relay_plugin::Event,
    ) -> Result<IngestOutcome, RelayBridgeError> {
        let event_key = RelayProjector::event_key(event);
        if self.seen_events.contains(&event_key) {
            return Ok(IngestOutcome::Duplicate { event_key });
        }

        let evidence = self
            .spool
            .as_mut()
            .map(|spool| spool.append(event))
            .transpose()?;
        let Some(request) = self.projector.project(event, evidence.as_ref())? else {
            self.seen_events.insert(event_key.clone());
            return Ok(IngestOutcome::Skipped { event_key });
        };
        let mut transaction = self.adapter.transaction(request)?;
        if let Some(source) = transaction.source.as_ref()
            && self.processor.has_source(&source.id)
        {
            self.projector.remember_event(event);
            self.seen_events.insert(event_key.clone());
            return Ok(IngestOutcome::Duplicate { event_key });
        }
        if let Some(source) = transaction.source.as_mut() {
            let processor = &self.processor;
            source.parents = self
                .projector
                .source_parents(event)
                .into_iter()
                .filter(|parent| {
                    processor.has_source(parent) || self.seen_events.contains(&parent.id)
                })
                .collect();
        }
        self.processor
            .process_transaction(transaction)
            .map_err(RelayBridgeError::Processing)?;
        self.projector.remember_event(event);
        self.seen_events.insert(event_key.clone());
        Ok(IngestOutcome::Ingested { event_key })
    }

    /// Ingest newline-delimited canonical ATOF events from a file or pipe.
    pub fn ingest_jsonl<R: BufRead>(&mut self, reader: R) -> Result<usize, RelayBridgeError> {
        let mut ingested = 0;
        for (line_index, line) in reader.lines().enumerate() {
            let line = line.map_err(RelayBridgeError::AtofRead)?;
            if line.trim().is_empty() {
                continue;
            }
            let event =
                serde_json::from_str(line.trim()).map_err(|source| RelayBridgeError::AtofLine {
                    line: line_index + 1,
                    source,
                })?;
            if matches!(self.ingest(&event)?, IngestOutcome::Ingested { .. }) {
                ingested += 1;
            }
        }
        Ok(ingested)
    }

    /// Access the service processor for state inspection or controlled recovery.
    pub fn processor(&self) -> &P {
        &self.processor
    }

    /// Mutably access the service processor for restart/recovery orchestration.
    pub fn processor_mut(&mut self) -> &mut P {
        &mut self.processor
    }

    /// Access the stateful projector.
    pub fn projector(&self) -> &RelayProjector {
        &self.projector
    }

    /// Access the local spool when configured.
    pub fn spool(&self) -> Option<&LocalEventSpool> {
        self.spool.as_ref()
    }
}
