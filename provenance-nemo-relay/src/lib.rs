//! NeMo Relay integration for the provenance observation plugin system.
//!
//! The crate keeps NeMo Relay's ATOF event stream as the normalization
//! boundary. By default it records only sparse attribution links for explicit
//! source operations, session boundaries, and external trace/evidence
//! references. Complete ATOF JSONL evidence and telemetry remain outside the
//! provenance graph; an explicit audit mode retains the all-event projection.

mod bridge;
mod config;
mod error;
mod identity;
mod native;
mod projector;
mod runtime;
mod spool;

pub use bridge::{IngestOutcome, RelayBridge, TransactionProcessor};
pub use config::{
    RelayCaptureMode, RelayIdentityConfig, RelayPluginConfig, RelayTelemetryUrlConfig,
    RelayTelemetryUrls,
};
pub use identity::AgentIdentity;
pub use native::{AgentService, NATIVE_PLUGIN_KIND, ProvenanceRelayPlugin, SUBSCRIBER_NAME};
pub use projector::{
    AGENT_EVENT_KIND, AGENT_LINK_KIND, AGENT_NAMESPACE, AGENT_SESSION_EVENT_KIND, RelayProjector,
};
pub use runtime::{DirectoryRuntime, DirectoryRuntimeError, durable_heads};
pub use spool::{EvidenceLocator, LocalEventSpool, spool_size};

/// Canonical ATOF version accepted by this integration package.
pub const ATOF_VERSION: &str = "0.1";
