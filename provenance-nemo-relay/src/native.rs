use std::fs;
use std::sync::{Arc, Mutex};

use nemo_relay_plugin::{
    ConfigDiagnostic, DiagnosticLevel, Event, Json, NativePlugin, PluginContext,
};
use provenance_core::{DefaultRules, Kernel, PluginModel, Service};
use provenance_plugin::{PluginManager, WasmPluginAdapter};
use serde_json::Map;

use crate::bridge::RelayBridge;
use crate::config::RelayPluginConfig;
use crate::error::RelayBridgeError;
use crate::identity::AgentIdentity;
use crate::projector::RelayProjector;
use crate::runtime::DirectoryRuntime;
use crate::spool::LocalEventSpool;

/// Stable NeMo Relay dynamic-plugin kind.
pub const NATIVE_PLUGIN_KIND: &str = "provenance";
/// Subscriber name registered in the Relay process.
pub const SUBSCRIBER_NAME: &str = "provenance-agent";

/// Concrete durable service used by the native dynamic plugin.
pub type AgentService =
    Service<PluginModel, AgentIdentity, DefaultRules, WasmPluginAdapter, DirectoryRuntime>;

/// Native NeMo Relay plugin that projects ATOF into provenance.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProvenanceRelayPlugin;

impl NativePlugin for ProvenanceRelayPlugin {
    fn plugin_kind(&self) -> &str {
        NATIVE_PLUGIN_KIND
    }

    fn allows_multiple_components(&self) -> bool {
        false
    }

    fn validate(&self, plugin_config: &Map<String, Json>) -> Vec<ConfigDiagnostic> {
        match RelayPluginConfig::from_map(plugin_config) {
            Ok(config) => match config.validate() {
                Ok(_) => Vec::new(),
                Err(message) => vec![diagnostic("provenance.config.invalid", message)],
            },
            Err(message) => vec![diagnostic("provenance.config.invalid", message)],
        }
    }

    fn register(
        &mut self,
        plugin_config: &Map<String, Json>,
        context: &mut PluginContext<'_>,
    ) -> nemo_relay_plugin::Result<()> {
        let config = RelayPluginConfig::from_map(plugin_config)?;
        let identity = config.validate()?;
        let component = fs::read(&config.agent_plugin_path)
            .map_err(|error| format!("cannot read agent plugin component: {error}"))?;
        let mut manager = PluginManager::new().map_err(|error| error.to_string())?;
        let manifest = manager
            .register_component(&component)
            .map_err(|error| error.to_string())?;
        if manifest.id != config.agent_plugin_id {
            return Err(format!(
                "agent component manifest id '{}' does not match configured id '{}'",
                manifest.id, config.agent_plugin_id
            ));
        }

        let adapter = manager
            .adapter(&config.agent_plugin_id)
            .map_err(|error| error.to_string())?;
        let runtime =
            DirectoryRuntime::open(&config.store_path).map_err(|error| error.to_string())?;
        let state = runtime.load_state().map_err(|error| error.to_string())?;
        let service = AgentService {
            kernel: Kernel::new(AgentIdentity),
            adapter: adapter.clone(),
            runtime,
            state,
        };
        let spool = config
            .spool_path
            .as_ref()
            .map(LocalEventSpool::open)
            .transpose()
            .map_err(|error| error.to_string())?;
        let bridge = Arc::new(Mutex::new(RelayBridge::new(
            adapter,
            RelayProjector::with_capture_mode_and_telemetry_urls(
                identity,
                config.capture_mode,
                config.telemetry_provider.clone(),
                config.telemetry_links.clone(),
            ),
            service,
            spool,
        )));
        context.register_subscriber(SUBSCRIBER_NAME, move |event: &Event| {
            let mut bridge = match bridge.lock() {
                Ok(bridge) => bridge,
                Err(_) => {
                    tracing::error!("provenance Relay bridge lock is poisoned");
                    return;
                }
            };
            if let Err(error) = bridge.ingest(event) {
                tracing::error!(error = %error, "ATOF event was not committed to provenance");
            }
        })?;
        Ok(())
    }
}

fn diagnostic(code: &str, message: String) -> ConfigDiagnostic {
    ConfigDiagnostic {
        level: DiagnosticLevel::Error,
        code: code.into(),
        component: Some(NATIVE_PLUGIN_KIND.into()),
        field: None,
        message,
    }
}

// Native entry symbol consumed by NeMo Relay's dynamic plugin loader.
nemo_relay_plugin::nemo_relay_plugin!(provenance_relay_plugin, || { ProvenanceRelayPlugin });

// Keep this import in the module's public error contract. The callback itself
// logs delivery errors because NeMo Relay subscriber callbacks cannot return
// a fallible result.
#[allow(dead_code)]
fn _bridge_error_type(_: RelayBridgeError) {}
