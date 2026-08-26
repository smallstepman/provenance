use std::collections::BTreeMap;

use provenance_core::{Adapter, Error as CoreError, SchemaKey, Transaction};
use thiserror::Error;
use wasmtime::{
    Config, Engine, Store,
    component::{Component, Linker},
};

use super::{PluginModel, bindings, conversion};

pub const SUPPORTED_ABI_VERSION: &str = "0.1.0";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PluginManifest {
    pub id: String,
    pub abi_version: String,
    pub namespace: String,
    pub schemas: Vec<SchemaKey>,
    pub required_schemas: Vec<SchemaKey>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PluginHostError {
    #[error("WASM engine error: {0}")]
    Wasmtime(String),
    #[error("plugin `{0}` is not registered")]
    UnknownPlugin(String),
    #[error("plugin id `{0}` is already registered")]
    DuplicatePlugin(String),
    #[error("namespace `{namespace}` is already owned by plugin `{owner}`")]
    NamespaceConflict { namespace: String, owner: String },
    #[error("plugin manifest field `{0}` must not be empty")]
    EmptyManifestField(&'static str),
    #[error("plugin `{plugin}` declares unsupported ABI `{actual}`; host supports `{expected}`")]
    UnsupportedAbi {
        plugin: String,
        actual: String,
        expected: &'static str,
    },
    #[error("plugin `{plugin}` declares schema `{namespace}@{version}` outside its namespace")]
    SchemaNamespaceMismatch {
        plugin: String,
        namespace: String,
        version: String,
    },
    #[error("plugin `{plugin}` declares duplicate schema `{namespace}@{version}`")]
    DuplicateSchema {
        plugin: String,
        namespace: String,
        version: String,
    },
    #[error("plugin `{plugin}` returned {kind}: {message}")]
    PluginReturned {
        plugin: String,
        kind: String,
        message: String,
    },
    #[error("WIT transaction conversion failed: {0}")]
    Conversion(#[from] conversion::ConversionError),
    #[error("provenance core rejected plugin transaction: {0:?}")]
    Core(CoreError),
}

impl From<wasmtime::Error> for PluginHostError {
    fn from(error: wasmtime::Error) -> Self {
        Self::Wasmtime(error.to_string())
    }
}

#[derive(Clone)]
struct RegisteredPlugin {
    manifest: PluginManifest,
    component: Component,
}

/// Loads and invokes typed provenance observation components.
///
/// Registration and invocation are host concerns. Returned transactions still
/// have to pass through provenance-core; this manager never mutates core state.
pub struct PluginManager {
    engine: Engine,
    plugins: BTreeMap<String, RegisteredPlugin>,
    namespaces: BTreeMap<String, String>,
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new().expect("default Wasmtime configuration")
    }
}

impl PluginManager {
    pub fn new() -> Result<Self, PluginHostError> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;
        Ok(Self {
            engine,
            plugins: BTreeMap::new(),
            namespaces: BTreeMap::new(),
        })
    }

    pub fn register_component(
        &mut self,
        component_bytes: &[u8],
    ) -> Result<PluginManifest, PluginHostError> {
        let component = Component::new(&self.engine, component_bytes)?;
        let linker = Linker::new(&self.engine);
        let mut store = Store::new(&self.engine, ());
        let instance = bindings::Plugin::instantiate(&mut store, &component, &linker)
            .map_err(|error| PluginHostError::Wasmtime(error.to_string()))?;
        let manifest = convert_manifest(instance.call_manifest(&mut store)?)?;
        self.validate_manifest(&manifest)?;

        let id = manifest.id.clone();
        let namespace = manifest.namespace.clone();
        self.plugins.insert(
            id.clone(),
            RegisteredPlugin {
                manifest: manifest.clone(),
                component,
            },
        );
        self.namespaces.insert(namespace, id);
        Ok(manifest)
    }

    pub fn manifest(&self, plugin_id: &str) -> Result<&PluginManifest, PluginHostError> {
        self.plugins
            .get(plugin_id)
            .map(|plugin| &plugin.manifest)
            .ok_or_else(|| PluginHostError::UnknownPlugin(plugin_id.to_owned()))
    }

    pub fn plugin_ids(&self) -> impl Iterator<Item = &str> {
        self.plugins.keys().map(String::as_str)
    }

    pub fn observe(
        &self,
        plugin_id: &str,
        request: bindings::ObservationRequest,
    ) -> Result<Transaction<PluginModel>, PluginHostError> {
        self.adapter(plugin_id)?.transaction(request)
    }

    pub fn adapter(&self, plugin_id: &str) -> Result<WasmPluginAdapter, PluginHostError> {
        let plugin = self
            .plugins
            .get(plugin_id)
            .ok_or_else(|| PluginHostError::UnknownPlugin(plugin_id.to_owned()))?;
        Ok(WasmPluginAdapter {
            engine: self.engine.clone(),
            component: plugin.component.clone(),
            plugin_id: plugin.manifest.id.clone(),
        })
    }

    fn validate_manifest(&self, manifest: &PluginManifest) -> Result<(), PluginHostError> {
        if manifest.id.trim().is_empty() {
            return Err(PluginHostError::EmptyManifestField("id"));
        }
        if manifest.abi_version.trim().is_empty() {
            return Err(PluginHostError::EmptyManifestField("abi-version"));
        }
        if manifest.namespace.trim().is_empty() {
            return Err(PluginHostError::EmptyManifestField("namespace"));
        }
        if manifest.abi_version != SUPPORTED_ABI_VERSION {
            return Err(PluginHostError::UnsupportedAbi {
                plugin: manifest.id.clone(),
                actual: manifest.abi_version.clone(),
                expected: SUPPORTED_ABI_VERSION,
            });
        }
        if self.plugins.contains_key(&manifest.id) {
            return Err(PluginHostError::DuplicatePlugin(manifest.id.clone()));
        }
        if let Some(owner) = self.namespaces.get(&manifest.namespace) {
            return Err(PluginHostError::NamespaceConflict {
                namespace: manifest.namespace.clone(),
                owner: owner.clone(),
            });
        }

        let mut seen = std::collections::BTreeSet::new();
        for schema in &manifest.schemas {
            let key = format!("{}@{}", schema.namespace.as_str(), schema.version.as_str());
            if !seen.insert(key) {
                return Err(PluginHostError::DuplicateSchema {
                    plugin: manifest.id.clone(),
                    namespace: schema.namespace.as_str().to_owned(),
                    version: schema.version.as_str().to_owned(),
                });
            }
            if schema.namespace.as_str() != manifest.namespace {
                return Err(PluginHostError::SchemaNamespaceMismatch {
                    plugin: manifest.id.clone(),
                    namespace: schema.namespace.as_str().to_owned(),
                    version: schema.version.as_str().to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// Core `Adapter` bridge for one registered WASM component.
#[derive(Clone)]
pub struct WasmPluginAdapter {
    engine: Engine,
    component: Component,
    plugin_id: String,
}

impl Adapter<PluginModel> for WasmPluginAdapter {
    type Input = bindings::ObservationRequest;
    type Error = PluginHostError;

    fn transaction(&self, input: Self::Input) -> Result<Transaction<PluginModel>, Self::Error> {
        let linker = Linker::new(&self.engine);
        let mut store = Store::new(&self.engine, ());
        let instance = bindings::Plugin::instantiate(&mut store, &self.component, &linker)
            .map_err(|error| PluginHostError::Wasmtime(error.to_string()))?;
        let result = instance
            .call_observe(&mut store, &input)
            .map_err(|error| PluginHostError::Wasmtime(error.to_string()))?;
        let transaction = result.map_err(|error| PluginHostError::PluginReturned {
            plugin: self.plugin_id.clone(),
            kind: plugin_error_kind(error.kind),
            message: error.message,
        })?;
        conversion::transaction(transaction).map_err(PluginHostError::Conversion)
    }

    fn core_error(&self, error: CoreError) -> Self::Error {
        PluginHostError::Core(error)
    }
}

fn convert_manifest(manifest: bindings::PluginManifest) -> Result<PluginManifest, PluginHostError> {
    Ok(PluginManifest {
        id: manifest.id,
        abi_version: manifest.abi_version,
        namespace: manifest.namespace,
        schemas: manifest
            .schemas
            .into_iter()
            .map(|key| SchemaKey::new(key.namespace, key.version))
            .collect(),
        required_schemas: manifest
            .required_schemas
            .into_iter()
            .map(|key| SchemaKey::new(key.namespace, key.version))
            .collect(),
        capabilities: manifest.capabilities,
    })
}

fn plugin_error_kind(kind: bindings::PluginErrorKind) -> String {
    match kind {
        bindings::PluginErrorKind::InvalidRequest => "invalid-request",
        bindings::PluginErrorKind::Unsupported => "unsupported",
        bindings::PluginErrorKind::SourceFailure => "source-failure",
        bindings::PluginErrorKind::Internal => "internal",
    }
    .to_owned()
}
