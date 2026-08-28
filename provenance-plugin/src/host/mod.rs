//! WASM component host for provenance observation plugins.
//!
//! A plugin can only return typed WIT observations. This crate owns component
//! loading, registration, manifest and namespace policy, and conversion into
//! the canonical provenance-core transaction protocol.
pub mod bindings;
mod conversion;
mod manager;
pub use provenance_core::PluginModel;

pub use manager::{
    PluginHostError, PluginManager, PluginManifest, SUPPORTED_ABI_VERSION, WasmPluginAdapter,
};
