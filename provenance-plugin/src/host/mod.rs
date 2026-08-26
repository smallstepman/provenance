//! WASM component host for provenance observation plugins.
//!
//! A plugin can only return typed WIT observations. This crate owns component
//! loading, registration, capability policy, and conversion into the generic
//! provenance-core transaction protocol.
pub mod bindings;
mod conversion;
mod manager;
mod model;

pub use manager::{
    PluginHostError, PluginManager, PluginManifest, SUPPORTED_ABI_VERSION, WasmPluginAdapter,
};
pub use model::PluginModel;
