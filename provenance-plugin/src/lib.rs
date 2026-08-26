//! Host and interface support for provenance observation plugins.
//!
//! The host loads typed WASM components and converts their observations into
//! `provenance-core` transactions. The interface module owns the versioned WIT
//! contract shared by host and guest implementations.

pub mod host;
pub mod interface;
pub use host::bindings;
pub use host::{
    PluginHostError, PluginManager, PluginManifest, PluginModel, SUPPORTED_ABI_VERSION,
    WasmPluginAdapter,
};
pub use interface::{PACKAGE, WIT_SOURCE};
