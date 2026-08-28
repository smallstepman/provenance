//! Stable WASM component contract for provenance observation plugins.
//!
//! The WIT contract deliberately contains no provenance-core generic types or
//! host capabilities. Hosts convert its concrete records into a typed core
//! transaction and retain authority over validation, publication, persistence,
//! and query execution.

/// The package identifier of the plugin component contract.
pub const PACKAGE: &str = "provenance:plugin@0.1.0";

/// The checked-in WIT source used by host and guest bindings.
pub const WIT_SOURCE: &str = include_str!("../../wit/provenance-plugin.wit");

/// Guest-side bindings for plugin authors.
///
#[cfg(feature = "guest")]
pub mod guest {
    #![allow(clippy::too_many_arguments)] // Generated bindings expose the WIT ABI records directly.
    wit_bindgen::generate!({
        path: "wit",
        world: "plugin",
    });
}
