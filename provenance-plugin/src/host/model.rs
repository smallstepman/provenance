use serde::{Deserialize, Serialize};

/// Primitive universe shared by transactions emitted through the WASM host.
///
/// Plugin entities from different namespaces must inhabit one model so the
/// generic provenance graph can explain relations across plugins.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PluginModel;

impl provenance_core::Model for PluginModel {
    type Id = String;
    type Seed = String;
    type ExternalId = String;
    type Payload = Vec<u8>;
}
