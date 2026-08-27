/// JJ uses the canonical provenance identity derivation.
pub use provenance_core::ProvenanceIdentity as JjIdentity;

/// Returns the stable transaction seed for a JJ operation observation.
pub fn observation_seed(operation_id: &str) -> String {
    format!("jj-operation:{operation_id}")
}
