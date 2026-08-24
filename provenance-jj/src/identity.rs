use provenance_core::{IdentityKind, IdentityScheme};
use sha2::{Digest, Sha256};

use crate::JjModel;

/// Deterministic identities derived from the JJ operation observation seed.
#[derive(Clone, Copy, Debug, Default)]
pub struct JjIdentity;

impl JjIdentity {
    fn kind_name(kind: IdentityKind) -> &'static str {
        match kind {
            IdentityKind::Operation => "operation",
            IdentityKind::Event => "event",
            IdentityKind::Session => "session",
            IdentityKind::Actor => "actor",
            IdentityKind::Object => "object",
            IdentityKind::Claim => "claim",
            IdentityKind::Replica => "replica",
        }
    }

    fn digest(input: &str) -> String {
        let digest = Sha256::digest(input.as_bytes());
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl IdentityScheme<JjModel> for JjIdentity {
    fn derive(&self, kind: IdentityKind, seed: &String, discriminator: &str) -> String {
        Self::digest(&format!(
            "provenance-jj/v1\0{}\0{}\0{}",
            Self::kind_name(kind),
            seed,
            discriminator
        ))
    }
}

/// Returns the stable transaction seed for a JJ operation observation.
pub fn observation_seed(operation_id: &str) -> String {
    format!("jj-operation:{operation_id}")
}
