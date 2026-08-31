use provenance_core::{IdentityKind, IdentityScheme, PluginModel};
use sha2::{Digest, Sha256};

/// Deterministic identity scheme for the harness-neutral agent graph.
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentIdentity;

impl IdentityScheme<PluginModel> for AgentIdentity {
    fn derive(&self, kind: IdentityKind, seed: &String, discriminator: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"provenance-agent/identity/v1\0");
        update_part(&mut hasher, identity_kind_name(kind));
        update_part(&mut hasher, seed);
        update_part(&mut hasher, discriminator);
        let digest = hasher.finalize();
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

fn update_part(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn identity_kind_name(kind: IdentityKind) -> &'static str {
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
