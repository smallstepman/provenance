use provenance_data_model::{IdentityKind, IdentityScheme, Model};
use sha2::{Digest, Sha256};

/// Deterministic identity derivation for the canonical string-based model.
///
/// Adapters own transaction seeds. The kind and discriminator separate
/// operation, event, object, and other identities, while the versioned domain
/// keeps this scheme stable across integrations.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProvenanceIdentity;

impl ProvenanceIdentity {
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

impl<M> IdentityScheme<M> for ProvenanceIdentity
where
    M: Model<Id = String, Seed = String>,
{
    fn derive(&self, kind: IdentityKind, seed: &String, discriminator: &str) -> String {
        Self::digest(&format!(
            "provenance/v1\0{}\0{}\0{}",
            Self::kind_name(kind),
            seed,
            discriminator
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use provenance_data_model::{OperationId, ProvenanceModel};

    #[test]
    fn identity_derivation_uses_the_versioned_provenance_domain() {
        let seed = "jj-operation:abc".to_owned();
        assert_eq!(
            <ProvenanceIdentity as IdentityScheme<ProvenanceModel>>::derive(
                &ProvenanceIdentity,
                IdentityKind::Event,
                &seed,
                "event-id",
            ),
            "4095c649ec3cab403930aa39da5aba43ae5b20adb0c0c6b733981bd3e4c2d130"
        );
    }

    #[test]
    fn identity_scheme_supports_the_canonical_model() {
        let seed = "plugin-observation".to_owned();
        let identity = ProvenanceIdentity;
        let operation: OperationId<ProvenanceModel> =
            <ProvenanceIdentity as IdentityScheme<ProvenanceModel>>::operation(&identity, &seed);
        assert_eq!(
            operation.raw().len(),
            64,
            "canonical identities are SHA-256 hex strings"
        );
    }
}
