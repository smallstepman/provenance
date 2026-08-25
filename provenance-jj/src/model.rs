use provenance_core::{EntityAddress, EntityKind, EntityRef, Namespace, Value};
use serde::{Deserialize, Serialize};

/// Primitive universe used by the Jujutsu provenance adapter.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct JjModel;

impl provenance_core::Model for JjModel {
    type Id = String;
    type Seed = String;
    type ExternalId = String;
    type Payload = Vec<u8>;
}

pub const JJ_NAMESPACE: &str = "jj";
pub const OPERATION_KIND: &str = "operation";
pub const COMMIT_KIND: &str = "commit";
pub const CHANGE_KIND: &str = "change";
pub const WORKSPACE_KIND: &str = "workspace";

pub fn jj_address(kind: impl Into<String>, id: impl Into<String>) -> EntityAddress<JjModel> {
    EntityAddress {
        namespace: Namespace::from(JJ_NAMESPACE),
        kind: EntityKind::from(kind.into()),
        id: id.into(),
    }
}

pub fn jj_operation(id: impl Into<String>) -> EntityAddress<JjModel> {
    jj_address(OPERATION_KIND, id)
}

pub fn jj_commit_address(id: impl Into<String>) -> EntityAddress<JjModel> {
    jj_address(COMMIT_KIND, id)
}

pub fn jj_change(id: impl Into<String>) -> EntityAddress<JjModel> {
    jj_address(CHANGE_KIND, id)
}

pub fn jj_workspace(id: impl Into<String>) -> EntityAddress<JjModel> {
    jj_address(WORKSPACE_KIND, id)
}

pub fn external(address: EntityAddress<JjModel>) -> EntityRef<JjModel> {
    EntityRef::External(address)
}

pub fn string_value(value: impl Into<String>) -> Value<JjModel> {
    Value::String(value.into().into_boxed_str())
}
