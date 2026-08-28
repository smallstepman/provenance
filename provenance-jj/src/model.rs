use provenance_core::{EntityAddress, EntityRef, PluginModel, Value};

/// JJ uses the canonical shared provenance graph model.
pub type JjModel = PluginModel;

pub const JJ_NAMESPACE: &str = "jj";
pub const OPERATION_KIND: &str = "operation";
pub const COMMIT_KIND: &str = "commit";
pub const CHANGE_KIND: &str = "change";
pub const WORKSPACE_KIND: &str = "workspace";

pub fn jj_address(kind: impl Into<String>, id: impl Into<String>) -> EntityAddress<JjModel> {
    EntityAddress::new(JJ_NAMESPACE, kind.into(), id.into())
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
