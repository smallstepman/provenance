use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use nemo_relay::api::scope::{PopScopeParams, PushScopeParams, ScopeType, pop_scope, push_scope};
use nemo_relay::api::subscriber::flush_subscribers;
use nemo_relay::plugin::dynamic::{NativePluginLoadSpec, load_native_plugins};
use nemo_relay::plugin::{
    ConfigReport, PluginComponentSpec, PluginConfig, clear_plugin_configuration,
    initialize_plugins_exact,
};
use provenance_data_model::{FieldName, Value};
use provenance_nemo_relay::{DirectoryRuntime, durable_heads, spool_size};
use serde_json::{Map, Value as Json, json};
use tempfile::{TempDir, tempdir};
use wit_component::ComponentEncoder;

const RELAY_VERSION: &str = "0.8.1";

struct BuiltArtifacts {
    _target: TempDir,
    component: PathBuf,
    library: PathBuf,
}

struct PluginCleanup {
    activation: Option<nemo_relay::plugin::dynamic::NativePluginActivation>,
    configuration_active: bool,
}

impl PluginCleanup {
    fn new(activation: nemo_relay::plugin::dynamic::NativePluginActivation) -> Self {
        Self {
            activation: Some(activation),
            configuration_active: false,
        }
    }

    fn mark_configuration_active(&mut self) {
        self.configuration_active = true;
    }
}

impl Drop for PluginCleanup {
    fn drop(&mut self) {
        if self.configuration_active {
            let _ = flush_subscribers();
            let _ = clear_plugin_configuration();
        }
        if let Some(activation) = self.activation.take() {
            activation.clear();
        }
    }
}

#[test]
fn native_relay_plugin_subscriber_persists_session_actor_and_evidence() {
    let artifacts = build_artifacts();
    let workspace = tempdir().expect("temporary provenance workspace");
    let store = workspace.path().join("provenance");
    let spool = workspace.path().join("atof/events.jsonl");
    let manifest_dir = tempdir().expect("temporary plugin manifest directory");
    let manifest = write_manifest(manifest_dir.path(), &artifacts.library);

    let activation = load_native_plugins([NativePluginLoadSpec {
        plugin_id: "provenance".into(),
        manifest_ref: manifest.to_string_lossy().into_owned(),
    }])
    .expect("load provenance native plugin");
    let mut cleanup = PluginCleanup::new(activation);

    let config = Map::from_iter([
        ("harness_id".into(), Json::String("omp".into())),
        ("installation_id".into(), Json::String("native-test".into())),
        (
            "agent_plugin_path".into(),
            Json::String(artifacts.component.to_string_lossy().into_owned()),
        ),
        (
            "store_path".into(),
            Json::String(store.to_string_lossy().into_owned()),
        ),
        (
            "spool_path".into(),
            Json::String(spool.to_string_lossy().into_owned()),
        ),
        ("telemetry_provider".into(), Json::String("phoenix".into())),
        (
            "telemetry_links".into(),
            Json::Object(
                json!({
                    "trace_ui_template": "http://127.0.0.1:6006/redirects/traces/{trace_id}",
                    "trace_api_template": "http://127.0.0.1:6006/v1/projects/omp/spans?trace_id={trace_id}",
                    "span_ui_template": "http://127.0.0.1:6006/redirects/spans/{span_id}",
                    "span_api_template": "http://127.0.0.1:6006/v1/projects/omp/spans?span_id={span_id}"
                })
                .as_object()
                .expect("telemetry link object")
                .clone(),
            ),
        ),
    ]);
    let mut plugin_config = PluginConfig::default();
    plugin_config.components.push(PluginComponentSpec {
        kind: "provenance".into(),
        enabled: true,
        config,
    });
    let ConfigReport { diagnostics, .. } =
        pollster::block_on(initialize_plugins_exact(plugin_config))
            .expect("initialize provenance native plugin");
    assert!(
        diagnostics.is_empty(),
        "unexpected diagnostics: {diagnostics:?}"
    );
    cleanup.mark_configuration_active();

    let root = push_scope(
        PushScopeParams::builder()
            .name("agent")
            .scope_type(ScopeType::Agent)
            .metadata(json!({"session_id":"session-42", "actor_id":"operator"}))
            .input(json!({"prompt":"not persisted in the graph"}))
            .build(),
    )
    .expect("push root agent scope");
    let child = push_scope(
        PushScopeParams::builder()
            .name("tool")
            .scope_type(ScopeType::Tool)
            .metadata(json!({"trace_id":"trace-1", "span_id":"span-1", "provider":"local"}))
            .input(json!({"command":"git status"}))
            .build(),
    )
    .expect("push child tool scope");
    pop_scope(
        PopScopeParams::builder()
            .handle_uuid(&child.uuid)
            .output(json!({"exit_code":0}))
            .metadata(json!({
                "provenance": {
                    "source": {
                        "namespace": "jj",
                        "kind": "operation",
                        "id": "abc123"
                    },
                    "outcome": "committed",
                    "trace_id": "trace-7",
                    "span_id": "span-99",
                    "provider": "phoenix"
                }
            }))
            .build(),
    )
    .expect("pop child tool scope");
    pop_scope(
        PopScopeParams::builder()
            .handle_uuid(&root.uuid)
            .output(json!({"status":"success"}))
            .build(),
    )
    .expect("pop root agent scope");
    flush_subscribers().expect("flush native provenance subscriber");

    let spool_contents = fs::read_to_string(&spool).unwrap_or_default();
    let state = DirectoryRuntime::open(&store)
        .expect("open persisted provenance store")
        .load_state()
        .expect("load persisted provenance state");
    assert_eq!(
        state.projection.events.len(),
        2,
        "native subscriber should persist one source link and one session boundary; spool contents: {spool_contents}"
    );
    assert_eq!(state.projection.sessions.len(), 1);
    assert_eq!(state.projection.actors.len(), 1);
    assert_eq!(state.projection.ended_sessions.len(), 1);
    let source_event = state
        .projection
        .events
        .values()
        .find(|event| {
            event.subjects.iter().any(|subject| {
                matches!(
                    subject,
                    provenance_core::EntityRef::External(address)
                        if address.namespace.as_str() == "jj"
                            && address.kind.as_str() == "operation"
                            && address.id.as_str() == "abc123"
                )
            })
        })
        .expect("persisted source event");
    assert!(matches!(
        source_event
            .attributes
            .get(&FieldName::from("telemetry-provider")),
        Some(Value::String(value)) if value.as_ref() == "phoenix"
    ));
    assert!(matches!(
        source_event
            .attributes
            .get(&FieldName::from("telemetry-trace-id")),
        Some(Value::String(value)) if !value.is_empty()
    ));
    assert!(matches!(
        source_event
            .attributes
            .get(&FieldName::from("telemetry-span-id")),
        Some(Value::String(value)) if !value.is_empty()
    ));
    let trace_id = match source_event
        .attributes
        .get(&FieldName::from("telemetry-trace-id"))
    {
        Some(Value::String(value)) => value.to_string(),
        other => panic!("missing persisted telemetry trace ID: {other:?}"),
    };
    let span_id = match source_event
        .attributes
        .get(&FieldName::from("telemetry-span-id"))
    {
        Some(Value::String(value)) => value.to_string(),
        other => panic!("missing persisted telemetry span ID: {other:?}"),
    };
    assert!(matches!(
        source_event
            .attributes
            .get(&FieldName::from("telemetry-trace-ui-url")),
        Some(Value::String(value))
            if value.as_ref() == format!("http://127.0.0.1:6006/redirects/traces/{trace_id}")
    ));
    assert!(matches!(
        source_event
            .attributes
            .get(&FieldName::from("telemetry-trace-api-url")),
        Some(Value::String(value))
            if value.as_ref()
                == format!(
                    "http://127.0.0.1:6006/v1/projects/omp/spans?trace_id={trace_id}"
                )
    ));
    assert!(matches!(
        source_event
            .attributes
            .get(&FieldName::from("telemetry-span-ui-url")),
        Some(Value::String(value))
            if value.as_ref() == format!("http://127.0.0.1:6006/redirects/spans/{span_id}")
    ));
    assert!(matches!(
        source_event
            .attributes
            .get(&FieldName::from("telemetry-span-api-url")),
        Some(Value::String(value))
            if value.as_ref()
                == format!("http://127.0.0.1:6006/v1/projects/omp/spans?span_id={span_id}")
    ));
    assert_eq!(durable_heads(&store).expect("read durable heads").len(), 1);
    assert!(spool_size(&spool).expect("read evidence spool") > 0);
}

fn build_artifacts() -> BuiltArtifacts {
    let target = TempDir::new().expect("temporary Cargo target directory");
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "--manifest-path",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../provenance-plugin/examples/agent/Cargo.toml"
            ),
            "--package",
            "provenance-agent-plugin",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--target-dir",
        ])
        .arg(target.path())
        .status()
        .expect("build agent projection plugin");
    assert!(status.success(), "agent projection plugin build failed");
    let module_path = target
        .path()
        .join("wasm32-unknown-unknown/release/provenance_agent_plugin.wasm");
    let module = fs::read(&module_path)
        .unwrap_or_else(|error| panic!("read agent module {}: {error}", module_path.display()));
    let component = ComponentEncoder::default()
        .module(&module)
        .expect("read wit-bindgen component metadata")
        .encode()
        .expect("encode agent component");
    let component_path = target.path().join("agent.wasm");
    fs::write(&component_path, component).expect("write agent component");

    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "--manifest-path",
            concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"),
            "--package",
            "provenance-nemo-relay",
            "--lib",
            "--release",
            "--target-dir",
        ])
        .arg(target.path())
        .status()
        .expect("build provenance native plugin");
    assert!(status.success(), "provenance native plugin build failed");
    let library = target.path().join("release").join(format!(
        "{}provenance_nemo_relay{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    assert!(
        library.exists(),
        "native plugin library is missing: {}",
        library.display()
    );

    BuiltArtifacts {
        _target: target,
        component: component_path,
        library,
    }
}

fn write_manifest(directory: &Path, library: &Path) -> PathBuf {
    let quote = |value: &str| serde_json::to_string(value).expect("encode TOML string");
    let manifest = format!(
        "manifest_version = 1\n\n[plugin]\nid = \"provenance\"\nkind = \"rust_dynamic\"\n\n[compat]\nrelay = {}\nnative_api = \"1\"\n\n[defaults]\nenabled = false\n\n[capabilities]\nitems = [\"plugin_native\"]\n\n[load]\nlibrary = {}\nsymbol = \"provenance_relay_plugin\"\n",
        quote(RELAY_VERSION),
        quote(&library.to_string_lossy()),
    );
    let path = directory.join("relay-plugin.toml");
    fs::write(&path, manifest).expect("write native plugin manifest");
    path
}
