use std::{
    collections::BTreeSet,
    convert::Infallible,
    env, fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output},
    thread,
    time::Duration,
};

use provenance_core::{
    Attributes, DefaultRules, EntityAddress, EntityRef, ExplanationRole, FieldName, FinalizeAction,
    IdentityKind, IdentityScheme, Kernel, Query, Runtime, Service, State, Value as CoreValue,
};
use provenance_plugin::{PluginManager, WasmPluginAdapter, bindings};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use wit_component::ComponentEncoder;

struct DgFixture {
    _root: TempDir,
    project: PathBuf,
    home: PathBuf,
    dg: PathBuf,
}

impl DgFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary dg fixture");
        let project = root.path().join("project");
        let home = root.path().join("home");
        fs::create_dir_all(&project).expect("create dg project");
        fs::create_dir_all(&home).expect("create isolated HOME");

        let dg = env::var_os("DG_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("dg"));
        let fixture = Self {
            _root: root,
            project,
            home,
            dg,
        };
        fixture.run_dg(&["init"]);
        fixture.install_hooks();
        fixture
    }

    fn command(&self, args: &[&str]) -> Output {
        Command::new(&self.dg)
            .arg("--root")
            .arg(&self.project)
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("XDG_DATA_HOME", self.home.join("data"))
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "dg E2E")
            .env("GIT_AUTHOR_EMAIL", "dg-e2e@example.invalid")
            .env("GIT_COMMITTER_NAME", "dg E2E")
            .env("GIT_COMMITTER_EMAIL", "dg-e2e@example.invalid")
            .current_dir(&self.project)
            .args(args)
            .output()
            .expect("execute dg")
    }

    fn run_dg(&self, args: &[&str]) -> String {
        let output = self.command(args);
        assert!(
            output.status.success(),
            "dg {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("dg stdout is UTF-8")
    }

    fn run_mutation(&self, args: &[&str]) -> String {
        self.clear_captures();
        self.run_dg(args)
    }

    fn install_hooks(&self) {
        let hooks = self.project.join(".dg/hooks");
        fs::create_dir_all(&hooks).expect("create dg hook directory");
        for event in ["create", "update", "delete"] {
            let capture = self.capture_path(event);
            let capture = capture.to_str().expect("capture path is UTF-8");
            let script = format!(
                "#!/bin/sh\nprintf '%s\\n%s\\n' \"$1\" \"$2\" > '{capture}'\ncat >> '{capture}'\n"
            );
            let hook = hooks.join(format!("on_{event}"));
            fs::write(&hook, script).expect("write dg lifecycle hook");
            let mut permissions = fs::metadata(&hook)
                .expect("stat dg lifecycle hook")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&hook, permissions).expect("make dg lifecycle hook executable");
        }
    }

    fn capture_path(&self, event: &str) -> PathBuf {
        self._root.path().join(format!("{event}-hook"))
    }

    fn clear_captures(&self) {
        for event in ["create", "update", "delete"] {
            let _ = fs::remove_file(self.capture_path(event));
        }
    }

    fn hook_record(&self, event: &str) -> (String, String, String, Value) {
        let path = self.capture_path(event);
        for _ in 0..100 {
            if let Ok(record) = fs::read_to_string(&path) {
                let mut lines = record.splitn(3, '\n');
                let id = lines.next().unwrap_or_default().to_owned();
                let observed_event = lines.next().unwrap_or_default().to_owned();
                if let Some(raw) = lines.next().filter(|raw| !raw.is_empty()) {
                    let payload = serde_json::from_str(raw).unwrap_or_else(|error| {
                        panic!("dg {event} hook returned invalid JSON: {error}\npayload:\n{raw}")
                    });
                    return (id, observed_event, raw.to_owned(), payload);
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("dg {event} hook did not produce a payload");
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TestIdentity;

impl IdentityScheme<provenance_core::ProvenanceModel> for TestIdentity {
    fn derive(&self, kind: IdentityKind, seed: &String, discriminator: &str) -> String {
        format!("{kind:?}:{seed}:{discriminator}")
    }
}

#[derive(Default)]
struct TestRuntime {
    published: usize,
}

impl Runtime<provenance_core::ProvenanceModel> for TestRuntime {
    type Error = Infallible;

    fn prepare(
        &mut self,
        _requirement: &provenance_core::PrepareRequirement<provenance_core::ProvenanceModel>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn publish(
        &mut self,
        _operation: &provenance_core::Operation<provenance_core::ProvenanceModel>,
    ) -> Result<(), Self::Error> {
        self.published += 1;
        Ok(())
    }

    fn finalize(
        &mut self,
        _action: &FinalizeAction<provenance_core::ProvenanceModel>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

type DgService = Service<
    provenance_core::ProvenanceModel,
    TestIdentity,
    DefaultRules,
    WasmPluginAdapter,
    TestRuntime,
>;

fn dg_component() -> Vec<u8> {
    let target = TempDir::new().expect("dg plugin target directory");
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "-p",
            "provenance-plugin-dg",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--target-dir",
        ])
        .arg(target.path())
        .status()
        .expect("build dg plugin");
    assert!(status.success(), "dg plugin build failed");

    let module_path = target
        .path()
        .join("wasm32-unknown-unknown/release/provenance_plugin_dg.wasm");
    let module = fs::read(&module_path)
        .unwrap_or_else(|error| panic!("read dg module {}: {error}", module_path.display()));
    ComponentEncoder::default()
        .module(&module)
        .expect("read wit-bindgen component metadata")
        .encode()
        .expect("encode dg component")
}

fn snapshot_request(
    id: &str,
    event: &str,
    raw_payload: &str,
    payload: &Value,
) -> bindings::ObservationRequest {
    let current = if event == "update" {
        &payload["after"]
    } else {
        payload
    };
    let frontmatter = current
        .get("frontmatter")
        .and_then(Value::as_object)
        .expect("dg document frontmatter object");
    let path = string_field(current, "path");
    let body = string_field(current, "body");
    let document_type = id
        .split_once('-')
        .map(|(prefix, _)| prefix.to_ascii_lowercase())
        .unwrap_or_else(|| "document".into());
    let revision = format!("{:x}", Sha256::digest(raw_payload.as_bytes()));
    let source_id = format!("{event}/{id}/{revision}");

    let mut attributes = vec![
        string_attribute("document-id", id),
        string_attribute("document-type", document_type),
        string_attribute("path", path),
        string_attribute("body", body),
        string_attribute("frontmatter-json", json_text(&current["frontmatter"])),
        string_attribute("sections-json", json_text(&current["sections"])),
        string_attribute("payload-json", raw_payload),
        string_attribute("revision", revision),
    ];
    if let Some(title) = current
        .get("sections")
        .and_then(Value::as_array)
        .and_then(|sections| sections.first())
        .and_then(|section| section.get("heading"))
        .and_then(Value::as_str)
    {
        attributes.push(string_attribute("title", title));
    }

    for (json_name, plugin_name) in [("status", "status"), ("author", "author"), ("date", "date")] {
        if let Some(value) = frontmatter.get(json_name).and_then(Value::as_str) {
            attributes.push(string_attribute(plugin_name, value));
        }
    }
    if let Some(tags) = frontmatter.get("tags") {
        attributes.push(string_or_list_attribute("tags", tags));
    }
    attributes.push(bindings::Attribute {
        name: "frontmatter".into(),
        value: bindings::Value::MapValue(frontmatter_map(frontmatter)),
    });

    for (json_name, plugin_name) in [
        ("supersedes", "supersedes"),
        ("enables", "enables"),
        ("triggers", "triggers"),
        ("depends_on", "depends-on"),
        ("implements", "implements"),
        ("conflicts_with", "conflicts-with"),
        ("related", "related"),
    ] {
        if let Some(value) = frontmatter.get(json_name) {
            attributes.push(string_or_list_attribute(plugin_name, value));
        }
    }

    if event == "update" {
        attributes.push(string_attribute(
            "before-json",
            json_text(&payload["before"]),
        ));
        attributes.push(string_attribute("after-json", json_text(&payload["after"])));
        attributes.push(string_attribute("diff-json", json_text(&payload["diff"])));
    }

    bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: "dg".into(),
            kind: "hook".into(),
            id: source_id,
        },
        cursor: Some(event.into()),
        attributes,
    }
}

fn frontmatter_map(frontmatter: &serde_json::Map<String, Value>) -> Vec<bindings::ScalarAttribute> {
    frontmatter
        .iter()
        .filter_map(|(name, value)| {
            let value = match value {
                Value::Null => bindings::ScalarValue::NullValue,
                Value::Bool(value) => bindings::ScalarValue::Boolean(*value),
                Value::Number(value) if value.is_i64() => {
                    bindings::ScalarValue::IntegerText(value.to_string())
                }
                Value::Number(value) => bindings::ScalarValue::DecimalText(value.to_string()),
                Value::String(value) => bindings::ScalarValue::StringValue(value.clone()),
                Value::Array(_) | Value::Object(_) => return None,
            };
            Some(bindings::ScalarAttribute {
                name: name.clone(),
                value,
            })
        })
        .collect()
}

fn string_or_list_attribute(name: &str, value: &Value) -> bindings::Attribute {
    let value = match value {
        Value::String(value) => bindings::Value::StringValue(value.clone()),
        Value::Array(values) => bindings::Value::ListValue(
            values
                .iter()
                .map(|value| {
                    bindings::ScalarValue::StringValue(
                        value
                            .as_str()
                            .expect("dg relation values are strings")
                            .to_owned(),
                    )
                })
                .collect(),
        ),
        _ => panic!("dg `{name}` field must be a string or string list"),
    };
    bindings::Attribute {
        name: name.into(),
        value,
    }
}

fn string_attribute(name: &str, value: impl Into<String>) -> bindings::Attribute {
    bindings::Attribute {
        name: name.into(),
        value: bindings::Value::StringValue(value.into()),
    }
}

fn json_text(value: &Value) -> String {
    serde_json::to_string(value).expect("dg hook JSON is serializable")
}

fn string_field<'a>(value: &'a Value, name: &str) -> &'a str {
    value[name]
        .as_str()
        .unwrap_or_else(|| panic!("dg document field `{name}` is a string"))
}

fn ingest(service: &mut DgService, manager: &PluginManager, request: bindings::ObservationRequest) {
    let transaction = manager
        .observe("dg", request)
        .expect("observe dg plugin transaction");
    service
        .process_transaction(transaction)
        .expect("process dg plugin transaction");
}

fn document_address(document_id: &str) -> EntityAddress<provenance_core::ProvenanceModel> {
    EntityAddress::new("dg", "document", document_id.to_owned())
}

fn value_string<'a>(
    attributes: &'a Attributes<provenance_core::ProvenanceModel>,
    name: &str,
) -> &'a str {
    attributes
        .get(&FieldName::from(name))
        .and_then(|value| match value {
            CoreValue::String(value) => Some(value.as_ref()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("provenance attribute `{name}` is a string"))
}

fn value_bool(attributes: &Attributes<provenance_core::ProvenanceModel>, name: &str) -> bool {
    attributes
        .get(&FieldName::from(name))
        .and_then(|value| match value {
            CoreValue::Bool(value) => Some(*value),
            _ => None,
        })
        .unwrap_or_else(|| panic!("provenance attribute `{name}` is a bool"))
}

#[test]
fn dg_lifecycle_snapshots_enter_generic_provenance_and_replay_idempotently() {
    let fixture = DgFixture::new();
    let component = dg_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    let manifest = manager
        .register_component(&component)
        .expect("register dg plugin");
    assert_eq!(manifest.id, "dg");
    assert_eq!(manifest.namespace, "dg");

    let adapter = manager.adapter("dg").expect("create dg adapter");
    let mut service = DgService {
        kernel: Kernel::new(TestIdentity),
        adapter,
        runtime: TestRuntime::default(),
        state: State::new(),
    };

    fixture.run_mutation(&["new", "adr", "Record a document decision"]);
    let (id, event, raw, created) = fixture.hook_record("create");
    assert_eq!(id, "ADR-001");
    assert_eq!(event, "create");
    assert_eq!(string_field(&created, "path").contains("adr-001"), true);
    assert_eq!(created["frontmatter"]["status"], "proposed");
    assert!(string_field(&created, "body").contains("# Record a document decision"));
    ingest(
        &mut service,
        &manager,
        snapshot_request(&id, &event, &raw, &created),
    );
    assert_eq!(service.state.operations.len(), 1);
    assert_eq!(service.runtime.published, 1);
    assert!(service.state.has_source(&EntityAddress::new(
        "dg",
        "hook",
        format!("create/{id}/{:x}", Sha256::digest(raw.as_bytes())),
    )));

    fixture.run_mutation(&["set", "ADR-001", "status=accepted"]);
    let (id, event, raw, updated) = fixture.hook_record("update");
    assert_eq!(id, "ADR-001");
    assert_eq!(event, "update");
    assert_eq!(updated["before"]["frontmatter"]["status"], "proposed");
    assert_eq!(updated["after"]["frontmatter"]["status"], "accepted");
    assert_eq!(updated["diff"]["id"], "ADR-001");
    ingest(
        &mut service,
        &manager,
        snapshot_request(&id, &event, &raw, &updated),
    );
    let document = document_address(&id);
    let latest = service
        .state
        .projection
        .entity(&document)
        .expect("latest dg document observation");
    assert_eq!(value_string(&latest.attributes, "status"), "accepted");
    assert!(!value_bool(&latest.attributes, "deleted"));
    assert_eq!(service.state.operations.len(), 2);

    let state_before_replay = service.state.clone();
    ingest(
        &mut service,
        &manager,
        snapshot_request(&id, &event, &raw, &updated),
    );
    assert_eq!(service.state, state_before_replay);
    assert_eq!(service.state.operations.len(), 2);
    assert_eq!(service.runtime.published, 2);

    fixture.run_mutation(&[
        "new",
        "spec",
        "Implement the decision",
        "implements",
        "ADR-001",
    ]);
    let (dependent_id, event, raw, dependent) = fixture.hook_record("create");
    assert_eq!(dependent_id, "SPEC-001");
    ingest(
        &mut service,
        &manager,
        snapshot_request(&dependent_id, &event, &raw, &dependent),
    );
    let dependent_ref = EntityRef::External(document_address(&dependent_id));
    let document_ref = EntityRef::External(document_address("ADR-001"));
    assert!(
        service
            .state
            .projection
            .outgoing
            .get(&dependent_ref)
            .is_some_and(|edges| {
                edges.iter().any(|edge| {
                    edge.relation.relation_type.name.as_str() == "implements"
                        && edge.relation.to == document_ref
                })
            })
    );
    let explanation = service
        .state
        .query(&Query::Explain {
            root: dependent_ref.clone(),
            max_depth: Some(1),
            roles: BTreeSet::from([ExplanationRole::Supporting]),
        })
        .expect("explain dg implementation relation");
    assert!(explanation.entities.contains(&document_ref));
    assert!(explanation.edges.iter().any(|edge| {
        edge.relation.relation_type.name.as_str() == "implements"
            && edge.relation.from == dependent_ref
            && edge.relation.to == document_ref
    }));
    assert_eq!(service.state.operations.len(), 3);

    fixture.run_mutation(&["delete", "SPEC-001"]);
    let (deleted_id, event, raw, deleted) = fixture.hook_record("delete");
    assert_eq!(deleted_id, "SPEC-001");
    assert_eq!(event, "delete");
    ingest(
        &mut service,
        &manager,
        snapshot_request(&deleted_id, &event, &raw, &deleted),
    );
    let latest = service
        .state
        .projection
        .entity(&document_address(&deleted_id))
        .expect("deleted dg document observation");
    assert!(value_bool(&latest.attributes, "deleted"));
    assert_eq!(service.state.operations.len(), 4);
}

#[test]
fn dg_plugin_rejects_unknown_and_malformed_observation_attributes() {
    let component = dg_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    manager
        .register_component(&component)
        .expect("register dg plugin");

    let request = bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: "dg".into(),
            kind: "hook".into(),
            id: "create/ADR-001/revision".into(),
        },
        cursor: Some("create".into()),
        attributes: vec![
            string_attribute("document-id", "ADR-001"),
            bindings::Attribute {
                name: "unknown-field".into(),
                value: bindings::Value::StringValue("unexpected".into()),
            },
        ],
    };
    let error = manager
        .observe("dg", request)
        .expect_err("unknown observation attributes must be rejected");
    assert!(matches!(
        error,
        provenance_plugin::PluginHostError::PluginReturned { kind, .. }
            if kind == "invalid-request"
    ));

    let request = bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: "dg".into(),
            kind: "hook".into(),
            id: "create/ADR-001/revision".into(),
        },
        cursor: Some("create".into()),
        attributes: vec![
            string_attribute("document-id", "ADR-001"),
            bindings::Attribute {
                name: "tags".into(),
                value: bindings::Value::Boolean(true),
            },
        ],
    };
    let error = manager
        .observe("dg", request)
        .expect_err("malformed list attribute must be rejected");
    assert!(matches!(
        error,
        provenance_plugin::PluginHostError::PluginReturned { kind, .. }
            if kind == "invalid-request"
    ));
}
