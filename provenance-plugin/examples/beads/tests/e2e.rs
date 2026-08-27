use std::{
    collections::BTreeSet, convert::Infallible, fs, path::PathBuf, process::Command, thread,
    time::Duration,
};

use provenance_core::{
    Attributes, DefaultRules, EntityAddress, EntityRef, ExplanationRole, FieldName, FinalizeAction,
    IdentityKind, IdentityScheme, Kernel, PrepareRequirement, ProvenanceModel, Query, Runtime,
    Service, State, Value as CoreValue,
};
use provenance_plugin::{PluginManager, WasmPluginAdapter, bindings};
use serde_json::Value;
use tempfile::TempDir;
use wit_component::ComponentEncoder;

struct BdFixture {
    _root: TempDir,
    repository: PathBuf,
    home: PathBuf,
}

impl BdFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary Beads fixture");
        let repository = root.path().join("repository");
        let home = root.path().join("home");
        fs::create_dir_all(&repository).expect("create Beads repository");
        fs::create_dir_all(&home).expect("create isolated HOME");

        let fixture = Self {
            _root: root,
            repository,
            home,
        };
        fixture.run_bd([
            "init",
            "--non-interactive",
            "--skip-agents",
            "--skip-hooks",
            "--prefix",
            "demo",
        ]);
        fixture.install_hooks();
        fixture
    }
    fn run_bd<const N: usize>(&self, args: [&str; N]) -> String {
        let _ = fs::remove_file(self.hook_capture());
        let _ = fs::remove_file(self.hook_event_path());
        let output = Command::new("bd")
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("XDG_DATA_HOME", self.home.join("data"))
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Beads E2E")
            .env("GIT_AUTHOR_EMAIL", "beads-e2e@example.invalid")
            .env("GIT_COMMITTER_NAME", "Beads E2E")
            .env("GIT_COMMITTER_EMAIL", "beads-e2e@example.invalid")
            .env("BEADS_HOOK_CAPTURE", self.hook_capture())
            .env("BEADS_HOOK_EVENT", self.hook_event_path())
            .current_dir(&self.repository)
            .args(args)
            .output()
            .expect("execute bd");
        assert!(
            output.status.success(),
            "bd {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("bd stdout is UTF-8")
    }

    fn json<const N: usize>(&self, args: [&str; N]) -> Value {
        let _ = fs::remove_file(self.hook_capture());
        let _ = fs::remove_file(self.hook_event_path());
        let mut command = vec!["--json"];
        command.extend(args);
        let output = Command::new("bd")
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("XDG_DATA_HOME", self.home.join("data"))
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Beads E2E")
            .env("GIT_AUTHOR_EMAIL", "beads-e2e@example.invalid")
            .env("GIT_COMMITTER_NAME", "Beads E2E")
            .env("GIT_COMMITTER_EMAIL", "beads-e2e@example.invalid")
            .env("BEADS_HOOK_CAPTURE", self.hook_capture())
            .env("BEADS_HOOK_EVENT", self.hook_event_path())
            .current_dir(&self.repository)
            .args(command)
            .output()
            .expect("execute bd JSON command");
        assert!(
            output.status.success(),
            "bd {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "bd {:?} returned invalid JSON: {error}\nstdout:\n{}",
                args,
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }

    fn hook_capture(&self) -> PathBuf {
        self._root.path().join("hook-payload.json")
    }

    fn hook_event_path(&self) -> PathBuf {
        self._root.path().join("hook-event")
    }

    fn hook_event_name(&self) -> String {
        for _ in 0..100 {
            if let Ok(event) = fs::read_to_string(self.hook_event_path())
                && !event.is_empty()
            {
                return event;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("Beads hook did not produce an event marker");
    }

    fn install_hooks(&self) {
        let hooks = self.repository.join(".beads/hooks");
        fs::create_dir_all(&hooks).expect("create Beads hook directory");
        for name in ["on_create", "on_update", "on_close"] {
            let hook = hooks.join(name);
            let script = format!(
                "#!/bin/sh\ncat > \"$BEADS_HOOK_CAPTURE\"\nprintf '%s' '{name}' > \"$BEADS_HOOK_EVENT\"\n"
            );
            fs::write(&hook, script).expect("write Beads lifecycle hook");
            let status = Command::new("chmod")
                .args(["+x", hook.to_str().expect("hook path is UTF-8")])
                .status()
                .expect("make Beads lifecycle hook executable");
            assert!(status.success(), "chmod Beads lifecycle hook failed");
        }
    }

    fn hook_snapshot(&self) -> Value {
        for _ in 0..100 {
            if let Ok(payload) = fs::read(self.hook_capture())
                && !payload.is_empty()
            {
                return serde_json::from_slice(&payload).unwrap_or_else(|error| {
                    panic!(
                        "Beads hook returned invalid JSON: {error}\npayload:\n{}",
                        String::from_utf8_lossy(&payload)
                    )
                });
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("Beads hook did not produce a payload");
    }

    fn create(&self, title: &str, description: &str) -> String {
        let result = self.json([
            "create",
            "--title",
            title,
            "--description",
            description,
            "--type",
            "task",
            "--priority",
            "1",
        ]);
        result["data"]["id"]
            .as_str()
            .expect("created issue id")
            .to_owned()
    }

    fn show(&self, issue_id: &str) -> Value {
        let result = self.json(["show", issue_id]);
        result["data"]
            .as_array()
            .and_then(|issues| issues.first())
            .cloned()
            .expect("shown issue")
    }

    fn dependency_ids(&self, issue_id: &str) -> Vec<String> {
        self.json(["dep", "list", issue_id])["data"]
            .as_array()
            .expect("dependency list")
            .iter()
            .filter_map(|dependency| dependency["id"].as_str().map(str::to_owned))
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TestIdentity;

impl IdentityScheme<ProvenanceModel> for TestIdentity {
    fn derive(&self, kind: IdentityKind, seed: &String, discriminator: &str) -> String {
        format!("{kind:?}:{seed}:{discriminator}")
    }
}

#[derive(Default)]
struct TestRuntime {
    published: usize,
}

impl Runtime<ProvenanceModel> for TestRuntime {
    type Error = Infallible;

    fn prepare(
        &mut self,
        _requirement: &PrepareRequirement<ProvenanceModel>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn publish(
        &mut self,
        _operation: &provenance_core::Operation<ProvenanceModel>,
    ) -> Result<(), Self::Error> {
        self.published += 1;
        Ok(())
    }

    fn finalize(&mut self, _action: &FinalizeAction<ProvenanceModel>) -> Result<(), Self::Error> {
        Ok(())
    }
}

type BeadsService =
    Service<ProvenanceModel, TestIdentity, DefaultRules, WasmPluginAdapter, TestRuntime>;

fn beads_component() -> Vec<u8> {
    let target = TempDir::new().expect("Beads plugin target directory");
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "-p",
            "provenance-plugin-beads",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--target-dir",
        ])
        .arg(target.path())
        .status()
        .expect("build Beads plugin");
    assert!(status.success(), "Beads plugin build failed");

    let module_path = target
        .path()
        .join("wasm32-unknown-unknown/release/provenance_plugin_beads.wasm");
    let module = fs::read(&module_path)
        .unwrap_or_else(|error| panic!("read Beads module {}: {error}", module_path.display()));
    ComponentEncoder::default()
        .module(&module)
        .expect("read wit-bindgen component metadata")
        .encode()
        .expect("encode Beads component")
}

// The host maps each hook's JSON issue snapshot to this stable request:
// `source = beads:hook:<event>/<issue-id>/<updated-at>`, `cursor = event`,
// and issue fields become typed attributes. Dependency IDs come from `bd dep
// list` so the plugin can emit relation subjects and edges from the same
// snapshot transaction.
fn snapshot_request(
    snapshot: &Value,
    event: &str,
    dependencies: &[String],
) -> bindings::ObservationRequest {
    let issue_id = string_field(snapshot, "id");
    let updated_at = string_field(snapshot, "updated_at");
    let source_id = format!("{event}/{issue_id}/{updated_at}");
    let mut attributes = Vec::new();

    for (json_name, plugin_name) in [
        ("id", "issue-id"),
        ("title", "title"),
        ("description", "description"),
        ("status", "status"),
        ("issue_type", "issue-type"),
        ("assignee", "assignee"),
        ("created_at", "created-at"),
        ("created_by", "created-by"),
        ("updated_at", "updated-at"),
        ("started_at", "started-at"),
        ("closed_at", "closed-at"),
        ("close_reason", "close-reason"),
        ("external_ref", "external-ref"),
        ("parent_id", "parent-id"),
    ] {
        if let Some(value) = snapshot.get(json_name).and_then(Value::as_str) {
            attributes.push(bindings::Attribute {
                name: plugin_name.into(),
                value: bindings::Value::StringValue(value.to_owned()),
            });
        }
    }

    attributes.push(bindings::Attribute {
        name: "priority".into(),
        value: bindings::Value::IntegerText(
            snapshot["priority"]
                .as_i64()
                .expect("issue priority")
                .to_string(),
        ),
    });
    if !dependencies.is_empty() {
        attributes.push(bindings::Attribute {
            name: "depends-on".into(),
            value: bindings::Value::ListValue(
                dependencies
                    .iter()
                    .cloned()
                    .map(bindings::ScalarValue::StringValue)
                    .collect(),
            ),
        });
    }

    bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: "beads".into(),
            kind: "hook".into(),
            id: source_id,
        },
        cursor: Some(event.into()),
        attributes,
    }
}

fn string_field<'a>(value: &'a Value, name: &str) -> &'a str {
    value[name]
        .as_str()
        .unwrap_or_else(|| panic!("Beads snapshot field `{name}` is a string"))
}

fn ingest(
    service: &mut BeadsService,
    manager: &PluginManager,
    request: bindings::ObservationRequest,
) {
    let transaction = manager
        .observe("beads", request)
        .expect("observe Beads plugin transaction");
    service
        .process_transaction(transaction)
        .expect("process Beads plugin transaction");
}

fn issue_address(issue_id: &str) -> EntityAddress<ProvenanceModel> {
    EntityAddress::new("beads", "issue", issue_id.to_owned())
}
fn value_string<'a>(attributes: &'a Attributes<ProvenanceModel>, name: &str) -> &'a str {
    attributes
        .get(&FieldName::from(name))
        .and_then(|value| match value {
            CoreValue::String(value) => Some(value.as_ref()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("provenance attribute `{name}` is a string"))
}

#[test]
fn bd_lifecycle_snapshots_enter_generic_provenance_and_replay_idempotently() {
    let fixture = BdFixture::new();
    let component = beads_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    let manifest = manager
        .register_component(&component)
        .expect("register Beads plugin");
    assert_eq!(manifest.id, "beads");
    assert_eq!(manifest.namespace, "beads");

    let adapter = manager.adapter("beads").expect("create Beads adapter");
    let mut service = BeadsService {
        kernel: Kernel::new(TestIdentity),
        adapter,
        runtime: TestRuntime::default(),
        state: State::new(),
    };

    let issue_id = fixture.create("Implement Beads integration", "Initial issue");
    let created = fixture.hook_snapshot();
    assert_eq!(fixture.hook_event_name(), "on_create");
    assert_eq!(string_field(&created, "id"), issue_id);
    assert_eq!(
        string_field(&created, "title"),
        "Implement Beads integration"
    );
    let create_request = snapshot_request(&created, "create", &[]);
    ingest(&mut service, &manager, create_request);
    assert_eq!(service.state.operations.len(), 1);
    assert_eq!(service.runtime.published, 1);
    assert!(service.state.has_source(&EntityAddress::new(
        "beads",
        "hook",
        format!(
            "create/{}/{}",
            issue_id,
            string_field(&created, "updated_at")
        ),
    )));

    fixture.run_bd([
        "update",
        &issue_id,
        "--status",
        "in_progress",
        "--assignee",
        "alice",
    ]);
    let updated = fixture.hook_snapshot();
    assert_eq!(fixture.hook_event_name(), "on_update");
    assert_eq!(string_field(&updated, "id"), issue_id);
    ingest(
        &mut service,
        &manager,
        snapshot_request(&updated, "update", &[]),
    );
    let issue = issue_address(&issue_id);
    let latest = service
        .state
        .projection
        .entity(&issue)
        .expect("latest Beads issue observation");
    assert_eq!(value_string(&latest.attributes, "status"), "in_progress");
    assert_eq!(value_string(&latest.attributes, "assignee"), "alice");
    assert_eq!(service.state.operations.len(), 2);

    fixture.run_bd(["close", &issue_id, "--reason", "implemented"]);
    let closed = fixture.hook_snapshot();
    assert_eq!(fixture.hook_event_name(), "on_close");
    assert_eq!(string_field(&closed, "id"), issue_id);
    ingest(
        &mut service,
        &manager,
        snapshot_request(&closed, "close", &[]),
    );
    let latest = service
        .state
        .projection
        .entity(&issue)
        .expect("closed Beads issue observation");
    assert_eq!(value_string(&latest.attributes, "status"), "closed");
    assert_eq!(
        value_string(&latest.attributes, "close_reason"),
        "implemented"
    );
    assert_eq!(service.state.operations.len(), 3);
    let state_before_replay = service.state.clone();
    ingest(
        &mut service,
        &manager,
        snapshot_request(&closed, "close", &[]),
    );
    assert_eq!(service.state, state_before_replay);
    assert_eq!(service.state.operations.len(), 3);
    assert_eq!(service.runtime.published, 3);

    let dependent_id = fixture.create("Dependent issue", "Needs the first issue");
    fixture.run_bd(["dep", "add", &dependent_id, &issue_id]);
    let dependent = fixture.show(&dependent_id);
    let dependencies = fixture.dependency_ids(&dependent_id);
    assert_eq!(dependencies, vec![issue_id.clone()]);
    ingest(
        &mut service,
        &manager,
        snapshot_request(&dependent, "create", &dependencies),
    );

    let dependent_ref = EntityRef::External(issue_address(&dependent_id));
    let issue_ref = EntityRef::External(issue_address(&issue_id));
    assert!(
        service
            .state
            .projection
            .outgoing
            .get(&dependent_ref)
            .is_some_and(|edges| {
                edges.iter().any(|edge| {
                    edge.relation.relation_type.name.as_str() == "depends-on"
                        && edge.relation.to == issue_ref
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
        .expect("explain dependency through generic query engine");
    assert!(explanation.entities.contains(&issue_ref));
    assert!(explanation.edges.iter().any(|edge| {
        edge.relation.relation_type.name.as_str() == "depends-on"
            && edge.relation.from == dependent_ref
            && edge.relation.to == issue_ref
    }));
    assert_eq!(service.state.operations.len(), 4);
}

#[test]
fn beads_plugin_rejects_unknown_observation_attributes() {
    let request = bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: "beads".into(),
            kind: "hook".into(),
            id: "create/demo-1/2026-08-27T00:00:00Z".into(),
        },
        cursor: Some("create".into()),
        attributes: vec![
            bindings::Attribute {
                name: "issue-id".into(),
                value: bindings::Value::StringValue("demo-1".into()),
            },
            bindings::Attribute {
                name: "title".into(),
                value: bindings::Value::StringValue("Reject unknown fields".into()),
            },
            bindings::Attribute {
                name: "status".into(),
                value: bindings::Value::StringValue("open".into()),
            },
            bindings::Attribute {
                name: "priority".into(),
                value: bindings::Value::IntegerText("1".into()),
            },
            bindings::Attribute {
                name: "issue-type".into(),
                value: bindings::Value::StringValue("task".into()),
            },
            bindings::Attribute {
                name: "created-at".into(),
                value: bindings::Value::StringValue("2026-08-27T00:00:00Z".into()),
            },
            bindings::Attribute {
                name: "updated-at".into(),
                value: bindings::Value::StringValue("2026-08-27T00:00:00Z".into()),
            },
            bindings::Attribute {
                name: "unknown-field".into(),
                value: bindings::Value::StringValue("unexpected".into()),
            },
        ],
    };

    let component = beads_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    manager
        .register_component(&component)
        .expect("register Beads plugin");
    let error = manager
        .observe("beads", request)
        .expect_err("unknown observation attributes must be rejected");
    assert!(matches!(
        error,
        provenance_plugin::PluginHostError::PluginReturned { kind, .. }
            if kind == "invalid-request"
    ));
}
