#![cfg(feature = "plugins")]

mod support;

use std::fs;
use std::process::Command;

use provenance_core::{EntityAddress, EntityRef};
use provenance_jj::{
    JjModel, JjPluginError, ingest_plugin_with_causal_parents, jj_commit_address, jj_operation,
    why_jj_commit,
};
use provenance_plugin::{PluginManager, bindings};
use tempfile::TempDir;
use wit_component::ComponentEncoder;

fn hello_component() -> Vec<u8> {
    let target = TempDir::new().expect("hello target directory");
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "-p",
            "provenance-plugin-hello",
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--target-dir",
        ])
        .arg(target.path())
        .status()
        .expect("build hello plugin");
    assert!(status.success(), "hello plugin build failed");

    let module_path = target
        .path()
        .join("wasm32-unknown-unknown/release/provenance_plugin_hello.wasm");
    let module = fs::read(&module_path)
        .unwrap_or_else(|error| panic!("read hello module {}: {error}", module_path.display()));
    ComponentEncoder::default()
        .module(&module)
        .expect("read wit-bindgen component metadata")
        .encode()
        .expect("encode hello component")
}

fn plugin_request(commit_id: &str) -> bindings::ObservationRequest {
    bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: "tracker".into(),
            kind: "poll".into(),
            id: "jj-commit".into(),
        },
        cursor: None,
        attributes: vec![bindings::Attribute {
            name: "current-commit".into(),
            value: bindings::Value::EntityValue(bindings::EntityRef::External(
                bindings::EntityAddress {
                    namespace: "jj".into(),
                    kind: "commit".into(),
                    id: commit_id.into(),
                },
            )),
        }],
    }
}

fn plugin_request_with_mode(commit_id: &str, mode: &str) -> bindings::ObservationRequest {
    let mut request = plugin_request(commit_id);
    request.attributes.push(bindings::Attribute {
        name: "test-mode".into(),
        value: bindings::Value::StringValue(mode.into()),
    });
    request
}

#[test]
fn plugin_transaction_joins_jj_history_and_explanation_graph() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "plugin integration\n");
    fixture.commit("plugin integration");

    let repository = fixture.repository();
    let jj_operation_id = repository.operation_id();
    let jj_source = jj_operation(jj_operation_id.clone());
    let commit_id = fixture.head_commit();
    let commit_address = jj_commit_address(commit_id.clone());
    let mut service = fixture.service();
    fixture.ingest(&mut service);

    let jj_operation = service
        .state
        .operation_for_source(&jj_source)
        .expect("ingested JJ operation source")
        .clone();
    let published_before = service
        .runtime
        .published_len()
        .expect("published operation count");
    let operations_before = service.state.operations.len();

    let component = hello_component();
    let mut plugins = PluginManager::new().expect("create plugin manager");
    plugins
        .register_component(&component)
        .expect("register hello component");

    ingest_plugin_with_causal_parents(
        &mut service,
        &plugins,
        "hello-tracker",
        plugin_request(&commit_id),
        [jj_source.clone()],
    )
    .expect("ingest plugin transaction");

    assert_eq!(service.state.operations.len(), operations_before + 1);
    assert_eq!(
        service
            .runtime
            .published_len()
            .expect("published operation count"),
        published_before + 1
    );

    let plugin_source: EntityAddress<JjModel> =
        EntityAddress::new("tracker", "poll", "jj-commit".to_owned());
    let plugin_operation_id = service
        .state
        .operation_for_source(&plugin_source)
        .expect("plugin source anchor")
        .clone();
    let plugin_operation = service
        .state
        .operation(&plugin_operation_id)
        .expect("plugin operation");
    assert!(plugin_operation.parents.contains(&jj_operation));

    let task: EntityRef<JjModel> =
        EntityRef::External(EntityAddress::new("tracker", "task", "123".to_owned()));
    let commit = EntityRef::External(commit_address);
    assert!(
        service
            .state
            .projection
            .outgoing
            .get(&task)
            .is_some_and(|edges| {
                edges.iter().any(|edge| {
                    edge.relation.relation_type.name.as_str() == "implemented-by"
                        && edge.relation.to == commit
                })
            })
    );

    let explanation = service
        .state
        .query(&why_jj_commit(&commit_id))
        .expect("explain JJ commit through plugin relation");
    assert!(explanation.entities.contains(&task));

    let state_after_first = service.state.clone();
    let published_after_first = service
        .runtime
        .published_len()
        .expect("published operation count");
    ingest_plugin_with_causal_parents(
        &mut service,
        &plugins,
        "hello-tracker",
        plugin_request(&commit_id),
        [jj_source],
    )
    .expect("replay plugin transaction");
    assert_eq!(service.state, state_after_first);
    assert_eq!(
        service
            .runtime
            .published_len()
            .expect("published operation count"),
        published_after_first
    );
}

#[test]
fn jj_plugin_ingestion_requires_source_anchor() {
    let fixture = support::Fixture::new();
    let mut service = fixture.service();
    let component = hello_component();
    let mut plugins = PluginManager::new().expect("create plugin manager");
    plugins
        .register_component(&component)
        .expect("register hello component");

    let error = ingest_plugin_with_causal_parents(
        &mut service,
        &plugins,
        "hello-tracker",
        plugin_request_with_mode("commit", "empty-source"),
        std::iter::empty(),
    )
    .expect_err("JJ plugin ingestion must require a source anchor");

    assert!(matches!(error, JjPluginError::MissingSourceAnchor));
    assert!(service.state.operations.is_empty());
}
