use std::fs;
use std::process::Command;

use provenance_core::{
    Adapter, FinalizeAction, IdentityKind, IdentityScheme, Kernel, PrepareRequirement, Runtime,
    Service, State,
};
use provenance_plugin::{PluginManager, PluginModel, bindings};
use tempfile::TempDir;
use wit_component::ComponentEncoder;

#[derive(Clone, Copy, Debug, Default)]
struct TestIdentity;

impl IdentityScheme<PluginModel> for TestIdentity {
    fn derive(&self, kind: IdentityKind, seed: &String, discriminator: &str) -> String {
        format!("{kind:?}:{seed}:{discriminator}")
    }
}

#[derive(Default)]
struct NoopRuntime {
    published: usize,
}

impl Runtime<PluginModel> for NoopRuntime {
    type Error = std::convert::Infallible;

    fn prepare(
        &mut self,
        _requirement: &PrepareRequirement<PluginModel>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn publish(
        &mut self,
        _operation: &provenance_core::Operation<PluginModel>,
    ) -> Result<(), Self::Error> {
        self.published += 1;
        Ok(())
    }

    fn finalize(&mut self, _action: &FinalizeAction<PluginModel>) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn hello_request() -> bindings::ObservationRequest {
    bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: "tracker".into(),
            kind: "poll".into(),
            id: "hello".into(),
        },
        cursor: None,
        attributes: Vec::new(),
    }
}
fn hello_request_with_mode(mode: &str) -> bindings::ObservationRequest {
    let mut request = hello_request();
    request.attributes.push(bindings::Attribute {
        name: "test-mode".into(),
        value: bindings::Value::StringValue(mode.into()),
    });
    request
}

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
    let mut encoder = ComponentEncoder::default()
        .module(&module)
        .expect("read wit-bindgen component metadata");
    encoder.encode().expect("encode hello component")
}

#[test]
fn manager_registers_and_bridges_real_wasm_plugin() {
    let component = hello_component();
    let mut manager = PluginManager::new().expect("create manager");
    let manifest = manager
        .register_component(&component)
        .expect("register hello component");

    assert_eq!(manifest.id, "hello-tracker");
    assert_eq!(manifest.namespace, "tracker");
    assert_eq!(manager.plugin_ids().collect::<Vec<_>>(), ["hello-tracker"]);

    let adapter = manager.adapter("hello-tracker").expect("create adapter");
    let mut service = Service {
        kernel: Kernel::new(TestIdentity),
        adapter,
        runtime: NoopRuntime::default(),
        state: State::new(),
    };

    service
        .process(hello_request())
        .expect("process hello transaction");
    service
        .process(hello_request())
        .expect("replay hello transaction");

    assert_eq!(service.state.operations.len(), 1);
    assert_eq!(service.runtime.published, 1);
    assert_eq!(service.state.projection.entities.iter().count(), 2);
    assert_eq!(service.state.projection.events.len(), 1);
    assert_eq!(
        service
            .state
            .projection
            .outgoing
            .iter()
            .map(|(_, edges)| edges.len())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        service.state.projection.source_to_operation.iter().count(),
        1
    );
}

#[test]
fn distinct_plugin_sources_remain_distinct() {
    let component = hello_component();
    let mut manager = PluginManager::new().expect("create manager");
    manager
        .register_component(&component)
        .expect("register hello component");
    let adapter = manager.adapter("hello-tracker").expect("create adapter");
    let mut service = Service {
        kernel: Kernel::new(TestIdentity),
        adapter,
        runtime: NoopRuntime::default(),
        state: State::new(),
    };

    service
        .process(hello_request())
        .expect("process first hello transaction");
    let mut second_request = hello_request();
    second_request.source.id = "second".into();
    service
        .process(second_request)
        .expect("process second hello transaction");

    assert_eq!(service.state.operations.len(), 2);
    assert_eq!(service.runtime.published, 2);
    assert_eq!(
        service.state.projection.source_to_operation.iter().count(),
        2
    );
}

#[test]
fn manager_rejects_duplicate_plugin_id() {
    let component = hello_component();
    let mut manager = PluginManager::new().expect("create manager");
    manager
        .register_component(&component)
        .expect("register first component");

    let error = manager
        .register_component(&component)
        .expect_err("duplicate registration must fail");
    assert!(matches!(
        error,
        provenance_plugin::PluginHostError::DuplicatePlugin(_)
    ));
}

#[test]
fn core_adapter_contract_is_used_for_wasm_output() {
    let component = hello_component();
    let mut manager = PluginManager::new().expect("create manager");
    manager
        .register_component(&component)
        .expect("register hello component");
    let adapter = manager.adapter("hello-tracker").expect("create adapter");
    let transaction = adapter
        .transaction(hello_request())
        .expect("call hello adapter");
    assert_eq!(
        transaction.seed,
        concat!(
            "provenance-plugin/v2",
            "\0",
            "7:tracker",
            "\0",
            "13:hello-tracker",
            "\0",
            "36:hello-observation\0tracker\0poll\0hello",
        )
    );
    assert_eq!(transaction.source.unwrap().id.namespace.as_str(), "tracker");
    assert_eq!(transaction.intents.len(), 4);
}

#[test]
fn host_rejects_empty_plugin_seed() {
    let component = hello_component();
    let mut manager = PluginManager::new().expect("create manager");
    manager
        .register_component(&component)
        .expect("register hello component");
    let adapter = manager.adapter("hello-tracker").expect("create adapter");

    let error = adapter
        .transaction(hello_request_with_mode("empty-seed"))
        .expect_err("empty plugin seed must fail at host boundary");
    assert!(matches!(
        error,
        provenance_plugin::PluginHostError::EmptyTransactionSeed { plugin }
            if plugin == "hello-tracker"
    ));
}

#[test]
fn host_rejects_plugin_source_outside_namespace() {
    let component = hello_component();
    let mut manager = PluginManager::new().expect("create manager");
    manager
        .register_component(&component)
        .expect("register hello component");
    let adapter = manager.adapter("hello-tracker").expect("create adapter");

    let error = adapter
        .transaction(hello_request_with_mode("bad-source"))
        .expect_err("foreign plugin source namespace must fail at host boundary");
    assert!(matches!(
        error,
        provenance_plugin::PluginHostError::SourceNamespaceMismatch {
            plugin,
            actual,
            expected,
        } if plugin == "hello-tracker" && actual == "other" && expected == "tracker"
    ));
}

#[test]
fn host_rejects_plugin_parent_outside_namespace() {
    let component = hello_component();
    let mut manager = PluginManager::new().expect("create manager");
    manager
        .register_component(&component)
        .expect("register hello component");
    let adapter = manager.adapter("hello-tracker").expect("create adapter");

    let error = adapter
        .transaction(hello_request_with_mode("bad-parent"))
        .expect_err("foreign plugin parent namespace must fail at host boundary");
    assert!(matches!(
        error,
        provenance_plugin::PluginHostError::SourceNamespaceMismatch {
            plugin,
            actual,
            expected,
        } if plugin == "hello-tracker" && actual == "other" && expected == "tracker"
    ));
}
