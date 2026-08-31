use std::fs;
use std::process::Command;

use nemo_relay_plugin::Event;
use provenance_core::{Kernel, Service};
use provenance_nemo_relay::{
    AgentIdentity, AgentService, DirectoryRuntime, IngestOutcome, RelayBridge, RelayCaptureMode,
    RelayIdentityConfig, RelayProjector, durable_heads,
};
use provenance_plugin::PluginManager;
use serde_json::json;
use tempfile::{TempDir, tempdir};
use wit_component::ComponentEncoder;

fn build_agent_component() -> Vec<u8> {
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
    ComponentEncoder::default()
        .module(&module)
        .expect("read wit-bindgen component metadata")
        .encode()
        .expect("encode agent component")
}

fn scope_event(
    uuid: &str,
    parent_uuid: Option<&str>,
    phase: &str,
    category: &str,
    metadata: serde_json::Value,
    data: serde_json::Value,
) -> Event {
    serde_json::from_value(json!({
        "kind": "scope",
        "atof_version": "0.1",
        "parent_uuid": parent_uuid,
        "uuid": uuid,
        "timestamp": "2026-08-28T12:00:00Z",
        "name": category,
        "data": data,
        "metadata": metadata,
        "scope_category": phase,
        "attributes": [],
        "category": category
    }))
    .expect("valid ATOF scope event")
}

fn mark_event(
    uuid: &str,
    parent_uuid: Option<&str>,
    name: &str,
    metadata: serde_json::Value,
) -> Event {
    serde_json::from_value(json!({
        "kind": "mark",
        "atof_version": "0.1",
        "parent_uuid": parent_uuid,
        "uuid": uuid,
        "timestamp": "2026-08-28T12:00:00Z",
        "name": name,
        "data": null,
        "metadata": metadata,
        "data_schema": null,
        "category": null,
        "category_profile": null
    }))
    .expect("valid ATOF mark event")
}

fn service(manager: &PluginManager, store: &std::path::Path) -> AgentService {
    let adapter = manager.adapter("agent").expect("agent adapter");
    let runtime = DirectoryRuntime::open(store).expect("open provenance runtime");
    let state = runtime.load_state().expect("load provenance state");
    Service {
        kernel: Kernel::new(AgentIdentity),
        adapter,
        runtime,
        state,
    }
}

#[test]
fn relay_events_commit_agent_graph_and_replay_idempotently() {
    let component = build_agent_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    manager
        .register_component(&component)
        .expect("register agent component");

    let root_uuid = "018f0c8e-7b12-7a34-9d56-123456789abc";
    let child_uuid = "018f0c8e-7b12-7a34-9d56-123456789abd";
    let root_start = scope_event(
        root_uuid,
        None,
        "start",
        "agent",
        json!({"session_id":"session-42", "actor_id":"operator"}),
        json!({"prompt":"not copied into provenance"}),
    );
    let child_start = scope_event(
        child_uuid,
        Some(root_uuid),
        "start",
        "tool",
        json!({"trace_id":"trace-1", "span_id":"span-1", "provider":"local"}),
        json!({"command":"git status"}),
    );
    let child_end = scope_event(
        child_uuid,
        Some(root_uuid),
        "end",
        "tool",
        json!({"trace_id":"trace-1", "span_id":"span-1", "provider":"local"}),
        json!({"exit_code":0}),
    );
    let root_end = scope_event(
        root_uuid,
        None,
        "end",
        "agent",
        json!({"session_id":"session-42", "actor_id":"operator"}),
        json!({"status":"success"}),
    );

    let workspace = tempdir().expect("temporary provenance workspace");
    let store = workspace.path().join("provenance");
    let spool = workspace.path().join("atof/events.jsonl");
    let projector = RelayProjector::with_capture_mode(
        RelayIdentityConfig::new("omp", "workstation-a").expect("identity"),
        RelayCaptureMode::Audit,
    );
    let mut bridge = RelayBridge::from_manager(
        &manager,
        "agent",
        projector,
        service(&manager, &store),
        Some(provenance_nemo_relay::LocalEventSpool::open(&spool).expect("open spool")),
    )
    .expect("create bridge");

    for event in [&root_start, &child_start, &child_end, &root_end] {
        assert!(matches!(
            bridge.ingest(event).expect("ingest ATOF event"),
            IngestOutcome::Ingested { .. }
        ));
    }
    assert_eq!(bridge.processor().state.operations.len(), 4);
    assert_eq!(bridge.processor().state.projection.events.len(), 4);
    assert_eq!(bridge.processor().state.projection.sessions.len(), 1);
    assert_eq!(bridge.processor().state.projection.actors.len(), 1);
    assert_eq!(bridge.processor().state.projection.ended_sessions.len(), 1);
    assert_eq!(
        bridge
            .processor()
            .state
            .projection
            .source_to_operation
            .iter()
            .count(),
        4
    );
    assert_eq!(durable_heads(&store).expect("read heads").len(), 1);

    let before_operations = bridge.processor().state.operations.len();
    let before_heads = durable_heads(&store).expect("read heads");
    assert!(matches!(
        bridge.ingest(&root_start).expect("replay duplicate"),
        IngestOutcome::Duplicate { .. }
    ));
    assert_eq!(bridge.processor().state.operations.len(), before_operations);
    assert_eq!(durable_heads(&store).expect("read heads"), before_heads);
    drop(bridge);

    let mut restarted = RelayBridge::from_manager(
        &manager,
        "agent",
        RelayProjector::with_capture_mode(
            RelayIdentityConfig::new("omp", "workstation-a").expect("identity"),
            RelayCaptureMode::Audit,
        ),
        service(&manager, &store),
        None,
    )
    .expect("create restarted bridge");
    for event in [&root_start, &child_start, &child_end, &root_end] {
        assert!(matches!(
            restarted.ingest(event).expect("replay persisted event"),
            IngestOutcome::Ingested { .. } | IngestOutcome::Duplicate { .. }
        ));
    }
    assert_eq!(
        restarted.processor().state.operations.len(),
        before_operations
    );
    assert_eq!(restarted.processor().state.projection.events.len(), 4);
    assert_eq!(durable_heads(&store).expect("read heads"), before_heads);
}

#[test]
fn relay_audit_accepts_multiple_hook_sessions() {
    let component = build_agent_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    manager
        .register_component(&component)
        .expect("register agent component");

    let session_instance_a = "018f0c8e-7b12-7a34-9d56-123456789abc";
    let session_instance_b = "018f0c8e-7b12-7a34-9d56-123456789abe";
    let session_start_a = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789abd",
        Some(session_instance_a),
        "session.start",
        json!({
            "hook_event_name": "SessionStart",
            "session_id": "session-a",
            "session_instance_id": session_instance_a
        }),
    );
    let turn_uuid_a = "018f0c8e-7b12-7a34-9d56-123456789ac2";
    let turn_start_a = scope_event(
        turn_uuid_a,
        Some(session_instance_a),
        "start",
        "custom",
        json!({}),
        json!({"prompt":"kept in evidence only"}),
    );
    let turn_end_a = scope_event(
        turn_uuid_a,
        Some(session_instance_a),
        "end",
        "custom",
        json!({}),
        json!({"status":"success"}),
    );
    let session_end_a = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789abf",
        Some(session_instance_a),
        "session.end",
        json!({"hook_event_name":"SessionEnd"}),
    );
    let session_start_b = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789ac0",
        Some(session_instance_b),
        "session.start",
        json!({
            "hook_event_name": "SessionStart",
            "session_id": "session-b",
            "session_instance_id": session_instance_b
        }),
    );
    let session_end_b = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789ac1",
        Some(session_instance_b),
        "session.end",
        json!({"hook_event_name":"SessionEnd"}),
    );

    let workspace = tempdir().expect("temporary provenance workspace");
    let store = workspace.path().join("provenance");
    let mut bridge = RelayBridge::from_manager(
        &manager,
        "agent",
        RelayProjector::with_capture_mode_and_telemetry(
            RelayIdentityConfig::new("codex", "workstation-a").expect("identity"),
            RelayCaptureMode::Audit,
            Some("phoenix".into()),
        ),
        service(&manager, &store),
        None,
    )
    .expect("create bridge");

    for event in [
        &session_start_a,
        &turn_start_a,
        &turn_end_a,
        &session_end_a,
        &session_start_b,
        &session_end_b,
    ] {
        assert!(matches!(
            bridge.ingest(event).expect("ingest hook event"),
            IngestOutcome::Ingested { .. }
        ));
    }
    assert_eq!(bridge.processor().state.operations.len(), 6);
    assert_eq!(bridge.processor().state.projection.events.len(), 6);
    assert_eq!(bridge.processor().state.projection.sessions.len(), 2);
    assert_eq!(bridge.processor().state.projection.actors.len(), 2);
    assert_eq!(bridge.processor().state.projection.ended_sessions.len(), 2);

    let turn_event = bridge
        .processor()
        .state
        .projection
        .events
        .values()
        .find(|event| {
            matches!(
                event
                    .attributes
                    .get(&provenance_data_model::FieldName::from("event-id")),
                Some(provenance_data_model::Value::String(value))
                    if value.as_ref() == turn_uuid_a
            )
        })
        .expect("turn telemetry correlation");
    assert!(matches!(
        turn_event
            .attributes
            .get(&provenance_data_model::FieldName::from("telemetry-provider")),
        Some(provenance_data_model::Value::String(value)) if value.as_ref() == "phoenix"
    ));
    assert!(matches!(
        turn_event
            .attributes
            .get(&provenance_data_model::FieldName::from("telemetry-trace-id")),
        Some(provenance_data_model::Value::String(value))
            if value.as_ref() == "018f0c8e7b127a349d56123456789ac2"
    ));
    assert!(matches!(
        turn_event
            .attributes
            .get(&provenance_data_model::FieldName::from("telemetry-span-id")),
        Some(provenance_data_model::Value::String(value))
            if value.as_ref() == "9d56123456789ac2"
    ));
    assert!(turn_event.subjects.iter().any(|subject| {
        matches!(
            subject,
            provenance_core::EntityRef::External(address)
                if address.namespace.as_str() == "telemetry"
                    && address.kind.as_str() == "trace"
                    && address.id.as_str() == "phoenix:018f0c8e7b127a349d56123456789ac2"
        )
    }));
    assert_eq!(durable_heads(&store).expect("read heads").len(), 2);
}

#[test]
fn relay_codex_turn_keeps_session_identity_out_of_trace_parent() {
    let component = build_agent_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    manager
        .register_component(&component)
        .expect("register agent component");

    let session_instance = "018f0c8e-7b12-7a34-9d56-123456789abc";
    let session_start = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789abd",
        Some(session_instance),
        "session.start",
        json!({
            "hook_event_name": "SessionStart",
            "session_id": "session-42",
            "session_instance_id": session_instance
        }),
    );
    let turn_uuid = "018f0c8e-7b12-7a34-9d56-123456789abe";
    let turn_start = scope_event(
        turn_uuid,
        Some(session_instance),
        "start",
        "codex-turn",
        json!({
            "session_id": "session-42",
            "session_instance_id": session_instance
        }),
        json!({"prompt": "keep outside provenance"}),
    );
    let llm_uuid = "018f0c8e-7b12-7a34-9d56-123456789abf";
    let llm_start = scope_event(
        llm_uuid,
        Some(turn_uuid),
        "start",
        "openai.responses",
        json!({}),
        json!({"model": "gpt-5.6-luna"}),
    );
    let llm_end = scope_event(
        llm_uuid,
        Some(turn_uuid),
        "end",
        "openai.responses",
        json!({}),
        json!({"status": "success"}),
    );
    let turn_end = scope_event(
        turn_uuid,
        Some(session_instance),
        "end",
        "codex-turn",
        json!({}),
        json!({"status": "success"}),
    );
    let session_end = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789ac0",
        Some(session_instance),
        "session.end",
        json!({"hook_event_name": "SessionEnd"}),
    );

    let workspace = tempdir().expect("temporary provenance workspace");
    let store = workspace.path().join("provenance");
    let mut bridge = RelayBridge::from_manager(
        &manager,
        "agent",
        RelayProjector::with_capture_mode_and_telemetry(
            RelayIdentityConfig::new("codex", "workstation-a").expect("identity"),
            RelayCaptureMode::Audit,
            Some("phoenix".into()),
        ),
        service(&manager, &store),
        None,
    )
    .expect("create Codex bridge");

    for event in [
        &session_start,
        &turn_start,
        &llm_start,
        &llm_end,
        &turn_end,
        &session_end,
    ] {
        assert!(matches!(
            bridge.ingest(event).expect("ingest Codex event"),
            IngestOutcome::Ingested { .. }
        ));
    }

    let state = &bridge.processor().state;
    assert_eq!(state.projection.sessions.len(), 1);
    assert_eq!(state.projection.actors.len(), 1);
    assert_eq!(state.projection.ended_sessions.len(), 1);

    let projected = |event_id: &str| {
        state
            .projection
            .events
            .values()
            .find(|event| {
                matches!(
                    event
                        .attributes
                        .get(&provenance_data_model::FieldName::from("event-id")),
                    Some(provenance_data_model::Value::String(value))
                        if value.as_ref() == event_id
                )
            })
            .unwrap_or_else(|| panic!("missing projected event {event_id}"))
    };
    let session_start_event = projected(session_start.uuid().to_string().as_str());
    let turn_event = projected(turn_uuid);
    let llm_event = projected(llm_uuid);
    let session_end_event = projected(session_end.uuid().to_string().as_str());

    for event in [
        session_start_event,
        turn_event,
        llm_event,
        session_end_event,
    ] {
        assert!(matches!(
            event
                .attributes
                .get(&provenance_data_model::FieldName::from("telemetry-provider")),
            Some(provenance_data_model::Value::String(value))
                if value.as_ref() == "phoenix"
        ));
        assert!(matches!(
            event
                .attributes
                .get(&provenance_data_model::FieldName::from(
                    "telemetry-session-id"
                )),
            Some(provenance_data_model::Value::String(value))
                if value.as_ref() == "session-42"
        ));
        assert!(matches!(
            event
                .attributes
                .get(&provenance_data_model::FieldName::from(
                    "telemetry-session-instance-id"
                )),
            Some(provenance_data_model::Value::String(value))
                if value.as_ref() == session_instance
        ));
    }

    assert!(!event_has_string_attribute(
        session_start_event,
        "telemetry-trace-id"
    ));
    assert!(!event_has_string_attribute(
        session_start_event,
        "telemetry-span-id"
    ));
    assert!(!event_has_string_attribute(
        session_end_event,
        "telemetry-trace-id"
    ));
    assert!(!event_has_string_attribute(
        session_end_event,
        "telemetry-span-id"
    ));

    assert_eq!(
        event_string_attribute(turn_event, "name"),
        Some("codex-turn")
    );
    assert_eq!(
        event_string_attribute(turn_event, "telemetry-trace-id"),
        Some("018f0c8e7b127a349d56123456789abe")
    );
    assert_eq!(
        event_string_attribute(turn_event, "telemetry-span-id"),
        Some("9d56123456789abe")
    );
    assert_eq!(
        event_string_attribute(llm_event, "telemetry-trace-id"),
        Some("018f0c8e7b127a349d56123456789abe")
    );
    assert_eq!(
        event_string_attribute(llm_event, "telemetry-span-id"),
        Some("9d56123456789abf")
    );
    assert!(turn_event.subjects.iter().any(|subject| {
        matches!(
            subject,
            provenance_core::EntityRef::External(address)
                if address.namespace.as_str() == "telemetry"
                    && address.kind.as_str() == "trace"
                    && address.id.as_str() == "phoenix:018f0c8e7b127a349d56123456789abe"
        )
    }));
}

fn event_string_attribute<'a>(
    event: &'a provenance_core::Event<provenance_core::PluginModel>,
    name: &str,
) -> Option<&'a str> {
    match event
        .attributes
        .get(&provenance_data_model::FieldName::from(name))
    {
        Some(provenance_data_model::Value::String(value)) => Some(value.as_ref()),
        _ => None,
    }
}

fn event_has_string_attribute(
    event: &provenance_core::Event<provenance_core::PluginModel>,
    name: &str,
) -> bool {
    event_string_attribute(event, name).is_some()
}

#[test]
fn link_only_commits_sparse_source_attribution_and_replays() {
    let component = build_agent_component();
    let mut manager = PluginManager::new().expect("create plugin manager");
    manager
        .register_component(&component)
        .expect("register agent component");

    let root_uuid = "018f0c8e-7b12-7a34-9d56-123456789abc";
    let source_uuid = "018f0c8e-7b12-7a34-9d56-123456789abd";
    let root_start = scope_event(
        root_uuid,
        None,
        "start",
        "agent",
        json!({"session_id":"session-42", "actor_id":"operator"}),
        json!({"prompt":"keep outside provenance"}),
    );
    let source_event = scope_event(
        source_uuid,
        Some(root_uuid),
        "end",
        "tool",
        json!({
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
        }),
        json!({"command":"jj commit", "output":"secret"}),
    );
    let root_end = scope_event(
        root_uuid,
        None,
        "end",
        "agent",
        json!({"session_id":"session-42", "actor_id":"operator"}),
        json!({"status":"success"}),
    );

    let workspace = tempdir().expect("temporary provenance workspace");
    let store = workspace.path().join("provenance");
    let mut bridge = RelayBridge::from_manager(
        &manager,
        "agent",
        RelayProjector::new(RelayIdentityConfig::new("omp", "workstation-a").expect("identity")),
        service(&manager, &store),
        None,
    )
    .expect("create sparse bridge");

    assert!(matches!(
        bridge.ingest(&root_start).expect("ingest root start"),
        IngestOutcome::Skipped { .. }
    ));
    assert!(matches!(
        bridge.ingest(&source_event).expect("ingest source event"),
        IngestOutcome::Ingested { .. }
    ));
    assert!(matches!(
        bridge.ingest(&root_end).expect("ingest root end"),
        IngestOutcome::Ingested { .. }
    ));
    assert_eq!(bridge.processor().state.operations.len(), 2);
    assert_eq!(bridge.processor().state.projection.events.len(), 2);
    assert_eq!(bridge.processor().state.projection.sessions.len(), 1);
    assert_eq!(bridge.processor().state.projection.actors.len(), 1);
    assert_eq!(bridge.processor().state.projection.ended_sessions.len(), 1);

    let link_event = bridge
        .processor()
        .state
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
        .expect("source attribution event");
    assert!(
        link_event
            .attributes
            .keys()
            .all(|name| name.as_str() != "data-digest" && name.as_str() != "metadata-digest")
    );

    let before_operations = bridge.processor().state.operations.len();
    assert!(matches!(
        bridge.ingest(&source_event).expect("replay source event"),
        IngestOutcome::Duplicate { .. }
    ));
    assert_eq!(bridge.processor().state.operations.len(), before_operations);
    drop(bridge);

    let mut restarted = RelayBridge::from_manager(
        &manager,
        "agent",
        RelayProjector::new(RelayIdentityConfig::new("omp", "workstation-a").expect("identity")),
        service(&manager, &store),
        None,
    )
    .expect("create restarted sparse bridge");
    assert!(matches!(
        restarted.ingest(&root_start).expect("replay root start"),
        IngestOutcome::Skipped { .. }
    ));
    assert!(matches!(
        restarted
            .ingest(&source_event)
            .expect("replay source event"),
        IngestOutcome::Duplicate { .. }
    ));
    assert!(matches!(
        restarted.ingest(&root_end).expect("replay root end"),
        IngestOutcome::Duplicate { .. }
    ));
    assert_eq!(
        restarted.processor().state.operations.len(),
        before_operations
    );
    assert_eq!(restarted.processor().state.projection.events.len(), 2);
}
