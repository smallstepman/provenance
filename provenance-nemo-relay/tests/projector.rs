use std::fs;

use nemo_relay_plugin::Event;
use provenance_nemo_relay::{
    AGENT_EVENT_KIND, AGENT_LINK_KIND, AGENT_NAMESPACE, AGENT_SESSION_EVENT_KIND, LocalEventSpool,
    RelayCaptureMode, RelayIdentityConfig, RelayProjector, RelayTelemetryUrlConfig,
};
use serde_json::json;
use tempfile::tempdir;

fn scope_event(
    uuid: &str,
    parent_uuid: Option<&str>,
    scope_category: &str,
    category: &str,
    metadata: serde_json::Value,
    data: serde_json::Value,
) -> Event {
    scope_event_named(
        uuid,
        parent_uuid,
        scope_category,
        category,
        category,
        metadata,
        data,
    )
}

fn scope_event_named(
    uuid: &str,
    parent_uuid: Option<&str>,
    scope_category: &str,
    name: &str,
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
        "name": name,
        "data": data,
        "metadata": metadata,
        "scope_category": scope_category,
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

fn attribute<'a>(
    request: &'a provenance_plugin::bindings::ObservationRequest,
    name: &str,
) -> &'a provenance_plugin::bindings::Value {
    &request
        .attributes
        .iter()
        .find(|attribute| attribute.name == name)
        .unwrap_or_else(|| panic!("missing attribute {name}"))
        .value
}

fn string_attribute<'a>(
    request: &'a provenance_plugin::bindings::ObservationRequest,
    name: &str,
) -> &'a str {
    match attribute(request, name) {
        provenance_plugin::bindings::Value::StringValue(value) => value,
        _ => panic!("attribute {name} is not a string"),
    }
}

fn has_attribute(request: &provenance_plugin::bindings::ObservationRequest, name: &str) -> bool {
    request
        .attributes
        .iter()
        .any(|attribute| attribute.name == name)
}

#[test]
fn projector_records_bounded_payload_evidence_not_payload_contents() {
    let identity = RelayIdentityConfig::new("omp", "workstation-a").expect("identity");
    let mut projector = RelayProjector::with_capture_mode(identity, RelayCaptureMode::Audit);
    let event = scope_event(
        "018f0c8e-7b12-7a34-9d56-123456789abc",
        None,
        "start",
        "agent",
        json!({"session_id":"session-42", "actor_id":"operator"}),
        json!({"prompt":"secret prompt", "large":"payload"}),
    );

    let request = projector
        .project(&event, None)
        .expect("project event")
        .expect("audit event is projected");
    assert_eq!(request.source.namespace, AGENT_NAMESPACE);
    assert_eq!(request.source.kind, AGENT_EVENT_KIND);
    assert!(request.source.id.ends_with("/start"));
    assert!(
        request
            .cursor
            .as_deref()
            .is_some_and(|cursor| cursor.contains("/start"))
    );
    assert!(matches!(
        attribute(&request, "session-start"),
        provenance_plugin::bindings::Value::Boolean(true)
    ));
    assert!(matches!(
        attribute(&request, "data-present"),
        provenance_plugin::bindings::Value::Boolean(true)
    ));
    assert!(
        request
            .attributes
            .iter()
            .all(|attribute| attribute.name != "prompt")
    );
    assert!(
        request
            .attributes
            .iter()
            .all(|attribute| attribute.name != "large")
    );
    assert!(
        matches!(attribute(&request, "data-size"), provenance_plugin::bindings::Value::IntegerText(size) if size == "44")
    );
}

#[test]
fn projector_inherits_session_and_links_scope_end_to_start() {
    let identity = RelayIdentityConfig::new("prime", "workstation-a").expect("identity");
    let mut projector = RelayProjector::with_capture_mode(identity, RelayCaptureMode::Audit);
    let root_uuid = "018f0c8e-7b12-7a34-9d56-123456789abc";
    let child_uuid = "018f0c8e-7b12-7a34-9d56-123456789abd";
    let root = scope_event(
        root_uuid,
        None,
        "start",
        "agent",
        json!({"session_id":"session-42", "actor_id":"operator"}),
        json!(null),
    );
    let child = scope_event(
        child_uuid,
        Some(root_uuid),
        "start",
        "tool",
        json!({}),
        json!({"command":"git status"}),
    );
    let child_end = scope_event(
        child_uuid,
        Some(root_uuid),
        "end",
        "tool",
        json!({}),
        json!({"exit_code":0}),
    );

    let root_request = projector
        .project(&root, None)
        .expect("root projection")
        .expect("root audit event");
    let child_request = projector
        .project(&child, None)
        .expect("child projection")
        .expect("child audit event");
    let child_end_request = projector
        .project(&child_end, None)
        .expect("child end projection")
        .expect("child end audit event");
    assert_eq!(
        string_attribute(&root_request, "session-key"),
        string_attribute(&child_request, "session-key")
    );
    assert_eq!(
        string_attribute(&child_request, "session-key"),
        string_attribute(&child_end_request, "session-key")
    );
    assert!(child_end_request.source.id.ends_with("/end"));

    let parents = projector.source_parents(&child_end);
    assert_eq!(parents.len(), 2);
    assert!(
        parents
            .iter()
            .any(|parent| parent.id == format!("{child_uuid}/start"))
    );
    assert!(
        parents
            .iter()
            .any(|parent| parent.id == format!("{root_uuid}/start"))
    );
}

#[test]
fn projector_recognizes_relay_session_hook_marks() {
    let identity = RelayIdentityConfig::new("codex", "workstation-a").expect("identity");
    let mut projector = RelayProjector::with_capture_mode(identity, RelayCaptureMode::Audit);
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
    let child = scope_event(
        "018f0c8e-7b12-7a34-9d56-123456789abe",
        Some(session_instance),
        "start",
        "custom",
        json!({}),
        json!({"prompt":"not persisted"}),
    );
    let session_end = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789abf",
        Some(session_instance),
        "session.end",
        json!({"hook_event_name":"SessionEnd"}),
    );

    let start = projector
        .project(&session_start, None)
        .expect("project session start")
        .expect("session start is projected");
    let child = projector
        .project(&child, None)
        .expect("project child")
        .expect("child is projected");
    let end = projector
        .project(&session_end, None)
        .expect("project session end")
        .expect("session end is projected");

    assert!(matches!(
        attribute(&start, "session-start"),
        provenance_plugin::bindings::Value::Boolean(true)
    ));
    assert_eq!(
        string_attribute(&start, "session-key"),
        string_attribute(&child, "session-key")
    );
    assert_eq!(
        string_attribute(&child, "session-key"),
        string_attribute(&end, "session-key")
    );
    assert!(matches!(
        attribute(&end, "session-end"),
        provenance_plugin::bindings::Value::Boolean(true)
    ));
}
#[test]
fn projector_correlates_relay_ids_with_configured_telemetry() {
    let identity = RelayIdentityConfig::new("codex", "workstation-a").expect("identity");
    let mut projector = RelayProjector::with_capture_mode_and_telemetry(
        identity,
        RelayCaptureMode::Audit,
        Some("phoenix".into()),
    );
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
    let turn_start_event = scope_event_named(
        turn_uuid,
        Some(session_instance),
        "start",
        "codex-turn",
        "custom",
        json!({
            "session_id": "session-42",
            "session_instance_id": session_instance
        }),
        json!({"prompt": "keep outside provenance"}),
    );
    let llm_uuid = "018f0c8e-7b12-7a34-9d56-123456789abf";
    let llm_start_event = scope_event_named(
        llm_uuid,
        Some(turn_uuid),
        "start",
        "openai.responses",
        "llm",
        json!({}),
        json!({"model": "gpt-5.6-luna"}),
    );
    let llm_chunk = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789ac1",
        Some(llm_uuid),
        "llm.chunk",
        json!({}),
    );
    let llm_end_event = scope_event_named(
        llm_uuid,
        Some(turn_uuid),
        "end",
        "openai.responses",
        "llm",
        json!({}),
        json!({"status": "success"}),
    );
    let turn_end_event = scope_event_named(
        turn_uuid,
        Some(session_instance),
        "end",
        "codex-turn",
        "custom",
        json!({}),
        json!({"status": "success"}),
    );
    let session_end = mark_event(
        "018f0c8e-7b12-7a34-9d56-123456789ac0",
        Some(session_instance),
        "session.end",
        json!({"hook_event_name":"SessionEnd"}),
    );

    let start = projector
        .project(&session_start, None)
        .expect("project session start")
        .expect("session start is projected");
    let turn_start = projector
        .project(&turn_start_event, None)
        .expect("project turn start")
        .expect("turn start is projected");
    let llm_start = projector
        .project(&llm_start_event, None)
        .expect("project LLM start")
        .expect("LLM start is projected");
    let llm_chunk = projector
        .project(&llm_chunk, None)
        .expect("project LLM chunk")
        .expect("LLM chunk is projected");
    let llm_end = projector
        .project(&llm_end_event, None)
        .expect("project LLM end")
        .expect("LLM end is projected");
    let turn_end = projector
        .project(&turn_end_event, None)
        .expect("project turn end")
        .expect("turn end is projected");
    let end = projector
        .project(&session_end, None)
        .expect("project session end")
        .expect("session end is projected");

    for request in [
        &start,
        &turn_start,
        &llm_start,
        &llm_chunk,
        &llm_end,
        &turn_end,
        &end,
    ] {
        assert_eq!(string_attribute(request, "telemetry-provider"), "phoenix");
        assert_eq!(
            string_attribute(request, "telemetry-session-instance-id"),
            session_instance
        );
        assert_eq!(
            string_attribute(request, "telemetry-session-id"),
            "session-42"
        );
    }
    assert!(!has_attribute(&start, "telemetry-trace-id"));
    assert!(!has_attribute(&start, "telemetry-span-id"));
    assert!(!has_attribute(&end, "telemetry-trace-id"));
    assert!(!has_attribute(&end, "telemetry-span-id"));

    let expected_trace_id = "018f0c8e7b127a349d56123456789abe";
    for request in [&turn_start, &llm_start, &llm_chunk, &llm_end, &turn_end] {
        assert_eq!(
            string_attribute(request, "telemetry-trace-id"),
            expected_trace_id
        );
    }
    assert_eq!(
        string_attribute(&turn_start, "telemetry-span-id"),
        "9d56123456789abe"
    );
    assert_eq!(
        string_attribute(&turn_end, "telemetry-span-id"),
        "9d56123456789abe"
    );
    assert_eq!(
        string_attribute(&llm_start, "telemetry-span-id"),
        "9d56123456789abf"
    );
    assert_eq!(
        string_attribute(&llm_chunk, "telemetry-span-id"),
        "9d56123456789abf"
    );
    assert_eq!(
        string_attribute(&llm_end, "telemetry-span-id"),
        "9d56123456789abf"
    );
}

#[test]
fn projector_renders_configured_telemetry_urls() {
    let identity = RelayIdentityConfig::new("codex", "workstation-a").expect("identity");
    let links = RelayTelemetryUrlConfig {
        project: Some("omp".into()),
        trace_ui_template: Some("http://127.0.0.1:6006/redirects/traces/{trace_id}".into()),
        trace_api_template: Some(
            "http://127.0.0.1:6006/v1/projects/{project}/spans?trace_id={trace_id}".into(),
        ),
        span_ui_template: Some("http://127.0.0.1:6006/redirects/spans/{span_id}".into()),
        span_api_template: Some(
            "http://127.0.0.1:6006/v1/projects/{project}/spans?span_id={span_id}".into(),
        ),
    };
    let mut projector = RelayProjector::with_capture_mode_and_telemetry_urls(
        identity,
        RelayCaptureMode::Audit,
        Some("phoenix".into()),
        links,
    );
    let event = scope_event_named(
        "018f0c8e-7b12-7a34-9d56-123456789abe",
        None,
        "start",
        "codex-turn",
        "custom",
        json!({}),
        json!(null),
    );
    let request = projector
        .project(&event, None)
        .expect("project event")
        .expect("audit event");

    assert_eq!(
        string_attribute(&request, "telemetry-trace-ui-url"),
        "http://127.0.0.1:6006/redirects/traces/018f0c8e7b127a349d56123456789abe"
    );
    assert_eq!(
        string_attribute(&request, "telemetry-trace-api-url"),
        "http://127.0.0.1:6006/v1/projects/omp/spans?trace_id=018f0c8e7b127a349d56123456789abe"
    );
    assert_eq!(
        string_attribute(&request, "telemetry-span-ui-url"),
        "http://127.0.0.1:6006/redirects/spans/9d56123456789abe"
    );
    assert_eq!(
        string_attribute(&request, "telemetry-span-api-url"),
        "http://127.0.0.1:6006/v1/projects/omp/spans?span_id=9d56123456789abe"
    );
}

#[test]
fn projector_supports_documented_langfuse_and_langsmith_links() {
    let cases = [
        (
            "langfuse",
            RelayTelemetryUrlConfig {
                project: Some("agent-project".into()),
                trace_ui_template: Some(
                    "https://cloud.langfuse.com/project/{project}/traces/{trace_id}".into(),
                ),
                trace_api_template: Some(
                    "https://cloud.langfuse.com/api/public/v2/observations?fields=core,basic&traceId={trace_id}"
                        .into(),
                ),
                ..RelayTelemetryUrlConfig::default()
            },
            [
                (
                    "telemetry-trace-ui-url",
                    "https://cloud.langfuse.com/project/agent-project/traces/018f0c8e7b127a349d56123456789abe",
                ),
                (
                    "telemetry-trace-api-url",
                    "https://cloud.langfuse.com/api/public/v2/observations?fields=core,basic&traceId=018f0c8e7b127a349d56123456789abe",
                ),
            ],
        ),
        (
            "langsmith",
            RelayTelemetryUrlConfig {
                trace_ui_template: Some("https://smith.langchain.com/public/{trace_id}/r".into()),
                trace_api_template: Some(
                    "https://api.smith.langchain.com/api/v1/runs/{trace_id}".into(),
                ),
                ..RelayTelemetryUrlConfig::default()
            },
            [
                (
                    "telemetry-trace-ui-url",
                    "https://smith.langchain.com/public/018f0c8e7b127a349d56123456789abe/r",
                ),
                (
                    "telemetry-trace-api-url",
                    "https://api.smith.langchain.com/api/v1/runs/018f0c8e7b127a349d56123456789abe",
                ),
            ],
        ),
    ];

    for (provider, links, expected_urls) in cases {
        let identity = RelayIdentityConfig::new(provider, "workstation-a").expect("identity");
        let mut projector = RelayProjector::with_capture_mode_and_telemetry_urls(
            identity,
            RelayCaptureMode::Audit,
            Some(provider.into()),
            links,
        );
        let event = scope_event_named(
            "018f0c8e-7b12-7a34-9d56-123456789abe",
            None,
            "start",
            "agent-turn",
            "custom",
            json!({}),
            json!(null),
        );
        let request = projector
            .project(&event, None)
            .expect("project event")
            .expect("audit event");

        assert_eq!(string_attribute(&request, "telemetry-provider"), provider);
        assert_eq!(
            string_attribute(&request, "telemetry-trace-id"),
            "018f0c8e7b127a349d56123456789abe"
        );
        for (name, expected) in expected_urls {
            assert_eq!(string_attribute(&request, name), expected);
        }
    }
}

#[test]
fn projector_keeps_logfire_identity_without_inventing_a_get_url() {
    let identity = RelayIdentityConfig::new("logfire", "workstation-a").expect("identity");
    let mut projector = RelayProjector::with_capture_mode_and_telemetry_urls(
        identity,
        RelayCaptureMode::Audit,
        Some("logfire".into()),
        RelayTelemetryUrlConfig::default(),
    );
    let event = scope_event_named(
        "018f0c8e-7b12-7a34-9d56-123456789abe",
        None,
        "start",
        "agent-turn",
        "custom",
        json!({}),
        json!(null),
    );
    let request = projector
        .project(&event, None)
        .expect("project event")
        .expect("audit event");

    assert_eq!(string_attribute(&request, "telemetry-provider"), "logfire");
    assert_eq!(
        string_attribute(&request, "telemetry-trace-id"),
        "018f0c8e7b127a349d56123456789abe"
    );
    assert!(!has_attribute(&request, "telemetry-trace-ui-url"));
    assert!(!has_attribute(&request, "telemetry-trace-api-url"));
}

#[test]
fn link_only_projects_source_attribution_and_session_boundary() {
    let identity = RelayIdentityConfig::new("omp", "workstation-a").expect("identity");
    let mut projector = RelayProjector::new(identity);
    let root_uuid = "018f0c8e-7b12-7a34-9d56-123456789abc";
    let source_uuid = "018f0c8e-7b12-7a34-9d56-123456789abd";
    let root = scope_event(
        root_uuid,
        Some("018f0c8e-7b12-7a34-9d56-123456789abf"),
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
    let unlinked_event = scope_event(
        "018f0c8e-7b12-7a34-9d56-123456789abe",
        Some(root_uuid),
        "start",
        "tool",
        json!({"trace_id":"trace-7", "span_id":"span-98"}),
        json!({"command":"git status"}),
    );
    let root_end = scope_event(
        root_uuid,
        Some("018f0c8e-7b12-7a34-9d56-123456789abf"),
        "end",
        "agent",
        json!({"session_id":"session-42", "actor_id":"operator"}),
        json!({"status":"success"}),
    );

    assert!(
        projector
            .project(&root, None)
            .expect("project root")
            .is_none()
    );
    assert!(
        projector
            .project(&unlinked_event, None)
            .expect("project unlinked event")
            .is_none()
    );

    let link = projector
        .project(&source_event, None)
        .expect("project source event")
        .expect("source event is projected");
    assert_eq!(link.source.namespace, AGENT_NAMESPACE);
    assert_eq!(link.source.kind, AGENT_LINK_KIND);
    assert_ne!(link.source.id, RelayProjector::event_key(&source_event));
    assert_eq!(string_attribute(&link, "source-namespace"), "jj");
    assert_eq!(string_attribute(&link, "source-kind"), "operation");
    assert_eq!(string_attribute(&link, "source-id"), "abc123");
    assert_eq!(string_attribute(&link, "outcome"), "committed");
    assert_eq!(string_attribute(&link, "trace-provider"), "phoenix");
    assert!(has_attribute(&link, "trace-id"));
    assert!(has_attribute(&link, "span-id"));
    assert!(!has_attribute(&link, "data-digest"));
    assert!(!has_attribute(&link, "metadata-digest"));
    assert!(!has_attribute(&link, "scope-attributes"));
    assert!(!has_attribute(&link, "prompt"));
    projector.remember_event(&source_event);

    let session_end = projector
        .project(&root_end, None)
        .expect("project session end")
        .expect("captured session end is projected");
    assert_eq!(session_end.source.kind, AGENT_SESSION_EVENT_KIND);
    assert!(matches!(
        attribute(&session_end, "session-end"),
        provenance_plugin::bindings::Value::Boolean(true)
    ));
    assert!(!has_attribute(&session_end, "source-id"));
    let parents = projector.source_parents(&root_end);
    assert_eq!(parents.len(), 1);
    assert_eq!(parents[0].namespace.as_str(), link.source.namespace);
    assert_eq!(parents[0].kind.as_str(), link.source.kind);
    assert_eq!(parents[0].id.as_str(), link.source.id);
}

#[test]
fn local_spool_returns_replayable_byte_locator() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("atof/events.jsonl");
    let event = scope_event(
        "018f0c8e-7b12-7a34-9d56-123456789abc",
        None,
        "start",
        "agent",
        json!({"session_id":"session-42"}),
        json!({"prompt":"secret"}),
    );
    let mut spool = LocalEventSpool::open(&path).expect("open spool");
    let locator = spool.append(&event).expect("append event");
    let bytes = fs::read(&path).expect("read spool");
    assert_eq!(locator.byte_offset, 0);
    assert_eq!(locator.byte_length as usize + 1, bytes.len());
    assert_eq!(locator.path, path.to_string_lossy());
    assert_eq!(&bytes[locator.byte_length as usize..], b"\n");
    assert!(!locator.digest.is_empty());
}
