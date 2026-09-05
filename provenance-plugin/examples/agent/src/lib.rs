#![no_std]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::{
    collections::BTreeMap,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

pub(crate) mod guest {
    pub(crate) use super::*;
}

const NAMESPACE: &str = "agent";
const SCHEMA_VERSION: &str = "1";
const EVENT_KIND: &str = "event";

struct AgentPlugin;

#[derive(Clone, Debug, Default)]
struct TelemetryCorrelation {
    provider: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    session_id: Option<String>,
    session_instance_id: Option<String>,
    trace_ui_url: Option<String>,
    trace_api_url: Option<String>,
    span_ui_url: Option<String>,
    span_api_url: Option<String>,
}

impl Guest for AgentPlugin {
    fn manifest() -> guest::PluginManifest {
        guest::PluginManifest {
            id: "agent".into(),
            abi_version: "0.2.0".into(),
            namespace: NAMESPACE.into(),
            schemas: vec![guest::SchemaKey {
                namespace: NAMESPACE.into(),
                version: SCHEMA_VERSION.into(),
            }],
            required_schemas: Vec::new(),
            capabilities: vec![
                "emit-observations".into(),
                "emit-agent-lifecycle".into(),
                "emit-attribution-links".into(),
            ],
        }
    }

    fn observe(
        request: guest::ObservationRequest,
    ) -> Result<guest::Transaction, guest::PluginError> {
        let mut attributes = BTreeMap::new();
        for attribute in request.attributes {
            if attributes
                .insert(attribute.name.clone(), attribute.value)
                .is_some()
            {
                return Err(invalid_request("duplicate observation attribute"));
            }
        }

        let projection_mode =
            optional_string(&mut attributes, "projection-mode")?.unwrap_or_else(|| "audit".into());
        let event_kind = required_string(&mut attributes, "event-kind")?;
        let event_id = required_string(&mut attributes, "event-id")?;
        let atof_version = required_string(&mut attributes, "atof-version")?;
        let name = required_string(&mut attributes, "name")?;
        let timestamp = required_string(&mut attributes, "timestamp")?;
        let harness_id = required_string(&mut attributes, "harness-id")?;
        let installation_id = required_string(&mut attributes, "installation-id")?;
        let parent_uuid = optional_string(&mut attributes, "parent-uuid")?;
        let category = optional_string(&mut attributes, "category")?;
        let scope_category = optional_string(&mut attributes, "scope-category")?;
        let session_key = optional_string(&mut attributes, "session-key")?;
        let session_id = optional_string(&mut attributes, "session-id")?;
        let actor_key = optional_string(&mut attributes, "actor-key")?;
        let actor_id = optional_string(&mut attributes, "actor-id")?;
        let session_start = optional_bool(&mut attributes, "session-start")?.unwrap_or(false);
        let session_end = optional_bool(&mut attributes, "session-end")?.unwrap_or(false);
        let scope_attributes = optional_string_list(&mut attributes, "scope-attributes")?;
        let model_name = optional_string(&mut attributes, "model-name")?;
        let tool_call_id = optional_string(&mut attributes, "tool-call-id")?;
        let profile_extra_keys = optional_string_list(&mut attributes, "profile-extra-keys")?;
        let data_digest = optional_string(&mut attributes, "data-digest")?;
        let data_size = optional_integer(&mut attributes, "data-size")?;
        let data_present = optional_bool(&mut attributes, "data-present")?;
        let metadata_digest = optional_string(&mut attributes, "metadata-digest")?;
        let metadata_size = optional_integer(&mut attributes, "metadata-size")?;
        let metadata_present = optional_bool(&mut attributes, "metadata-present")?;
        let data_schema_name = optional_string(&mut attributes, "data-schema-name")?;
        let data_schema_version = optional_string(&mut attributes, "data-schema-version")?;
        let trace_id = optional_string(&mut attributes, "trace-id")?;
        let span_id = optional_string(&mut attributes, "span-id")?;
        let provider = optional_string(&mut attributes, "provider")?;
        let trace_provider = optional_string(&mut attributes, "trace-provider")?;
        let evidence_location = optional_string(&mut attributes, "evidence-location")?;
        let evidence_ref = optional_string(&mut attributes, "evidence-ref")?;
        let evidence_offset = optional_integer(&mut attributes, "evidence-offset")?;
        let evidence_length = optional_integer(&mut attributes, "evidence-length")?;
        let evidence_digest = optional_string(&mut attributes, "evidence-digest")?;
        let telemetry_provider = optional_string(&mut attributes, "telemetry-provider")?;
        let telemetry_trace_id = optional_string(&mut attributes, "telemetry-trace-id")?;
        let telemetry_span_id = optional_string(&mut attributes, "telemetry-span-id")?;
        let telemetry_session_id = optional_string(&mut attributes, "telemetry-session-id")?;
        let telemetry_session_instance_id =
            optional_string(&mut attributes, "telemetry-session-instance-id")?;
        let telemetry_trace_ui_url = optional_string(&mut attributes, "telemetry-trace-ui-url")?;
        let telemetry_trace_api_url = optional_string(&mut attributes, "telemetry-trace-api-url")?;
        let telemetry_span_ui_url = optional_string(&mut attributes, "telemetry-span-ui-url")?;
        let telemetry_span_api_url = optional_string(&mut attributes, "telemetry-span-api-url")?;
        let telemetry = TelemetryCorrelation {
            provider: telemetry_provider,
            trace_id: telemetry_trace_id,
            span_id: telemetry_span_id,
            session_id: telemetry_session_id,
            session_instance_id: telemetry_session_instance_id,
            trace_ui_url: telemetry_trace_ui_url,
            trace_api_url: telemetry_trace_api_url,
            span_ui_url: telemetry_span_ui_url,
            span_api_url: telemetry_span_api_url,
        };
        let source_namespace = optional_string(&mut attributes, "source-namespace")?;
        let source_kind = optional_string(&mut attributes, "source-kind")?;
        let source_id = optional_string(&mut attributes, "source-id")?;
        let outcome = optional_string(&mut attributes, "outcome")?;
        if !attributes.is_empty() {
            return Err(invalid_request("unknown observation attribute"));
        }

        if event_kind != "scope" && event_kind != "mark" {
            return Err(invalid_request("event-kind must be scope or mark"));
        }
        if session_start && session_key.is_none() {
            return Err(invalid_request("session-start requires session-key"));
        }
        if session_end && session_key.is_none() {
            return Err(invalid_request("session-end requires session-key"));
        }
        if projection_mode == "link" {
            return link_transaction(
                request.source,
                request.cursor,
                event_kind,
                event_id,
                atof_version,
                name,
                timestamp,
                harness_id,
                installation_id,
                category,
                scope_category,
                session_key,
                session_id,
                actor_key,
                actor_id,
                session_start,
                session_end,
                trace_id,
                span_id,
                trace_provider,
                source_namespace,
                source_kind,
                source_id,
                outcome,
                evidence_ref,
                evidence_offset,
                evidence_length,
                evidence_digest,
                telemetry,
            );
        }
        if projection_mode != "audit" {
            return Err(invalid_request("projection-mode must be link or audit"));
        }

        let schema_key = guest::SchemaKey {
            namespace: NAMESPACE.into(),
            version: SCHEMA_VERSION.into(),
        };
        let event = guest::EntityAddress {
            namespace: NAMESPACE.into(),
            kind: EVENT_KIND.into(),
            id: request.source.id.clone(),
        };
        let observed_attributes = observed_attributes(
            event_id,
            event_kind,
            atof_version,
            name,
            timestamp,
            harness_id.clone(),
            installation_id.clone(),
            parent_uuid,
            category,
            scope_category,
            session_key.clone(),
            session_id.clone(),
            actor_key.clone(),
            actor_id.clone(),
            scope_attributes,
            model_name,
            tool_call_id,
            profile_extra_keys,
            data_digest,
            data_size,
            data_present,
            metadata_digest,
            metadata_size,
            metadata_present,
            data_schema_name,
            data_schema_version,
            trace_id,
            span_id,
            provider,
            evidence_location,
            evidence_offset,
            evidence_length,
            evidence_digest,
            &telemetry,
        );

        let mut intents = vec![guest::Intent::RegisterSchema(schema_definition(
            schema_key.clone(),
        ))];
        if session_start {
            let session_seed = session_key
                .clone()
                .ok_or_else(|| invalid_request("session-start requires session-key"))?;
            let session = session_id
                .clone()
                .ok_or_else(|| invalid_request("session-start requires session-id"))?;
            intents.push(guest::Intent::OpenSession(guest::OpenSession {
                seed: session_seed,
                parent: None,
                attributes: lifecycle_attributes(&harness_id, &installation_id, &telemetry),
            }));
            if let Some(actor_seed) = actor_key.clone() {
                let actor_session = Some(session.clone());
                intents.push(guest::Intent::DeclareActor(guest::DeclareActor {
                    seed: actor_seed,
                    session: actor_session,
                    attributes: lifecycle_attributes(&harness_id, &installation_id, &telemetry),
                }));
            }
        }

        let session_for_event = session_id.clone();
        let actor_for_event = actor_id.clone();
        let mut subjects = vec![guest::EntityRef::External(event.clone())];
        subjects.extend(telemetry_subjects(&telemetry));

        intents.push(guest::Intent::ObserveEntity(guest::EntityObservation {
            entity: event.clone(),
            schema: schema_key.clone(),
            attributes: observed_attributes.clone(),
        }));
        intents.push(guest::Intent::RecordEvent(guest::EventIntent {
            session: session_for_event,
            actor: actor_for_event,
            additional_parents: Vec::new(),
            subjects,
            relations: Vec::new(),
            requires: Vec::new(),
            attributes: observed_attributes,
        }));
        if session_end {
            intents.push(guest::Intent::EndSession(
                session_id.ok_or_else(|| invalid_request("session-end requires session-id"))?,
            ));
        }

        Ok(guest::Transaction {
            seed: observation_seed(&request.source, request.cursor.as_deref()),
            source: Some(guest::SourceOperation {
                id: request.source,
                parents: Vec::new(),
            }),
            intents,
            attributes: Vec::new(),
        })
    }

    fn install(_request: guest::InstallRequest) -> Result<guest::InstallPlan, guest::PluginError> {
        Err(guest::PluginError {
            kind: guest::PluginErrorKind::Unsupported,
            message: "this plugin does not install source hooks".into(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn link_transaction(
    source: guest::EntityAddress,
    cursor: Option<String>,
    event_kind: String,
    event_id: String,
    atof_version: String,
    name: String,
    timestamp: String,
    harness_id: String,
    installation_id: String,
    category: Option<String>,
    scope_category: Option<String>,
    session_key: Option<String>,
    session_id: Option<String>,
    actor_key: Option<String>,
    actor_id: Option<String>,
    session_start: bool,
    session_end: bool,
    trace_id: Option<String>,
    span_id: Option<String>,
    trace_provider: Option<String>,
    source_namespace: Option<String>,
    source_kind: Option<String>,
    source_id: Option<String>,
    outcome: Option<String>,
    evidence_ref: Option<String>,
    evidence_offset: Option<String>,
    evidence_length: Option<String>,
    evidence_digest: Option<String>,
    telemetry: TelemetryCorrelation,
) -> Result<guest::Transaction, guest::PluginError> {
    let source_subject = match (&source_namespace, &source_kind, &source_id) {
        (Some(namespace), Some(kind), Some(id)) => {
            Some(guest::EntityRef::External(guest::EntityAddress {
                namespace: namespace.clone(),
                kind: kind.clone(),
                id: id.clone(),
            }))
        }
        (None, None, None) if session_end => None,
        _ => {
            return Err(invalid_request(
                "link projection requires a complete source reference",
            ));
        }
    };

    let mut attributes = vec![
        text_attribute("event-id", event_id),
        text_attribute("event-kind", event_kind),
        text_attribute("atof-version", atof_version),
        text_attribute("name", name),
        text_attribute("occurred-at", timestamp),
        text_attribute("harness-id", harness_id.clone()),
        text_attribute("installation-id", installation_id.clone()),
    ];
    push_optional_text(&mut attributes, "category", category);
    push_optional_text(&mut attributes, "scope-category", scope_category);
    push_optional_text(&mut attributes, "session-id", session_id.clone());
    push_optional_text(&mut attributes, "actor-id", actor_id.clone());
    push_optional_text(&mut attributes, "source-namespace", source_namespace);
    push_optional_text(&mut attributes, "source-kind", source_kind);
    push_optional_text(&mut attributes, "source-id", source_id);
    push_optional_text(&mut attributes, "outcome", outcome);
    push_optional_text(&mut attributes, "trace-id", trace_id);
    push_optional_text(&mut attributes, "span-id", span_id);
    push_optional_text(&mut attributes, "trace-provider", trace_provider);
    push_optional_text(&mut attributes, "evidence-ref", evidence_ref);
    push_optional_integer(&mut attributes, "evidence-offset", evidence_offset);
    push_optional_integer(&mut attributes, "evidence-length", evidence_length);
    push_optional_text(&mut attributes, "evidence-digest", evidence_digest);
    append_telemetry_attributes(&mut attributes, &telemetry);
    let mut intents = Vec::new();

    if session_start {
        let session_seed =
            session_key.ok_or_else(|| invalid_request("session-start requires session-key"))?;
        let session = session_id
            .clone()
            .ok_or_else(|| invalid_request("session-start requires session-id"))?;
        intents.push(guest::Intent::OpenSession(guest::OpenSession {
            seed: session_seed,
            parent: None,
            attributes: lifecycle_attributes(&harness_id, &installation_id, &telemetry),
        }));

        if let Some(actor_seed) = actor_key {
            intents.push(guest::Intent::DeclareActor(guest::DeclareActor {
                seed: actor_seed,
                session: Some(session),
                attributes: lifecycle_attributes(&harness_id, &installation_id, &telemetry),
            }));
        }
    }
    let mut subjects = source_subject.into_iter().collect::<Vec<_>>();
    subjects.extend(telemetry_subjects(&telemetry));

    intents.push(guest::Intent::RecordEvent(guest::EventIntent {
        session: session_id.clone(),
        actor: actor_id,
        additional_parents: Vec::new(),
        subjects,
        relations: Vec::new(),
        requires: Vec::new(),
        attributes,
    }));
    if session_end {
        intents.push(guest::Intent::EndSession(
            session_id.ok_or_else(|| invalid_request("session-end requires session-id"))?,
        ));
    }

    let seed = observation_seed(&source, cursor.as_deref());
    Ok(guest::Transaction {
        seed,
        source: Some(guest::SourceOperation {
            id: source,
            parents: Vec::new(),
        }),
        intents,
        attributes: Vec::new(),
    })
}

fn schema_definition(schema_key: guest::SchemaKey) -> guest::SchemaDefinition {
    let mut fields = vec![
        field("event-id", guest::ValueType::StringValue, true),
        field("event-kind", guest::ValueType::StringValue, true),
        field("atof-version", guest::ValueType::StringValue, true),
        field("name", guest::ValueType::StringValue, true),
        field("timestamp", guest::ValueType::StringValue, true),
        field("harness-id", guest::ValueType::StringValue, true),
        field("installation-id", guest::ValueType::StringValue, true),
    ];
    for name in [
        "parent-uuid",
        "category",
        "scope-category",
        "session-key",
        "session-id",
        "actor-key",
        "actor-id",
        "model-name",
        "tool-call-id",
        "data-digest",
        "metadata-digest",
        "data-schema-name",
        "data-schema-version",
        "trace-id",
        "span-id",
        "provider",
        "telemetry-provider",
        "telemetry-trace-id",
        "telemetry-span-id",
        "telemetry-session-id",
        "telemetry-session-instance-id",
        "telemetry-trace-ui-url",
        "telemetry-trace-api-url",
        "telemetry-span-ui-url",
        "telemetry-span-api-url",
        "evidence-location",
        "evidence-digest",
    ] {
        fields.push(field(name, guest::ValueType::StringValue, false));
    }
    for name in [
        "data-size",
        "metadata-size",
        "evidence-offset",
        "evidence-length",
    ] {
        fields.push(field(name, guest::ValueType::Integer, false));
    }
    for name in ["data-present", "metadata-present"] {
        fields.push(field(name, guest::ValueType::Boolean, false));
    }
    for name in ["scope-attributes", "profile-extra-keys"] {
        fields.push(field(
            name,
            guest::ValueType::ListValue(guest::ScalarValueType::StringValue),
            false,
        ));
    }
    guest::SchemaDefinition {
        key: schema_key,
        requires: Vec::new(),
        entities: vec![guest::EntitySchema {
            kind: EVENT_KIND.into(),
            fields,
            allow_unknown_fields: false,
        }],
        relations: Vec::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn observed_attributes(
    event_id: String,
    event_kind: String,
    atof_version: String,
    name: String,
    timestamp: String,
    harness_id: String,
    installation_id: String,
    parent_uuid: Option<String>,
    category: Option<String>,
    scope_category: Option<String>,
    session_key: Option<String>,
    session_id: Option<String>,
    actor_key: Option<String>,
    actor_id: Option<String>,
    scope_attributes: Vec<String>,
    model_name: Option<String>,
    tool_call_id: Option<String>,
    profile_extra_keys: Vec<String>,
    data_digest: Option<String>,
    data_size: Option<String>,
    data_present: Option<bool>,
    metadata_digest: Option<String>,
    metadata_size: Option<String>,
    metadata_present: Option<bool>,
    data_schema_name: Option<String>,
    data_schema_version: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    provider: Option<String>,
    evidence_location: Option<String>,
    evidence_offset: Option<String>,
    evidence_length: Option<String>,
    evidence_digest: Option<String>,
    telemetry: &TelemetryCorrelation,
) -> Vec<guest::Attribute> {
    let mut result = vec![
        text_attribute("event-id", event_id),
        text_attribute("event-kind", event_kind),
        text_attribute("atof-version", atof_version),
        text_attribute("name", name),
        text_attribute("timestamp", timestamp),
        text_attribute("harness-id", harness_id),
        text_attribute("installation-id", installation_id),
    ];
    push_optional_text(&mut result, "parent-uuid", parent_uuid);
    push_optional_text(&mut result, "category", category);
    push_optional_text(&mut result, "session-id", session_id);
    push_optional_text(&mut result, "scope-category", scope_category);
    push_optional_text(&mut result, "session-key", session_key);
    push_optional_text(&mut result, "actor-key", actor_key);
    push_optional_list(&mut result, "scope-attributes", scope_attributes);
    push_optional_text(&mut result, "actor-id", actor_id);
    push_optional_text(&mut result, "model-name", model_name);
    push_optional_text(&mut result, "tool-call-id", tool_call_id);
    push_optional_list(&mut result, "profile-extra-keys", profile_extra_keys);
    push_optional_text(&mut result, "data-digest", data_digest);
    push_optional_text(&mut result, "metadata-digest", metadata_digest);
    push_optional_text(&mut result, "data-schema-name", data_schema_name);
    push_optional_text(&mut result, "data-schema-version", data_schema_version);
    push_optional_text(&mut result, "trace-id", trace_id);
    push_optional_text(&mut result, "span-id", span_id);
    push_optional_text(&mut result, "provider", provider);
    push_optional_text(&mut result, "evidence-location", evidence_location);
    push_optional_text(&mut result, "evidence-digest", evidence_digest);
    append_telemetry_attributes(&mut result, telemetry);
    push_optional_integer(&mut result, "data-size", data_size);
    push_optional_integer(&mut result, "metadata-size", metadata_size);
    push_optional_integer(&mut result, "evidence-offset", evidence_offset);
    push_optional_integer(&mut result, "evidence-length", evidence_length);
    push_optional_bool(&mut result, "data-present", data_present);
    push_optional_bool(&mut result, "metadata-present", metadata_present);
    result
}

fn lifecycle_attributes(
    harness_id: &str,
    installation_id: &str,
    telemetry: &TelemetryCorrelation,
) -> Vec<guest::Attribute> {
    let mut attributes = vec![
        text_attribute("harness-id", harness_id.into()),
        text_attribute("installation-id", installation_id.into()),
    ];
    append_telemetry_attributes(&mut attributes, telemetry);
    attributes
}
fn append_telemetry_attributes(
    attributes: &mut Vec<guest::Attribute>,
    telemetry: &TelemetryCorrelation,
) {
    push_optional_text(attributes, "telemetry-provider", telemetry.provider.clone());
    push_optional_text(attributes, "telemetry-trace-id", telemetry.trace_id.clone());
    push_optional_text(attributes, "telemetry-span-id", telemetry.span_id.clone());
    push_optional_text(
        attributes,
        "telemetry-session-id",
        telemetry.session_id.clone(),
    );
    push_optional_text(
        attributes,
        "telemetry-session-instance-id",
        telemetry.session_instance_id.clone(),
    );
    push_optional_text(
        attributes,
        "telemetry-trace-ui-url",
        telemetry.trace_ui_url.clone(),
    );
    push_optional_text(
        attributes,
        "telemetry-trace-api-url",
        telemetry.trace_api_url.clone(),
    );
    push_optional_text(
        attributes,
        "telemetry-span-ui-url",
        telemetry.span_ui_url.clone(),
    );
    push_optional_text(
        attributes,
        "telemetry-span-api-url",
        telemetry.span_api_url.clone(),
    );
}

fn telemetry_subjects(telemetry: &TelemetryCorrelation) -> Vec<guest::EntityRef> {
    let provider = telemetry.provider.as_deref();
    let mut subjects = Vec::new();
    if let Some(trace_id) = telemetry.trace_id.as_deref() {
        subjects.push(telemetry_subject("trace", provider, trace_id));
    }
    if let Some(span_id) = telemetry.span_id.as_deref() {
        subjects.push(telemetry_subject("span", provider, span_id));
    }
    if let Some(session_id) = telemetry.session_id.as_deref() {
        subjects.push(telemetry_subject("session", provider, session_id));
    }
    if let Some(session_instance_id) = telemetry.session_instance_id.as_deref() {
        subjects.push(telemetry_subject(
            "session-instance",
            provider,
            session_instance_id,
        ));
    }
    subjects
}

fn telemetry_subject(kind: &str, provider: Option<&str>, id: &str) -> guest::EntityRef {
    let id = provider
        .filter(|provider| !provider.trim().is_empty())
        .map(|provider| format!("{provider}:{id}"))
        .unwrap_or_else(|| id.into());
    guest::EntityRef::External(guest::EntityAddress {
        namespace: "telemetry".into(),
        kind: kind.into(),
        id,
    })
}

fn field(name: &str, value_type: guest::ValueType, required: bool) -> guest::FieldDefinition {
    guest::FieldDefinition {
        name: name.into(),
        schema: guest::FieldSchema {
            value_type,
            required,
        },
    }
}

fn text_attribute(name: &str, value: String) -> guest::Attribute {
    guest::Attribute {
        name: name.into(),
        value: guest::Value::StringValue(value),
    }
}

fn integer_attribute(name: &str, value: String) -> guest::Attribute {
    guest::Attribute {
        name: name.into(),
        value: guest::Value::IntegerText(value),
    }
}

fn bool_attribute(name: &str, value: bool) -> guest::Attribute {
    guest::Attribute {
        name: name.into(),
        value: guest::Value::Boolean(value),
    }
}

fn list_attribute(name: &str, values: Vec<String>) -> guest::Attribute {
    guest::Attribute {
        name: name.into(),
        value: guest::Value::ListValue(
            values
                .into_iter()
                .map(guest::ScalarValue::StringValue)
                .collect(),
        ),
    }
}

fn push_optional_text(attributes: &mut Vec<guest::Attribute>, name: &str, value: Option<String>) {
    if let Some(value) = value {
        attributes.push(text_attribute(name, value));
    }
}

fn push_optional_integer(
    attributes: &mut Vec<guest::Attribute>,
    name: &str,
    value: Option<String>,
) {
    if let Some(value) = value {
        attributes.push(integer_attribute(name, value));
    }
}

fn push_optional_bool(attributes: &mut Vec<guest::Attribute>, name: &str, value: Option<bool>) {
    if let Some(value) = value {
        attributes.push(bool_attribute(name, value));
    }
}

fn push_optional_list(attributes: &mut Vec<guest::Attribute>, name: &str, values: Vec<String>) {
    if !values.is_empty() {
        attributes.push(list_attribute(name, values));
    }
}

fn required_string(
    attributes: &mut BTreeMap<String, guest::Value>,
    name: &str,
) -> Result<String, guest::PluginError> {
    optional_string(attributes, name)?
        .ok_or_else(|| invalid_request("required attribute is missing"))
}

fn optional_string(
    attributes: &mut BTreeMap<String, guest::Value>,
    name: &str,
) -> Result<Option<String>, guest::PluginError> {
    match attributes.remove(name) {
        None => Ok(None),
        Some(guest::Value::StringValue(value)) => Ok(Some(value)),
        Some(_) => Err(invalid_request("attribute must be a string")),
    }
}

fn optional_integer(
    attributes: &mut BTreeMap<String, guest::Value>,
    name: &str,
) -> Result<Option<String>, guest::PluginError> {
    match attributes.remove(name) {
        None => Ok(None),
        Some(guest::Value::IntegerText(value)) => Ok(Some(value)),
        Some(guest::Value::StringValue(value)) => Ok(Some(value)),
        Some(_) => Err(invalid_request("attribute must be integer text")),
    }
}

fn optional_bool(
    attributes: &mut BTreeMap<String, guest::Value>,
    name: &str,
) -> Result<Option<bool>, guest::PluginError> {
    match attributes.remove(name) {
        None => Ok(None),
        Some(guest::Value::Boolean(value)) => Ok(Some(value)),
        Some(_) => Err(invalid_request("attribute must be boolean")),
    }
}

fn optional_string_list(
    attributes: &mut BTreeMap<String, guest::Value>,
    name: &str,
) -> Result<Vec<String>, guest::PluginError> {
    match attributes.remove(name) {
        None => Ok(Vec::new()),
        Some(guest::Value::ListValue(values)) => values
            .into_iter()
            .map(|value| match value {
                guest::ScalarValue::StringValue(value) => Ok(value),
                _ => Err(invalid_request("list attribute must contain strings")),
            })
            .collect(),
        Some(guest::Value::StringValue(value)) => Ok(vec![value]),
        Some(_) => Err(invalid_request("list attribute must be a string list")),
    }
}

fn observation_seed(source: &guest::EntityAddress, cursor: Option<&str>) -> String {
    let mut seed = String::from("agent-atof/v1\0");
    append_part(&mut seed, &source.namespace);
    append_part(&mut seed, &source.kind);
    append_part(&mut seed, &source.id);
    append_part(&mut seed, cursor.unwrap_or(""));
    seed
}

fn append_part(seed: &mut String, value: &str) {
    seed.push_str(&value.len().to_string());
    seed.push(':');
    seed.push_str(value);
    seed.push('\0');
}

fn invalid_request(message: &str) -> guest::PluginError {
    guest::PluginError {
        kind: guest::PluginErrorKind::InvalidRequest,
        message: message.to_string(),
    }
}

export!(AgentPlugin);
