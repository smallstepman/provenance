use std::collections::{BTreeMap, BTreeSet};

use crate::config::{
    RelayCaptureMode, RelayIdentityConfig, RelayTelemetryUrlConfig, RelayTelemetryUrls,
};
use crate::error::ProjectionError;
use crate::identity::AgentIdentity;
use crate::spool::EvidenceLocator;
use nemo_relay_plugin::{Event, ScopeCategory};
use provenance_core::{EntityAddress, IdentityScheme, PluginModel};
use provenance_plugin::bindings;
use serde_json::Value as Json;
use sha2::Digest;
use uuid::Uuid;

/// Namespace and external entity kind owned by the generic agent projection.
pub const AGENT_NAMESPACE: &str = "agent";
/// External entity kind used for one ATOF lifecycle event in audit mode.
pub const AGENT_EVENT_KIND: &str = "event";
/// External entity kind used for one source attribution link.
pub const AGENT_LINK_KIND: &str = "attribution";
/// External entity kind used for a captured session lifecycle boundary.
pub const AGENT_SESSION_EVENT_KIND: &str = "session-lifecycle";

#[derive(Clone, Debug)]
struct ProvenanceSource {
    address: EntityAddress<PluginModel>,
    outcome: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct TelemetryCorrelation {
    provider: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    session_id: Option<String>,
    session_instance_id: Option<String>,
    urls: RelayTelemetryUrls,
}

struct ProjectionContext {
    session: Option<String>,
    session_id: Option<String>,
    actor: Option<String>,
    actor_id: Option<String>,
    telemetry: TelemetryCorrelation,
}

/// Stateful, harness-neutral ATOF projector.
///
/// Link-only capture is the default. It records only observations carrying an
/// explicit `metadata.provenance.source` reference, plus the end boundary of a
/// session that produced one. Audit mode retains the previous all-event
/// projection for explicit compliance or debugging use.
///
/// A source reference has the shape
/// `{ "namespace": "...", "kind": "...", "id": "..." }`.
/// Optional `outcome`, `trace_id`/`trace-id`, and `provider` fields are copied
/// as bounded provenance attributes. Complete event payloads remain in the
/// configured evidence spool when one is enabled; only its byte locator and
/// digest enter the graph.
#[derive(Clone, Debug)]
pub struct RelayProjector {
    identity: RelayIdentityConfig,
    capture_mode: RelayCaptureMode,
    telemetry_provider: Option<String>,
    telemetry_urls: RelayTelemetryUrlConfig,
    scope_sessions: BTreeMap<Uuid, String>,

    session_actors: BTreeMap<String, String>,
    telemetry_traces: BTreeMap<Uuid, String>,
    telemetry_spans: BTreeMap<Uuid, String>,
    telemetry_sessions: BTreeMap<Uuid, String>,
    telemetry_session_instances: BTreeMap<Uuid, String>,
    last_session_sources: BTreeMap<String, EntityAddress<PluginModel>>,
    captured_sessions: BTreeSet<String>,
}

impl RelayProjector {
    /// Create a sparse provenance projector.
    pub fn new(identity: RelayIdentityConfig) -> Self {
        Self::with_capture_mode(identity, RelayCaptureMode::LinkOnly)
    }

    /// Create a projector with an explicit capture policy.
    pub fn with_capture_mode(
        identity: RelayIdentityConfig,
        capture_mode: RelayCaptureMode,
    ) -> Self {
        Self::with_capture_mode_and_telemetry(identity, capture_mode, None)
    }

    /// Create a projector with capture and telemetry correlation policies.
    pub fn with_capture_mode_and_telemetry(
        identity: RelayIdentityConfig,
        capture_mode: RelayCaptureMode,
        telemetry_provider: Option<String>,
    ) -> Self {
        Self::with_capture_mode_and_telemetry_urls(
            identity,
            capture_mode,
            telemetry_provider,
            RelayTelemetryUrlConfig::default(),
        )
    }

    /// Create a projector with telemetry URL templates.
    pub fn with_capture_mode_and_telemetry_urls(
        identity: RelayIdentityConfig,
        capture_mode: RelayCaptureMode,
        telemetry_provider: Option<String>,
        telemetry_urls: RelayTelemetryUrlConfig,
    ) -> Self {
        Self {
            identity,
            capture_mode,
            telemetry_provider,
            telemetry_urls,
            scope_sessions: BTreeMap::new(),
            session_actors: BTreeMap::new(),
            telemetry_traces: BTreeMap::new(),
            telemetry_spans: BTreeMap::new(),
            telemetry_sessions: BTreeMap::new(),
            telemetry_session_instances: BTreeMap::new(),
            last_session_sources: BTreeMap::new(),
            captured_sessions: BTreeSet::new(),
        }
    }

    /// Return the configured capture policy.
    pub fn capture_mode(&self) -> RelayCaptureMode {
        self.capture_mode
    }

    /// Return the immutable source key for an ATOF event emission.
    pub fn event_key(event: &Event) -> String {
        format!("{}/{}", event.uuid(), lifecycle(event))
    }

    /// Return the source address for the projected event.
    ///
    /// Audit events are keyed by their ATOF lifecycle identity. Link-only
    /// events are keyed by the source operation they explain, making repeated
    /// observations of the same source fact idempotent across restarts.
    pub fn source_address(&self, event: &Event) -> EntityAddress<PluginModel> {
        match self.capture_mode {
            RelayCaptureMode::Audit => {
                EntityAddress::new(AGENT_NAMESPACE, AGENT_EVENT_KIND, Self::event_key(event))
            }
            RelayCaptureMode::LinkOnly => {
                if let Some(source) = self.provenance_source(event) {
                    EntityAddress::new(
                        AGENT_NAMESPACE,
                        AGENT_LINK_KIND,
                        source_link_id(&source.address),
                    )
                } else if let Some(session) = self.link_session_end(event) {
                    EntityAddress::new(
                        AGENT_NAMESPACE,
                        AGENT_SESSION_EVENT_KIND,
                        session_link_id(&session),
                    )
                } else {
                    EntityAddress::new(AGENT_NAMESPACE, AGENT_LINK_KIND, Self::event_key(event))
                }
            }
        }
    }

    /// Return deterministic source-operation parents for one projected event.
    ///
    /// Audit mode preserves ATOF scope ancestry. Link-only mode keeps only the
    /// previous provenance-relevant source fact in the same session; ignored
    /// telemetry scopes never become causal provenance parents.
    pub fn source_parents(&self, event: &Event) -> Vec<EntityAddress<PluginModel>> {
        let mut parents = BTreeSet::new();
        if self.capture_mode == RelayCaptureMode::Audit {
            if event.is_scope_end() {
                parents.insert(self.scope_start_address(event.uuid()));
            }
            if let Some(parent_uuid) = event.parent_uuid() {
                parents.insert(self.scope_start_address(parent_uuid));
            }
        }
        if let Some(session) = self.session_for_event(event)
            && let Some(previous) = self.last_session_sources.get(&session)
        {
            let current = self.source_address(event);
            if previous != &current {
                parents.insert(previous.clone());
            }
        }
        parents.into_iter().collect()
    }

    /// Record a successfully committed event as the session's latest source.
    pub fn remember_event(&mut self, event: &Event) {
        if self.capture_mode == RelayCaptureMode::LinkOnly && !self.is_link_relevant(event) {
            return;
        }
        if let Some(session) = self.session_for_event(event) {
            self.last_session_sources
                .insert(session.clone(), self.source_address(event));
            if self.provenance_source(event).is_some() {
                self.captured_sessions.insert(session);
            }
        }
    }

    /// Project one ATOF event into the checked-in provenance WIT request.
    ///
    /// `None` means the event remains telemetry/evidence only under the
    /// configured sparse capture policy.
    pub fn project(
        &mut self,
        event: &Event,
        evidence: Option<&EvidenceLocator>,
    ) -> Result<Option<bindings::ObservationRequest>, ProjectionError> {
        let session = self.resolve_session(event);
        let session_id = session
            .as_ref()
            .map(|seed| AgentIdentity.session(seed).raw().clone());
        let actor = self.resolve_actor(event, session.as_ref());
        let actor_id = actor
            .as_ref()
            .map(|seed| AgentIdentity.actor(seed).raw().clone());
        let telemetry = self.resolve_telemetry(event);
        let context = ProjectionContext {
            session,
            session_id,
            actor,
            actor_id,
            telemetry,
        };

        match self.capture_mode {
            RelayCaptureMode::Audit => Ok(Some(self.project_audit(event, evidence, &context)?)),
            RelayCaptureMode::LinkOnly => self.project_link(event, evidence, &context),
        }
    }

    fn project_audit(
        &self,
        event: &Event,
        evidence: Option<&EvidenceLocator>,
        context: &ProjectionContext,
    ) -> Result<bindings::ObservationRequest, ProjectionError> {
        let session_start = self.is_root_agent_start(event);
        let session_end = self.is_root_agent_end(event);

        let mut attributes = Vec::new();
        string_attribute(&mut attributes, "projection-mode", "audit");
        string_attribute(&mut attributes, "event-kind", event.kind());
        string_attribute(&mut attributes, "event-id", event.uuid().to_string());
        string_attribute(&mut attributes, "atof-version", atof_version(event));
        string_attribute(&mut attributes, "name", event.name());
        string_attribute(&mut attributes, "timestamp", event.timestamp().to_rfc3339());
        string_attribute(&mut attributes, "harness-id", &self.identity.harness_id);
        string_attribute(
            &mut attributes,
            "installation-id",
            &self.identity.installation_id,
        );
        if let Some(parent_uuid) = event.parent_uuid() {
            string_attribute(&mut attributes, "parent-uuid", parent_uuid.to_string());
        }
        if let Some(category) = event.category() {
            string_attribute(&mut attributes, "category", category.as_str());
        }
        if let Some(scope_category) = event.scope_category() {
            string_attribute(
                &mut attributes,
                "scope-category",
                match scope_category {
                    ScopeCategory::Start => "start",
                    ScopeCategory::End => "end",
                },
            );
        }
        if let Some(session) = &context.session {
            string_attribute(&mut attributes, "session-key", session);
        }
        if let Some(session_id) = &context.session_id {
            string_attribute(&mut attributes, "session-id", session_id);
        }
        if let Some(actor) = &context.actor {
            string_attribute(&mut attributes, "actor-key", actor);
        }
        if let Some(actor_id) = &context.actor_id {
            string_attribute(&mut attributes, "actor-id", actor_id);
        }
        if session_start {
            boolean_attribute(&mut attributes, "session-start", true);
        }
        if session_end {
            boolean_attribute(&mut attributes, "session-end", true);
        }
        if let Some(scope_attributes) = event.attributes()
            && !scope_attributes.is_empty()
        {
            list_attribute(&mut attributes, "scope-attributes", scope_attributes);
        }
        if let Some(model_name) = event.model_name() {
            string_attribute(&mut attributes, "model-name", model_name);
        }
        if let Some(tool_call_id) = event.tool_call_id() {
            string_attribute(&mut attributes, "tool-call-id", tool_call_id);
        }
        if let Some(profile) = event.category_profile()
            && !profile.extra.is_empty()
        {
            let keys = profile.extra.keys().cloned().collect::<Vec<_>>();
            list_attribute(&mut attributes, "profile-extra-keys", &keys);
        }

        append_json_digest(&mut attributes, "data", event.data())?;
        append_json_digest(&mut attributes, "metadata", event.metadata())?;
        if let Some(schema) = event.data_schema() {
            string_attribute(&mut attributes, "data-schema-name", &schema.name);
            string_attribute(&mut attributes, "data-schema-version", &schema.version);
        }
        for (attribute, keys) in [
            ("trace-id", TRACE_KEYS),
            ("span-id", SPAN_KEYS),
            ("provider", PROVIDER_KEYS),
        ] {
            if let Some(value) = metadata_string(event.metadata(), keys) {
                string_attribute(&mut attributes, attribute, value);
            }
        }
        append_telemetry_attributes(&mut attributes, &context.telemetry);
        if let Some(evidence) = evidence {
            string_attribute(&mut attributes, "evidence-location", &evidence.path);
            integer_attribute(&mut attributes, "evidence-offset", evidence.byte_offset);
            integer_attribute(&mut attributes, "evidence-length", evidence.byte_length);
            string_attribute(&mut attributes, "evidence-digest", &evidence.digest);
        }

        Ok(bindings::ObservationRequest {
            source: bindings::EntityAddress {
                namespace: AGENT_NAMESPACE.into(),
                kind: AGENT_EVENT_KIND.into(),
                id: Self::event_key(event),
            },
            cursor: Some(format!("atof/0.1/{}", Self::event_key(event))),
            attributes,
        })
    }

    fn project_link(
        &self,
        event: &Event,
        evidence: Option<&EvidenceLocator>,
        context: &ProjectionContext,
    ) -> Result<Option<bindings::ObservationRequest>, ProjectionError> {
        let source = self.provenance_source(event);
        let session_end = self.link_session_end(event).is_some();
        if source.is_none() && !session_end {
            return Ok(None);
        }
        let session_start = source.is_some()
            && context
                .session
                .as_ref()
                .is_some_and(|session| !self.captured_sessions.contains(session));

        let source_address = self.source_address(event);
        let mut attributes = Vec::new();
        string_attribute(&mut attributes, "projection-mode", "link");
        string_attribute(&mut attributes, "event-kind", event.kind());
        string_attribute(&mut attributes, "event-id", event.uuid().to_string());
        string_attribute(&mut attributes, "atof-version", atof_version(event));
        string_attribute(&mut attributes, "name", event.name());
        string_attribute(&mut attributes, "timestamp", event.timestamp().to_rfc3339());
        string_attribute(&mut attributes, "harness-id", &self.identity.harness_id);
        string_attribute(
            &mut attributes,
            "installation-id",
            &self.identity.installation_id,
        );
        if let Some(category) = event.category() {
            string_attribute(&mut attributes, "category", category.as_str());
        }
        if let Some(scope_category) = event.scope_category() {
            string_attribute(
                &mut attributes,
                "scope-category",
                match scope_category {
                    ScopeCategory::Start => "start",
                    ScopeCategory::End => "end",
                },
            );
        }
        if let Some(session) = &context.session {
            // These are consumed by the trusted WASM adapter to derive IDs;
            // link-mode output never persists the seed.
            string_attribute(&mut attributes, "session-key", session);
        }
        if let Some(session_id) = &context.session_id {
            string_attribute(&mut attributes, "session-id", session_id);
        }
        if let Some(actor) = &context.actor {
            string_attribute(&mut attributes, "actor-key", actor);
        }
        if let Some(actor_id) = &context.actor_id {
            string_attribute(&mut attributes, "actor-id", actor_id);
        }
        if session_start {
            boolean_attribute(&mut attributes, "session-start", true);
        }
        if session_end {
            boolean_attribute(&mut attributes, "session-end", true);
        }
        if let Some(source) = &source {
            string_attribute(
                &mut attributes,
                "source-namespace",
                source.address.namespace.as_str(),
            );
            string_attribute(&mut attributes, "source-kind", source.address.kind.as_str());
            string_attribute(&mut attributes, "source-id", source.address.id.as_str());
            if let Some(outcome) = &source.outcome {
                string_attribute(&mut attributes, "outcome", outcome);
            }
        }
        if let Some(trace_id) = metadata_string(event.metadata(), TRACE_KEYS) {
            string_attribute(&mut attributes, "trace-id", trace_id);
        }
        if let Some(span_id) = metadata_string(event.metadata(), SPAN_KEYS) {
            string_attribute(&mut attributes, "span-id", span_id);
        }
        if let Some(provider) = metadata_string(event.metadata(), PROVIDER_KEYS) {
            string_attribute(&mut attributes, "trace-provider", provider);
        }
        append_telemetry_attributes(&mut attributes, &context.telemetry);
        if let Some(evidence) = evidence {
            string_attribute(&mut attributes, "evidence-ref", &evidence.path);
            integer_attribute(&mut attributes, "evidence-offset", evidence.byte_offset);
            integer_attribute(&mut attributes, "evidence-length", evidence.byte_length);
            string_attribute(&mut attributes, "evidence-digest", &evidence.digest);
        }

        Ok(Some(bindings::ObservationRequest {
            source: bindings::EntityAddress {
                namespace: source_address.namespace.as_str().into(),
                kind: source_address.kind.as_str().into(),
                id: source_address.id.clone(),
            },
            cursor: Some(format!("provenance/link/v1/{}", source_address.id)),
            attributes,
        }))
    }

    fn session_for_event(&self, event: &Event) -> Option<String> {
        self.scope_sessions
            .get(&event.uuid())
            .or_else(|| {
                event
                    .parent_uuid()
                    .and_then(|parent_uuid| self.scope_sessions.get(&parent_uuid))
            })
            .cloned()
            .or_else(|| {
                metadata_string(event.metadata(), SESSION_KEYS)
                    .map(|raw_session| self.identity.session_seed(&raw_session))
            })
    }

    fn resolve_session(&mut self, event: &Event) -> Option<String> {
        let scope_uuid = event.uuid();

        if let Some(session) = self.scope_sessions.get(&scope_uuid) {
            return Some(session.clone());
        }

        if let Some(raw_session) = metadata_string(event.metadata(), SESSION_KEYS) {
            let session = self.identity.session_seed(&raw_session);
            let is_root_agent_scope = event.is_scope_start() && self.is_root_agent_scope(event);
            if is_scope_event(event) || is_session_start(event) {
                self.scope_sessions.insert(scope_uuid, session.clone());
                // A Relay root can be nested under an unannotated internal scope.
                // Keep that parent outside the session so the root end remains
                // recognizable as the session boundary.
                if let Some(parent_uuid) = event.parent_uuid()
                    && !is_root_agent_scope
                {
                    self.scope_sessions.insert(parent_uuid, session.clone());
                }
            }
            return Some(session);
        }

        if let Some(parent_uuid) = event.parent_uuid()
            && let Some(session) = self.scope_sessions.get(&parent_uuid)
        {
            let session = session.clone();
            if is_scope_event(event) {
                self.scope_sessions.insert(scope_uuid, session.clone());
            }
            return Some(session);
        }

        if self.is_root_agent_scope(event) {
            let session = self.identity.fallback_session_seed(&scope_uuid.to_string());
            self.scope_sessions.insert(scope_uuid, session.clone());
            return Some(session);
        }

        None
    }
    fn resolve_telemetry(&mut self, event: &Event) -> TelemetryCorrelation {
        let event_uuid = event.uuid();
        let parent_uuid = event.parent_uuid();
        let session_id = metadata_string(event.metadata(), SESSION_KEYS)
            .or_else(|| self.telemetry_sessions.get(&event_uuid).cloned())
            .or_else(|| {
                parent_uuid
                    .and_then(|parent_uuid| self.telemetry_sessions.get(&parent_uuid).cloned())
            });
        let session_instance_id = metadata_string(event.metadata(), SESSION_INSTANCE_KEYS)
            .or_else(|| self.telemetry_session_instances.get(&event_uuid).cloned())
            .or_else(|| {
                parent_uuid.and_then(|parent_uuid| {
                    self.telemetry_session_instances.get(&parent_uuid).cloned()
                })
            });

        if let Some(session_id) = &session_id {
            self.telemetry_sessions
                .insert(event_uuid, session_id.clone());
            if let Some(parent_uuid) = parent_uuid {
                self.telemetry_sessions
                    .entry(parent_uuid)
                    .or_insert_with(|| session_id.clone());
            }
        }
        if let Some(session_instance_id) = &session_instance_id {
            self.telemetry_session_instances
                .insert(event_uuid, session_instance_id.clone());
            if let Some(parent_uuid) = parent_uuid {
                self.telemetry_session_instances
                    .entry(parent_uuid)
                    .or_insert_with(|| session_instance_id.clone());
            }
        }

        // Relay's OTel ID generator derives a trace ID from the UUID of a
        // scope that actually starts a span. A session-instance UUID is only
        // lifecycle metadata: it is not an OTel span and must never become a
        // synthetic trace parent. Marks inherit the containing scope's
        // correlation when one is known; otherwise they have no span
        // reference (GenAI exporters omit orphan marks).
        let trace_id = metadata_string(event.metadata(), TELEMETRY_TRACE_KEYS)
            .or_else(|| self.telemetry_traces.get(&event_uuid).cloned())
            .or_else(|| {
                parent_uuid.and_then(|parent_uuid| self.telemetry_traces.get(&parent_uuid).cloned())
            })
            .or_else(|| event.is_scope_start().then(|| relay_trace_id(event_uuid)));
        let span_id = metadata_string(event.metadata(), TELEMETRY_SPAN_KEYS)
            .or_else(|| self.telemetry_spans.get(&event_uuid).cloned())
            .or_else(|| event.is_scope_start().then(|| relay_span_id(event_uuid)))
            .or_else(|| {
                parent_uuid.and_then(|parent_uuid| self.telemetry_spans.get(&parent_uuid).cloned())
            });

        if let Some(trace_id) = &trace_id {
            self.telemetry_traces.insert(event_uuid, trace_id.clone());
        }
        if is_scope_event(event)
            && let Some(span_id) = &span_id
        {
            self.telemetry_spans.insert(event_uuid, span_id.clone());
        }

        let provider = self
            .telemetry_provider
            .clone()
            .or_else(|| metadata_string(event.metadata(), TELEMETRY_PROVIDER_KEYS));
        let urls = self.telemetry_urls.render(
            provider.as_deref(),
            trace_id.as_deref(),
            span_id.as_deref(),
        );

        TelemetryCorrelation {
            provider,
            trace_id,
            span_id,
            session_id,
            session_instance_id,
            urls,
        }
    }

    fn resolve_actor(&mut self, event: &Event, session: Option<&String>) -> Option<String> {
        let session = session?;
        if let Some(raw_actor) = metadata_string(event.metadata(), ACTOR_KEYS) {
            let actor_seed = format!("provenance-agent/actor/v1\0{session}\0{raw_actor}");
            let actor = self.identity.actor_seed(&actor_seed);
            self.session_actors.insert(session.clone(), actor.clone());
            return Some(actor);
        }
        if let Some(actor) = self.session_actors.get(session) {
            return Some(actor.clone());
        }
        let actor_seed = format!("provenance-agent/actor/v1\0{session}\0agent");
        let actor = self.identity.actor_seed(&actor_seed);
        self.session_actors.insert(session.clone(), actor.clone());
        Some(actor)
    }

    fn provenance_source(&self, event: &Event) -> Option<ProvenanceSource> {
        let context = provenance_metadata(event.metadata())?;
        let source = context
            .get("source")
            .or_else(|| context.get("source_ref"))?;
        let Json::Object(source) = source else {
            return None;
        };
        let namespace = object_string(source, &["namespace"])?;
        let kind = object_string(source, &["kind"])?;
        let id = object_string(source, &["id"])?;
        if namespace.trim().is_empty() || kind.trim().is_empty() || id.trim().is_empty() {
            return None;
        }
        Some(ProvenanceSource {
            address: EntityAddress::new(namespace, kind, id),
            outcome: object_string(context, &["outcome", "status"]),
        })
    }

    fn is_link_relevant(&self, event: &Event) -> bool {
        self.provenance_source(event).is_some() || self.link_session_end(event).is_some()
    }

    fn link_session_end(&self, event: &Event) -> Option<String> {
        if !self.is_root_agent_end(event) {
            return None;
        }
        let session = self.session_for_event(event)?;
        self.captured_sessions.contains(&session).then_some(session)
    }

    fn is_root_agent_scope(&self, event: &Event) -> bool {
        matches!(event, Event::Scope(_))
            && event
                .category()
                .is_some_and(|category| category.as_str() == "agent")
            && event
                .parent_uuid()
                .is_none_or(|parent_uuid| !self.scope_sessions.contains_key(&parent_uuid))
    }

    fn is_root_agent_start(&self, event: &Event) -> bool {
        is_session_start(event) || (event.is_scope_start() && self.is_root_agent_scope(event))
    }

    fn is_root_agent_end(&self, event: &Event) -> bool {
        is_session_end(event) || (event.is_scope_end() && self.is_root_agent_scope(event))
    }

    fn scope_start_address(&self, uuid: Uuid) -> EntityAddress<PluginModel> {
        EntityAddress::new(AGENT_NAMESPACE, AGENT_EVENT_KIND, format!("{uuid}/start"))
    }
}

const SESSION_KEYS: &[&str] = &["session_id", "session-id", "session_key", "session-key"];
const SESSION_INSTANCE_KEYS: &[&str] = &[
    "session_instance_id",
    "session-instance-id",
    "session_instance",
    "session-instance",
];
const HOOK_EVENT_KEYS: &[&str] = &["hook_event_name", "hook-event-name"];
const ACTOR_KEYS: &[&str] = &["actor_id", "actor-id", "actor_key", "actor-key"];
const TRACE_KEYS: &[&str] = &["trace_id", "trace-id"];
const SPAN_KEYS: &[&str] = &["span_id", "span-id"];
const PROVIDER_KEYS: &[&str] = &["provider", "provider_name", "provider-name"];
const TELEMETRY_TRACE_KEYS: &[&str] = &[
    "telemetry_trace_id",
    "telemetry-trace-id",
    "otel_trace_id",
    "otel-trace-id",
];
const TELEMETRY_SPAN_KEYS: &[&str] = &[
    "telemetry_span_id",
    "telemetry-span-id",
    "otel_span_id",
    "otel-span-id",
];
const TELEMETRY_PROVIDER_KEYS: &[&str] = &[
    "telemetry_provider",
    "telemetry-provider",
    "otel_provider",
    "otel-provider",
];
const PROVENANCE_CONTAINERS: &[&str] = &["provenance", "provenance_context"];

fn lifecycle(event: &Event) -> &'static str {
    match event {
        Event::Scope(scope) => match scope.scope_category {
            ScopeCategory::Start => "start",
            ScopeCategory::End => "end",
        },
        Event::Mark(_) => "mark",
    }
}

fn atof_version(event: &Event) -> &str {
    match event {
        Event::Scope(scope) => &scope.base.atof_version,
        Event::Mark(mark) => &mark.base.atof_version,
    }
}

fn is_scope_event(event: &Event) -> bool {
    matches!(event, Event::Scope(_))
}

fn is_session_start(event: &Event) -> bool {
    matches!(event, Event::Mark(_))
        && (event.name() == "session.start"
            || metadata_string(event.metadata(), HOOK_EVENT_KEYS)
                .is_some_and(|hook_event| hook_event == "SessionStart"))
}

fn is_session_end(event: &Event) -> bool {
    matches!(event, Event::Mark(_))
        && (event.name() == "session.end"
            || metadata_string(event.metadata(), HOOK_EVENT_KEYS)
                .is_some_and(|hook_event| hook_event == "SessionEnd"))
}

fn metadata_string(metadata: Option<&Json>, keys: &[&str]) -> Option<String> {
    let Json::Object(object) = metadata? else {
        return None;
    };
    find_string(object, keys).or_else(|| {
        [
            "agent",
            "session",
            "context",
            "telemetry",
            "provenance",
            "provenance_context",
        ]
        .into_iter()
        .filter_map(|container| object.get(container))
        .find_map(|value| match value {
            Json::Object(nested) => find_string(nested, keys),
            _ => None,
        })
    })
}
fn provenance_metadata(metadata: Option<&Json>) -> Option<&serde_json::Map<String, Json>> {
    let Json::Object(object) = metadata? else {
        return None;
    };
    PROVENANCE_CONTAINERS.iter().find_map(|container| {
        object.get(*container).and_then(|value| match value {
            Json::Object(nested) => Some(nested),
            _ => None,
        })
    })
}

fn object_string(object: &serde_json::Map<String, Json>, keys: &[&str]) -> Option<String> {
    find_string(object, keys)
}

fn source_link_id(source: &EntityAddress<PluginModel>) -> String {
    digest_id(
        "provenance-agent/link/v1",
        &[
            source.namespace.as_str(),
            source.kind.as_str(),
            source.id.as_str(),
        ],
    )
}

fn session_link_id(session: &str) -> String {
    digest_id("provenance-agent/session-end/v1", &[session])
}

fn digest_id(prefix: &str, parts: &[&str]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(prefix.as_bytes());

    hasher.update([0]);
    for part in parts {
        hasher.update(part.len().to_string().as_bytes());
        hasher.update(*b":");
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn find_string(object: &serde_json::Map<String, Json>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Json::as_str))
        .map(ToOwned::to_owned)
}

fn relay_trace_id(uuid: Uuid) -> String {
    uuid.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn relay_span_id(uuid: Uuid) -> String {
    uuid.as_bytes()[8..]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn append_telemetry_attributes(
    attributes: &mut Vec<bindings::Attribute>,
    telemetry: &TelemetryCorrelation,
) {
    if let Some(provider) = &telemetry.provider {
        string_attribute(attributes, "telemetry-provider", provider);
    }
    if let Some(trace_id) = &telemetry.trace_id {
        string_attribute(attributes, "telemetry-trace-id", trace_id);
    }
    if let Some(span_id) = &telemetry.span_id {
        string_attribute(attributes, "telemetry-span-id", span_id);
    }
    if let Some(session_id) = &telemetry.session_id {
        string_attribute(attributes, "telemetry-session-id", session_id);
    }
    if let Some(session_instance_id) = &telemetry.session_instance_id {
        string_attribute(
            attributes,
            "telemetry-session-instance-id",
            session_instance_id,
        );
    }
    if let Some(url) = &telemetry.urls.trace_ui {
        string_attribute(attributes, "telemetry-trace-ui-url", url);
    }
    if let Some(url) = &telemetry.urls.trace_api {
        string_attribute(attributes, "telemetry-trace-api-url", url);
    }
    if let Some(url) = &telemetry.urls.span_ui {
        string_attribute(attributes, "telemetry-span-ui-url", url);
    }
    if let Some(url) = &telemetry.urls.span_api {
        string_attribute(attributes, "telemetry-span-api-url", url);
    }
}

fn string_attribute(attributes: &mut Vec<bindings::Attribute>, name: &str, value: impl AsRef<str>) {
    attributes.push(bindings::Attribute {
        name: name.into(),
        value: bindings::Value::StringValue(value.as_ref().into()),
    });
}

fn boolean_attribute(attributes: &mut Vec<bindings::Attribute>, name: &str, value: bool) {
    attributes.push(bindings::Attribute {
        name: name.into(),
        value: bindings::Value::Boolean(value),
    });
}

fn integer_attribute(attributes: &mut Vec<bindings::Attribute>, name: &str, value: u64) {
    attributes.push(bindings::Attribute {
        name: name.into(),
        value: bindings::Value::IntegerText(value.to_string()),
    });
}

fn list_attribute(attributes: &mut Vec<bindings::Attribute>, name: &str, values: &[String]) {
    attributes.push(bindings::Attribute {
        name: name.into(),
        value: bindings::Value::ListValue(
            values
                .iter()
                .cloned()
                .map(bindings::ScalarValue::StringValue)
                .collect(),
        ),
    });
}

fn append_json_digest(
    attributes: &mut Vec<bindings::Attribute>,
    field: &'static str,
    value: Option<&Json>,
) -> Result<(), ProjectionError> {
    let Some(value) = value else {
        return Ok(());
    };
    let bytes =
        serde_json::to_vec(value).map_err(|source| ProjectionError::Serialize { field, source })?;
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    let digest = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    string_attribute(attributes, &format!("{field}-digest"), digest);
    integer_attribute(attributes, &format!("{field}-size"), bytes.len() as u64);
    boolean_attribute(attributes, &format!("{field}-present"), true);
    Ok(())
}
