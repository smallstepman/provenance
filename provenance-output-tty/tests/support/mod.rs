use provenance_core::{
    Actor, ActorId, Availability, EntityAddress, EntityObservation, EntityRef, EntitySchema,
    EntityType, EntityTypePattern, Event, EventId, Fact, FieldName, FieldSchema, Integrity,
    NamedQueryDefinition, Operation, OperationId, QueryKey, QueryTemplate, Relation, RelationName,
    RelationSchema, RelationType, Resource, ResourceObservation, ResourceRequirement,
    RetentionClaim, RetentionOwner, RetentionStrength, SchemaDefinition, SchemaKey, Session,
    SessionId, State, Value, ValueType,
};
use std::collections::{BTreeMap, BTreeSet};

pub type FixtureModel = provenance_core::PluginModel;

pub struct ProvenanceFixture {
    pub state: State<FixtureModel>,
    ordered_operations: Vec<OperationId<FixtureModel>>,
}

impl ProvenanceFixture {
    pub fn new() -> Self {
        let beads_schema_key = SchemaKey::new("beads", "1");
        let jj_schema_key = SchemaKey::new("jj", "1");
        let agent_schema_key = SchemaKey::new("agent", "1");

        let issue = beads_issue();
        let parent_issue = beads_parent_issue();
        let jj_operation_address = jj_operation_address();
        let jj_parent_operation = address("jj", "operation", "jj-op-0000");
        let jj_commit = jj_commit_address();
        let jj_predecessor = address("jj", "commit", "deadbeef0000");
        let jj_change = address("jj", "change", "change-abc");
        let jj_workspace = address("jj", "workspace", "default");
        let agent_event = agent_event_address();

        let session = SessionId::<FixtureModel>::new("session-0001".to_owned());
        let actor = ActorId::<FixtureModel>::new("actor-operator".to_owned());
        let agent_event_id = EventId::<FixtureModel>::new("agent-event-0001".to_owned());

        let root_id = OperationId::<FixtureModel>::new("op-root-schemas".to_owned());
        let beads_operation_id = OperationId::<FixtureModel>::new("op-beads-update".to_owned());
        let jj_operation_id = OperationId::<FixtureModel>::new("op-jj-observation".to_owned());
        let agent_operation_id = OperationId::<FixtureModel>::new("op-agent-turn".to_owned());

        let root_operation = Operation {
            id: root_id.clone(),
            parents: BTreeSet::new(),
            source: None,
            facts: vec![
                Fact::SchemaRegistered(beads_schema(beads_schema_key.clone())),
                Fact::SchemaRegistered(jj_schema(jj_schema_key.clone())),
                Fact::SchemaRegistered(agent_schema(agent_schema_key.clone())),
                Fact::NamedQueryRegistered(jj_why_query()),
            ],
            attributes: BTreeMap::from([(
                FieldName::from("fixture"),
                text("interconnected beads, jj, and agent graph"),
            )]),
        };

        let beads_operation = Operation {
            id: beads_operation_id.clone(),
            parents: BTreeSet::from([root_id.clone()]),
            source: Some(address(
                "beads",
                "hook",
                "update/bd-42/2026-08-31T12:00:01Z",
            )),
            facts: vec![
                Fact::EntityObserved(EntityObservation {
                    entity: issue.clone(),
                    schema: beads_schema_key.clone(),
                    attributes: beads_issue_attributes(),
                }),
                Fact::EntityObserved(EntityObservation {
                    entity: parent_issue.clone(),
                    schema: beads_schema_key.clone(),
                    attributes: beads_parent_issue_attributes(),
                }),
                Fact::EventRecorded(Event {
                    id: EventId::<FixtureModel>::new("beads-event-0001".to_owned()),
                    session: None,
                    actor: None,
                    parents: BTreeSet::new(),
                    subjects: BTreeSet::from([
                        external(issue.clone()),
                        external(parent_issue.clone()),
                    ]),
                    relations: BTreeSet::from([
                        beads_relation(
                            beads_schema_key.clone(),
                            "depends-on",
                            issue.clone(),
                            parent_issue.clone(),
                        ),
                        beads_relation(
                            beads_schema_key.clone(),
                            "belongs-to",
                            issue.clone(),
                            parent_issue.clone(),
                        ),
                    ]),
                    requires: BTreeSet::new(),
                    attributes: BTreeMap::from([
                        (FieldName::from("hook"), text("update")),
                        (FieldName::from("issue-id"), text("bd-42")),
                    ]),
                }),
            ],
            attributes: BTreeMap::from([(FieldName::from("source-system"), text("bd"))]),
        };

        let jj_snapshot_operation = Operation {
            id: jj_operation_id.clone(),
            parents: BTreeSet::from([root_id.clone()]),
            source: Some(jj_operation_address.clone()),
            facts: vec![
                Fact::EntityObserved(EntityObservation {
                    entity: jj_operation_address.clone(),
                    schema: jj_schema_key.clone(),
                    attributes: BTreeMap::from([
                        (FieldName::from("operation_id"), text("jj-op-0001")),
                        (
                            FieldName::from("description"),
                            text("rebase and amend agent change"),
                        ),
                    ]),
                }),
                Fact::EntityObserved(EntityObservation {
                    entity: jj_commit.clone(),
                    schema: jj_schema_key.clone(),
                    attributes: jj_commit_attributes(
                        "deadbeef0001",
                        "change-abc",
                        "Implement agent provenance links",
                    ),
                }),
                Fact::EntityObserved(EntityObservation {
                    entity: jj_predecessor.clone(),
                    schema: jj_schema_key.clone(),
                    attributes: jj_commit_attributes(
                        "deadbeef0000",
                        "change-abc",
                        "Initial provenance fixture",
                    ),
                }),
                Fact::EntityObserved(EntityObservation {
                    entity: jj_change.clone(),
                    schema: jj_schema_key.clone(),
                    attributes: BTreeMap::from([
                        (FieldName::from("change_id"), text("change-abc")),
                        (
                            FieldName::from("current_commit"),
                            Value::Entity(external(jj_commit.clone())),
                        ),
                    ]),
                }),
                Fact::EntityObserved(EntityObservation {
                    entity: jj_workspace.clone(),
                    schema: jj_schema_key.clone(),
                    attributes: BTreeMap::from([(
                        FieldName::from("workspace_id"),
                        text("default"),
                    )]),
                }),
                Fact::EventRecorded(Event {
                    id: EventId::<FixtureModel>::new("jj-event-0001".to_owned()),
                    session: None,
                    actor: None,
                    parents: BTreeSet::new(),
                    subjects: BTreeSet::from([
                        external(jj_operation_address.clone()),
                        external(jj_commit.clone()),
                        external(jj_predecessor.clone()),
                        external(jj_change.clone()),
                        external(jj_workspace.clone()),
                    ]),
                    relations: BTreeSet::from([
                        jj_relation(
                            jj_schema_key.clone(),
                            "follows",
                            jj_operation_address.clone(),
                            jj_parent_operation,
                        ),
                        jj_relation(
                            jj_schema_key.clone(),
                            "produced",
                            jj_operation_address.clone(),
                            jj_commit.clone(),
                        ),
                        jj_relation(
                            jj_schema_key.clone(),
                            "predecessor-of",
                            jj_commit.clone(),
                            jj_predecessor,
                        ),
                        jj_relation(
                            jj_schema_key.clone(),
                            "belongs-to-change",
                            jj_commit.clone(),
                            jj_change.clone(),
                        ),
                        jj_relation(
                            jj_schema_key.clone(),
                            "contains",
                            jj_workspace.clone(),
                            jj_commit.clone(),
                        ),
                        jj_relation(
                            jj_schema_key,
                            "currently-at",
                            jj_workspace,
                            jj_commit.clone(),
                        ),
                    ]),
                    requires: BTreeSet::new(),
                    attributes: BTreeMap::from([(
                        FieldName::from("operation-kind"),
                        text("rebase"),
                    )]),
                }),
            ],
            attributes: BTreeMap::from([(FieldName::from("source-system"), text("jj"))]),
        };

        let agent_event_attributes = agent_event_attributes();
        let agent_event_resource = Resource {
            entity: external(agent_event.clone()),
        };
        let agent_operation = Operation {
            id: agent_operation_id.clone(),
            parents: BTreeSet::from([
                beads_operation_id.clone(),
                jj_operation_id.clone(),
                root_id.clone(),
            ]),
            source: Some(agent_event.clone()),
            facts: vec![
                Fact::SessionOpened(Session {
                    id: session.clone(),
                    parent: None,
                    attributes: BTreeMap::from([
                        (FieldName::from("harness-id"), text("codex")),
                        (FieldName::from("installation-id"), text("workstation-a")),
                    ]),
                }),
                Fact::ActorDeclared(Actor {
                    id: actor.clone(),
                    session: Some(session.clone()),
                    attributes: BTreeMap::from([
                        (FieldName::from("role"), text("operator")),
                        (FieldName::from("harness"), text("codex")),
                    ]),
                }),
                Fact::EntityObserved(EntityObservation {
                    entity: agent_event.clone(),
                    schema: agent_schema_key.clone(),
                    attributes: agent_event_attributes.clone(),
                }),
                Fact::EventRecorded(Event {
                    id: agent_event_id.clone(),
                    session: Some(session),
                    actor: Some(actor),
                    parents: BTreeSet::new(),
                    subjects: BTreeSet::from([
                        external(agent_event.clone()),
                        external(issue.clone()),
                        external(jj_commit.clone()),
                        external(jj_operation_address.clone()),
                    ]),
                    relations: BTreeSet::from([
                        agent_relation(
                            agent_schema_key.clone(),
                            "triggered-by",
                            agent_event.clone(),
                            issue.clone(),
                        ),
                        agent_relation(
                            agent_schema_key.clone(),
                            "observed-commit",
                            agent_event.clone(),
                            jj_commit.clone(),
                        ),
                        agent_relation(
                            agent_schema_key.clone(),
                            "correlates-with",
                            agent_event.clone(),
                            jj_operation_address.clone(),
                        ),
                        agent_relation(agent_schema_key, "implemented-by", issue, jj_commit),
                    ]),
                    requires: BTreeSet::from([ResourceRequirement {
                        resource: agent_event_resource.clone(),
                        strength: RetentionStrength::Escrowed,
                    }]),
                    attributes: agent_event_attributes,
                }),
                Fact::RetentionClaimed(RetentionClaim {
                    id: provenance_core::ClaimId::<FixtureModel>::new(
                        "claim-agent-event-0001".to_owned(),
                    ),
                    owner: RetentionOwner::Event(agent_event_id),
                    resource: agent_event_resource.clone(),
                    strength: RetentionStrength::Escrowed,
                }),
                Fact::ResourceObserved {
                    resource: agent_event_resource,
                    observation: ResourceObservation {
                        availability: Availability::Local,
                        integrity: Integrity::Verified,
                        observed_retention: Some(RetentionStrength::Escrowed),
                    },
                },
            ],
            attributes: BTreeMap::from([(FieldName::from("projection"), text("audit"))]),
        };

        let mut state = State::new();
        for operation in [
            root_operation,
            beads_operation,
            jj_snapshot_operation,
            agent_operation,
        ] {
            state
                .operations
                .declare(operation.id.clone(), operation)
                .expect("insert fixture operation");
        }
        state
            .rebuild()
            .expect("rebuild interconnected provenance state");

        Self {
            state,
            ordered_operations: vec![
                root_id,
                beads_operation_id,
                jj_operation_id,
                agent_operation_id,
            ],
        }
    }

    pub fn operations(&self) -> impl Iterator<Item = &Operation<FixtureModel>> {
        self.ordered_operations
            .iter()
            .map(|id| self.state.operation(id).expect("ordered operation exists"))
    }
}

pub fn beads_issue() -> EntityAddress<FixtureModel> {
    address("beads", "issue", "bd-42")
}

pub fn beads_parent_issue() -> EntityAddress<FixtureModel> {
    address("beads", "issue", "bd-17")
}

pub fn jj_commit_address() -> EntityAddress<FixtureModel> {
    address("jj", "commit", "deadbeef0001")
}

pub fn jj_operation_address() -> EntityAddress<FixtureModel> {
    address("jj", "operation", "jj-op-0001")
}

pub fn agent_event_address() -> EntityAddress<FixtureModel> {
    address("agent", "event", "agent-event-0001/start")
}

fn beads_schema(key: SchemaKey) -> SchemaDefinition {
    let fields = BTreeMap::from([
        (
            FieldName::from("issue_id"),
            FieldSchema::required(ValueType::String),
        ),
        (
            FieldName::from("title"),
            FieldSchema::required(ValueType::String),
        ),
        (
            FieldName::from("status"),
            FieldSchema::required(ValueType::String),
        ),
        (
            FieldName::from("priority"),
            FieldSchema::required(ValueType::Integer),
        ),
        (
            FieldName::from("issue_type"),
            FieldSchema::required(ValueType::String),
        ),
        (
            FieldName::from("created_at"),
            FieldSchema::required(ValueType::String),
        ),
        (
            FieldName::from("updated_at"),
            FieldSchema::required(ValueType::String),
        ),
        (
            FieldName::from("description"),
            FieldSchema::optional(ValueType::String),
        ),
        (
            FieldName::from("assignee"),
            FieldSchema::optional(ValueType::String),
        ),
        (
            FieldName::from("parent_id"),
            FieldSchema::optional(ValueType::String),
        ),
        (
            FieldName::from("labels"),
            FieldSchema::optional(ValueType::List(Box::new(ValueType::String))),
        ),
    ]);
    SchemaDefinition {
        key,
        requires: BTreeSet::new(),
        entities: BTreeMap::from([(
            provenance_core::EntityKind::from("issue"),
            EntitySchema {
                entity_type: EntityType::new("beads", "issue"),
                fields,
                allow_unknown_fields: false,
            },
        )]),
        relations: BTreeMap::from([
            (
                RelationName::from("depends-on"),
                cross_relation_schema(
                    ("beads", "depends-on"),
                    ("beads", "issue"),
                    ("beads", "issue"),
                    provenance_core::ExplanationRole::Supporting,
                    provenance_core::ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("belongs-to"),
                cross_relation_schema(
                    ("beads", "belongs-to"),
                    ("beads", "issue"),
                    ("beads", "issue"),
                    provenance_core::ExplanationRole::Supporting,
                    provenance_core::ExplanationDirection::FromExplainedByTo,
                ),
            ),
        ]),
    }
}

fn jj_schema(key: SchemaKey) -> SchemaDefinition {
    let commit_schema = EntitySchema {
        entity_type: EntityType::new("jj", "commit"),
        fields: BTreeMap::from([
            (
                FieldName::from("commit_id"),
                FieldSchema::required(ValueType::String),
            ),
            (
                FieldName::from("change_id"),
                FieldSchema::required(ValueType::String),
            ),
            (
                FieldName::from("description"),
                FieldSchema::required(ValueType::String),
            ),
            (
                FieldName::from("author"),
                FieldSchema::required(ValueType::String),
            ),
            (
                FieldName::from("timestamp"),
                FieldSchema::required(ValueType::String),
            ),
        ]),
        allow_unknown_fields: false,
    };
    let change_schema = EntitySchema {
        entity_type: EntityType::new("jj", "change"),
        fields: BTreeMap::from([
            (
                FieldName::from("change_id"),
                FieldSchema::required(ValueType::String),
            ),
            (
                FieldName::from("current_commit"),
                FieldSchema::required(ValueType::Entity(EntityTypePattern::External(
                    EntityType::new("jj", "commit"),
                ))),
            ),
        ]),
        allow_unknown_fields: false,
    };
    let operation_schema = EntitySchema {
        entity_type: EntityType::new("jj", "operation"),
        fields: BTreeMap::from([
            (
                FieldName::from("operation_id"),
                FieldSchema::required(ValueType::String),
            ),
            (
                FieldName::from("description"),
                FieldSchema::required(ValueType::String),
            ),
        ]),
        allow_unknown_fields: false,
    };
    let workspace_schema = EntitySchema {
        entity_type: EntityType::new("jj", "workspace"),
        fields: BTreeMap::from([(
            FieldName::from("workspace_id"),
            FieldSchema::required(ValueType::String),
        )]),
        allow_unknown_fields: false,
    };

    SchemaDefinition {
        key,
        requires: BTreeSet::new(),
        entities: BTreeMap::from([
            (provenance_core::EntityKind::from("commit"), commit_schema),
            (provenance_core::EntityKind::from("change"), change_schema),
            (
                provenance_core::EntityKind::from("operation"),
                operation_schema,
            ),
            (
                provenance_core::EntityKind::from("workspace"),
                workspace_schema,
            ),
        ]),
        relations: BTreeMap::from([
            (
                RelationName::from("belongs-to-change"),
                cross_relation_schema(
                    ("jj", "belongs-to-change"),
                    ("jj", "commit"),
                    ("jj", "change"),
                    provenance_core::ExplanationRole::Primary,
                    provenance_core::ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("predecessor-of"),
                cross_relation_schema(
                    ("jj", "predecessor-of"),
                    ("jj", "commit"),
                    ("jj", "commit"),
                    provenance_core::ExplanationRole::Primary,
                    provenance_core::ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("produced"),
                cross_relation_schema(
                    ("jj", "produced"),
                    ("jj", "operation"),
                    ("jj", "commit"),
                    provenance_core::ExplanationRole::Primary,
                    provenance_core::ExplanationDirection::ToExplainedByFrom,
                ),
            ),
            (
                RelationName::from("follows"),
                cross_relation_schema(
                    ("jj", "follows"),
                    ("jj", "operation"),
                    ("jj", "operation"),
                    provenance_core::ExplanationRole::Supporting,
                    provenance_core::ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("contains"),
                cross_relation_schema(
                    ("jj", "contains"),
                    ("jj", "workspace"),
                    ("jj", "commit"),
                    provenance_core::ExplanationRole::Contextual,
                    provenance_core::ExplanationDirection::ToExplainedByFrom,
                ),
            ),
            (
                RelationName::from("currently-at"),
                cross_relation_schema(
                    ("jj", "currently-at"),
                    ("jj", "workspace"),
                    ("jj", "commit"),
                    provenance_core::ExplanationRole::Contextual,
                    provenance_core::ExplanationDirection::ToExplainedByFrom,
                ),
            ),
        ]),
    }
}

fn agent_schema(key: SchemaKey) -> SchemaDefinition {
    let mut fields = BTreeMap::new();
    for name in [
        "event-id",
        "event-kind",
        "name",
        "timestamp",
        "harness-id",
        "installation-id",
        "category",
        "scope-category",
        "session-id",
        "actor-id",
        "model-name",
        "telemetry-provider",
        "telemetry-session-id",
        "telemetry-session-instance-id",
        "telemetry-trace-id",
        "telemetry-span-id",
        "conversation-ref",
        "evidence-ref",
        "evidence-digest",
        "outcome",
    ] {
        let schema = if matches!(
            name,
            "event-id" | "event-kind" | "name" | "timestamp" | "harness-id" | "installation-id"
        ) {
            FieldSchema::required(ValueType::String)
        } else {
            FieldSchema::optional(ValueType::String)
        };
        fields.insert(FieldName::from(name), schema);
    }
    for name in [
        "data-size",
        "metadata-size",
        "evidence-offset",
        "evidence-length",
    ] {
        fields.insert(
            FieldName::from(name),
            FieldSchema::optional(ValueType::Integer),
        );
    }
    for name in ["data-present", "metadata-present"] {
        fields.insert(
            FieldName::from(name),
            FieldSchema::optional(ValueType::Bool),
        );
    }
    fields.insert(
        FieldName::from("scope-attributes"),
        FieldSchema::optional(ValueType::List(Box::new(ValueType::String))),
    );

    SchemaDefinition {
        key,
        requires: BTreeSet::new(),
        entities: BTreeMap::from([(
            provenance_core::EntityKind::from("event"),
            EntitySchema {
                entity_type: EntityType::new("agent", "event"),
                fields,
                allow_unknown_fields: false,
            },
        )]),
        relations: BTreeMap::from([
            (
                RelationName::from("triggered-by"),
                cross_relation_schema(
                    ("agent", "triggered-by"),
                    ("agent", "event"),
                    ("beads", "issue"),
                    provenance_core::ExplanationRole::Causal,
                    provenance_core::ExplanationDirection::ToExplainedByFrom,
                ),
            ),
            (
                RelationName::from("observed-commit"),
                cross_relation_schema(
                    ("agent", "observed-commit"),
                    ("agent", "event"),
                    ("jj", "commit"),
                    provenance_core::ExplanationRole::Supporting,
                    provenance_core::ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("correlates-with"),
                cross_relation_schema(
                    ("agent", "correlates-with"),
                    ("agent", "event"),
                    ("jj", "operation"),
                    provenance_core::ExplanationRole::Contextual,
                    provenance_core::ExplanationDirection::Symmetric,
                ),
            ),
            (
                RelationName::from("implemented-by"),
                cross_relation_schema(
                    ("agent", "implemented-by"),
                    ("beads", "issue"),
                    ("jj", "commit"),
                    provenance_core::ExplanationRole::Primary,
                    provenance_core::ExplanationDirection::FromExplainedByTo,
                ),
            ),
        ]),
    }
}

fn jj_why_query() -> NamedQueryDefinition {
    NamedQueryDefinition {
        key: QueryKey::new("jj", "1", "why"),
        input: EntityTypePattern::External(EntityType::new("jj", "commit")),
        template: QueryTemplate::Explain {
            max_depth: None,
            roles: BTreeSet::from([
                provenance_core::ExplanationRole::Primary,
                provenance_core::ExplanationRole::Supporting,
            ]),
        },
    }
}

fn beads_issue_attributes() -> BTreeMap<FieldName, Value<FixtureModel>> {
    BTreeMap::from([
        (FieldName::from("issue_id"), text("bd-42")),
        (
            FieldName::from("title"),
            text("Connect agent turn to JJ implementation"),
        ),
        (FieldName::from("status"), text("in_progress")),
        (FieldName::from("priority"), Value::Integer(1)),
        (FieldName::from("issue_type"), text("feature")),
        (FieldName::from("created_at"), text("2026-08-30T10:00:00Z")),
        (FieldName::from("updated_at"), text("2026-08-31T12:00:01Z")),
        (
            FieldName::from("description"),
            text("Keep harness history linkable without copying the transcript"),
        ),
        (FieldName::from("assignee"), text("operator")),
        (FieldName::from("parent_id"), text("bd-17")),
        (
            FieldName::from("labels"),
            Value::List(vec![text("provenance"), text("agent")]),
        ),
    ])
}

fn beads_parent_issue_attributes() -> BTreeMap<FieldName, Value<FixtureModel>> {
    BTreeMap::from([
        (FieldName::from("issue_id"), text("bd-17")),
        (
            FieldName::from("title"),
            text("Agent provenance integration"),
        ),
        (FieldName::from("status"), text("open")),
        (FieldName::from("priority"), Value::Integer(1)),
        (FieldName::from("issue_type"), text("epic")),
        (FieldName::from("created_at"), text("2026-08-28T09:00:00Z")),
        (FieldName::from("updated_at"), text("2026-08-30T09:00:00Z")),
    ])
}

fn jj_commit_attributes(
    commit_id: &str,
    change_id: &str,
    description: &str,
) -> BTreeMap<FieldName, Value<FixtureModel>> {
    BTreeMap::from([
        (FieldName::from("commit_id"), text(commit_id)),
        (FieldName::from("change_id"), text(change_id)),
        (FieldName::from("description"), text(description)),
        (
            FieldName::from("author"),
            text("Marcin <marcin@example.com>"),
        ),
        (
            FieldName::from("timestamp"),
            text("2026-08-31T11:59:58Z@+00:00"),
        ),
    ])
}

fn agent_event_attributes() -> BTreeMap<FieldName, Value<FixtureModel>> {
    BTreeMap::from([
        (FieldName::from("actor-id"), text("actor-operator")),
        (FieldName::from("category"), text("llm")),
        (FieldName::from("data-present"), Value::Bool(true)),
        (FieldName::from("data-size"), Value::Integer(317)),
        (FieldName::from("event-id"), text("agent-event-0001")),
        (FieldName::from("event-kind"), text("scope")),
        (
            FieldName::from("evidence-digest"),
            text("sha256:evidence-0001"),
        ),
        (FieldName::from("evidence-length"), Value::Integer(1210)),
        (
            FieldName::from("evidence-ref"),
            text("atof/events.jsonl#offset=777"),
        ),
        (FieldName::from("harness-id"), text("codex")),
        (FieldName::from("installation-id"), text("workstation-a")),
        (FieldName::from("metadata-present"), Value::Bool(true)),
        (FieldName::from("metadata-size"), Value::Integer(566)),
        (FieldName::from("model-name"), text("gpt-5.6-luna")),
        (FieldName::from("name"), text("codex-turn")),
        (
            FieldName::from("conversation-ref"),
            text("~/.omp/agents/session-0001.jsonl"),
        ),
        (FieldName::from("outcome"), text("success")),
        (
            FieldName::from("scope-attributes"),
            Value::List(vec![text("llm"), text("turn")]),
        ),
        (FieldName::from("scope-category"), text("start")),
        (FieldName::from("session-id"), text("session-0001")),
        (FieldName::from("telemetry-provider"), text("phoenix")),
        (
            FieldName::from("telemetry-session-id"),
            text("phoenix-session-0001"),
        ),
        (
            FieldName::from("telemetry-session-instance-id"),
            text("phoenix-instance-0001"),
        ),
        (
            FieldName::from("telemetry-span-id"),
            text("span-000000000001"),
        ),
        (
            FieldName::from("telemetry-trace-id"),
            text("trace-000000000001"),
        ),
        (FieldName::from("timestamp"), text("2026-08-31T12:00:00Z")),
    ])
}

fn beads_relation(
    schema: SchemaKey,
    name: &str,
    from: EntityAddress<FixtureModel>,
    to: EntityAddress<FixtureModel>,
) -> Relation<FixtureModel> {
    Relation {
        schema,
        relation_type: RelationType::new("beads", name),
        from: external(from),
        to: external(to),
        attributes: BTreeMap::new(),
    }
}

fn jj_relation(
    schema: SchemaKey,
    name: &str,
    from: EntityAddress<FixtureModel>,
    to: EntityAddress<FixtureModel>,
) -> Relation<FixtureModel> {
    Relation {
        schema,
        relation_type: RelationType::new("jj", name),
        from: external(from),
        to: external(to),
        attributes: BTreeMap::new(),
    }
}

fn agent_relation(
    schema: SchemaKey,
    name: &str,
    from: EntityAddress<FixtureModel>,
    to: EntityAddress<FixtureModel>,
) -> Relation<FixtureModel> {
    Relation {
        schema,
        relation_type: RelationType::new("agent", name),
        from: external(from),
        to: external(to),
        attributes: BTreeMap::new(),
    }
}

fn cross_relation_schema(
    relation: (&str, &str),
    from: (&str, &str),
    to: (&str, &str),
    role: provenance_core::ExplanationRole,
    direction: provenance_core::ExplanationDirection,
) -> RelationSchema {
    RelationSchema::new(
        RelationType::new(relation.0, relation.1),
        EntityTypePattern::External(EntityType::new(from.0, from.1)),
        EntityTypePattern::External(EntityType::new(to.0, to.1)),
        Some(provenance_core::ExplanationSemantics::new(role, direction)),
    )
}

fn external(address: EntityAddress<FixtureModel>) -> EntityRef<FixtureModel> {
    EntityRef::External(address)
}

fn address(namespace: &str, kind: &str, id: &str) -> EntityAddress<FixtureModel> {
    EntityAddress::new(namespace, kind, id.to_owned())
}

fn text(value: &str) -> Value<FixtureModel> {
    Value::String(value.to_owned().into_boxed_str())
}
