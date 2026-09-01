//! Deterministic human-readable output for provenance records.
//!
//! The renderer is intentionally a presentation boundary. It does not know
//! about storage, terminals, telemetry clients, or provider URLs. The compact
//! mode is suitable for the default `show` command; verbose mode exposes the
//! operation's complete semantic envelope without using `Debug` on the
//! provenance model.

use provenance_core::{
    Actor, Attributes, Availability, EntityAddress, EntityObservation, EntityRef,
    EntityTypePattern, Event, Fact, FieldName, Integrity, InternalEntityKind, InternalEntityRef,
    Model, NamedQueryDefinition, Object, Operation, Relation, RelationSchema, RelationType,
    Replica, Resource, ResourceObservation, ResourceRequirement, RetentionClaim, RetentionOwner,
    RetentionStrength, SchemaDefinition, SchemaKey, Session, SourceAnchor, Value, ValueType,
};
use provenance_data_model::{Id, NodeRef, NodeType};
use std::fmt::{Display, Write as _};

const COMPACT_ID_LENGTH: usize = 12;
const COMPACT_TEXT_LENGTH: usize = 48;
const VALUE_TEXT_LENGTH: usize = 160;
const VALUE_ITEM_LIMIT: usize = 8;

/// Output detail level.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RenderMode {
    /// A short operation header followed by one-line event summaries.
    #[default]
    Compact,
    /// A deterministic tree containing operation, fact, relation, and value details.
    Verbose,
}

/// Stateless renderer for provenance operations.
///
/// `TtyRenderer::default()` uses [`RenderMode::Compact`]. Rendering returns a
/// `String` so callers can choose whether to print it, page it, or embed it in
/// another command's output.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TtyRenderer {
    mode: RenderMode,
}

impl TtyRenderer {
    /// Creates a renderer at the requested detail level.
    pub const fn new(mode: RenderMode) -> Self {
        Self { mode }
    }

    /// Creates the default narrow renderer.
    pub const fn compact() -> Self {
        Self::new(RenderMode::Compact)
    }

    /// Creates the detailed tree renderer.
    pub const fn verbose() -> Self {
        Self::new(RenderMode::Verbose)
    }

    /// Returns the configured detail level.
    pub const fn mode(self) -> RenderMode {
        self.mode
    }

    /// Renders one operation.
    ///
    /// IDs are formatted through `Display` because the data model deliberately
    /// leaves their primitive representation open. All current repository
    /// models use `String` IDs and satisfy this bound.
    pub fn render<M>(&self, operation: &Operation<M>) -> String
    where
        M: Model,
        M::Id: Display,
        M::ExternalId: Display,
    {
        match self.mode {
            RenderMode::Compact => render_compact(operation),
            RenderMode::Verbose => render_verbose(operation),
        }
    }
}

/// Renders one operation using the narrow default layout.
pub fn render_operation<M>(operation: &Operation<M>) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    TtyRenderer::compact().render(operation)
}

/// Renders one operation using the detailed tree layout.
pub fn render_operation_verbose<M>(operation: &Operation<M>) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    TtyRenderer::verbose().render(operation)
}

fn render_compact<M>(operation: &Operation<M>) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut output = String::new();
    write!(output, "Operation {}", format_id(&operation.id, true))
        .expect("writing to String cannot fail");

    if let Some(source) = &operation.source {
        write!(output, " · source {}", format_address(source, true))
            .expect("writing to String cannot fail");
    }
    if !operation.parents.is_empty() {
        write!(output, " · parents {}", operation.parents.len())
            .expect("writing to String cannot fail");
    }
    if !operation.facts.is_empty() {
        write!(output, " · facts {}", operation.facts.len())
            .expect("writing to String cannot fail");
    }

    for fact in &operation.facts {
        match fact {
            Fact::EventRecorded(event) => {
                output.push('\n');
                output.push_str(&compact_event(event));
            }
            other => {
                output.push('\n');
                write!(output, "  Fact {}", fact_label(other))
                    .expect("writing to String cannot fail");
            }
        }
    }

    output
}

fn compact_event<M>(event: &Event<M>) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let name = attribute_string(&event.attributes, "name")
        .or_else(|| attribute_string(&event.attributes, "event-kind"))
        .unwrap_or_else(|| format_id(&event.id, true));
    let mut output = format!("  Event {}", compact_text(&name, COMPACT_TEXT_LENGTH));

    if let Some(phase) = event_phase(&event.attributes) {
        write!(
            output,
            " · phase {}",
            compact_text(&phase, COMPACT_TEXT_LENGTH)
        )
        .expect("writing to String cannot fail");
    }
    if let Some(timestamp) = attribute_string(&event.attributes, "timestamp")
        .or_else(|| attribute_string(&event.attributes, "occurred-at"))
    {
        write!(
            output,
            " · {}",
            compact_text(&timestamp, COMPACT_TEXT_LENGTH)
        )
        .expect("writing to String cannot fail");
    }
    if let Some(session) = &event.session {
        write!(output, " · session {}", format_id(session, true))
            .expect("writing to String cannot fail");
    }
    if let Some(actor) = &event.actor {
        write!(output, " · actor {}", format_id(actor, true))
            .expect("writing to String cannot fail");
    }
    if let Some(telemetry) = compact_telemetry(&event.attributes) {
        write!(output, " · {}", telemetry).expect("writing to String cannot fail");
    }

    output
}

fn event_phase<M>(attributes: &Attributes<M>) -> Option<String>
where
    M: Model,
{
    attribute_string(attributes, "scope-category")
        .or_else(|| attribute_string(attributes, "category"))
        .or_else(|| {
            attribute_string(attributes, "event-kind")
                .filter(|kind| kind != "scope" && kind != "mark")
        })
}

fn compact_telemetry<M>(attributes: &Attributes<M>) -> Option<String>
where
    M: Model,
{
    let provider = attribute_string(attributes, "telemetry-provider")
        .or_else(|| attribute_string(attributes, "trace-provider"))
        .or_else(|| attribute_string(attributes, "provider"));
    let trace = attribute_string(attributes, "telemetry-trace-id")
        .or_else(|| attribute_string(attributes, "trace-id"));
    let span = attribute_string(attributes, "telemetry-span-id")
        .or_else(|| attribute_string(attributes, "span-id"));
    let session = attribute_string(attributes, "telemetry-session-id");
    let instance = attribute_string(attributes, "telemetry-session-instance-id");

    if provider.is_none()
        && trace.is_none()
        && span.is_none()
        && session.is_none()
        && instance.is_none()
    {
        return None;
    }

    let mut output = String::from("telemetry");
    if let Some(provider) = provider {
        write!(output, " {}", compact_text(&provider, COMPACT_TEXT_LENGTH))
            .expect("writing to String cannot fail");
    }
    if let Some(trace) = trace {
        write!(
            output,
            " trace {}",
            compact_text(&short_id(&trace), COMPACT_TEXT_LENGTH)
        )
        .expect("writing to String cannot fail");
    }
    if let Some(span) = span {
        write!(
            output,
            " span {}",
            compact_text(&short_id(&span), COMPACT_TEXT_LENGTH)
        )
        .expect("writing to String cannot fail");
    }
    if let Some(session) = session {
        write!(
            output,
            " session {}",
            compact_text(&short_id(&session), COMPACT_TEXT_LENGTH)
        )
        .expect("writing to String cannot fail");
    }
    if let Some(instance) = instance {
        write!(
            output,
            " instance {}",
            compact_text(&short_id(&instance), COMPACT_TEXT_LENGTH)
        )
        .expect("writing to String cannot fail");
    }
    Some(output)
}

fn render_verbose<M>(operation: &Operation<M>) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut root = TreeNode::new(format!("Operation {}", format_id(&operation.id, false)));

    if let Some(source) = &operation.source {
        root.push(TreeNode::new(format!(
            "source {}",
            format_address(source, false)
        )));
    }
    if !operation.parents.is_empty() {
        let mut parents = TreeNode::new(format!("parents ({})", operation.parents.len()));
        for parent in &operation.parents {
            parents.push(TreeNode::new(format_id(parent, false)));
        }
        root.push(parents);
    }
    if !operation.attributes.is_empty() {
        root.push(attributes_node(&operation.attributes));
    }

    let mut facts = TreeNode::new(format!("facts ({})", operation.facts.len()));
    for fact in &operation.facts {
        facts.push(fact_node(fact));
    }
    root.push(facts);

    root.render()
}

fn fact_node<M>(fact: &Fact<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    match fact {
        Fact::SchemaRegistered(schema) => schema_node(schema),
        Fact::NamedQueryRegistered(query) => named_query_node(query),
        Fact::SourceAnchored(anchor) => source_anchor_node(anchor),
        Fact::SessionOpened(session) => session_node(session),
        Fact::SessionEnded { session } => {
            let mut node = TreeNode::new("SessionEnded");
            push_field(&mut node, "session", format_id(session, false));
            node
        }
        Fact::ActorDeclared(actor) => actor_node(actor),
        Fact::ObjectDeclared(object) => object_node(object),
        Fact::EntityObserved(observation) => entity_observation_node(observation),
        Fact::EventRecorded(event) => event_node(event),
        Fact::ReplicaDeclared(replica) => replica_node(replica),
        Fact::RetentionClaimed(claim) => retention_claim_node(claim),
        Fact::RetentionReleased { claim } => {
            let mut node = TreeNode::new("RetentionReleased");
            push_field(&mut node, "claim", format_id(claim, false));
            node
        }
        Fact::ResourceObserved {
            resource,
            observation,
        } => resource_observed_node(resource, observation),
    }
}

fn schema_node(schema: &SchemaDefinition) -> TreeNode {
    let mut node = TreeNode::new("SchemaRegistered");
    push_field(&mut node, "key", format_schema_key(&schema.key));

    if !schema.requires.is_empty() {
        let mut requires = TreeNode::new(format!("requires ({})", schema.requires.len()));
        for required in &schema.requires {
            requires.push(TreeNode::new(format_schema_key(required)));
        }
        node.push(requires);
    }

    if !schema.entities.is_empty() {
        let mut entities = TreeNode::new(format!("entities ({})", schema.entities.len()));
        for (kind, entity_schema) in &schema.entities {
            let mut entity = TreeNode::new(format!("entity {}", kind.as_str()));
            if !entity_schema.fields.is_empty() {
                let mut fields = TreeNode::new(format!("fields ({})", entity_schema.fields.len()));
                for (name, field) in &entity_schema.fields {
                    let required = if field.required {
                        "required"
                    } else {
                        "optional"
                    };
                    fields.push(TreeNode::new(format!(
                        "{}: {} ({})",
                        name.as_str(),
                        format_value_type(&field.value_type),
                        required
                    )));
                }
                entity.push(fields);
            }
            push_field(
                &mut entity,
                "allow-unknown-fields",
                entity_schema.allow_unknown_fields,
            );
            entities.push(entity);
        }
        node.push(entities);
    }

    if !schema.relations.is_empty() {
        let mut relations = TreeNode::new(format!("relations ({})", schema.relations.len()));
        for relation in schema.relations.values() {
            relations.push(relation_schema_node(relation));
        }
        node.push(relations);
    }

    node
}

fn relation_schema_node(schema: &RelationSchema) -> TreeNode {
    let mut node = TreeNode::new(format!(
        "relation {}",
        format_relation_type(&schema.relation_type)
    ));
    push_field(&mut node, "from", format_entity_type_pattern(&schema.from));
    push_field(&mut node, "to", format_entity_type_pattern(&schema.to));
    if let Some(explanation) = schema.explanation {
        push_field(
            &mut node,
            "explanation",
            format!(
                "{} / {}",
                format_explanation_role(explanation.role),
                format_explanation_direction(explanation.direction)
            ),
        );
    }
    node
}

fn named_query_node(query: &NamedQueryDefinition) -> TreeNode {
    let mut node = TreeNode::new("NamedQueryRegistered");
    push_field(&mut node, "key", format_query_key(&query.key));
    push_field(&mut node, "input", format_entity_type_pattern(&query.input));
    push_field(
        &mut node,
        "template",
        format_query_template(&query.template),
    );
    node
}

fn source_anchor_node<M>(anchor: &SourceAnchor<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("SourceAnchored");
    push_field(&mut node, "source", format_address(&anchor.source, false));
    push_field(&mut node, "operation", format_id(&anchor.operation, false));
    node
}

fn session_node<M>(session: &Session<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("SessionOpened");
    push_field(&mut node, "id", format_id(&session.id, false));
    if let Some(parent) = &session.parent {
        push_field(&mut node, "parent", format_id(parent, false));
    }
    if !session.attributes.is_empty() {
        node.push(attributes_node(&session.attributes));
    }
    node
}

fn actor_node<M>(actor: &Actor<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("ActorDeclared");
    push_field(&mut node, "id", format_id(&actor.id, false));
    if let Some(session) = &actor.session {
        push_field(&mut node, "session", format_id(session, false));
    }
    if !actor.attributes.is_empty() {
        node.push(attributes_node(&actor.attributes));
    }
    node
}

fn object_node<M>(object: &Object<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("ObjectDeclared");
    push_field(&mut node, "id", format_id(&object.id, false));
    // Payloads are intentionally opaque here. In particular, this renderer
    // must never turn a potentially large evidence object into terminal text.
    push_field(&mut node, "payload", "opaque");
    if !object.attributes.is_empty() {
        node.push(attributes_node(&object.attributes));
    }
    node
}

fn entity_observation_node<M>(observation: &EntityObservation<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("EntityObserved");
    push_field(
        &mut node,
        "entity",
        format_address(&observation.entity, false),
    );
    push_field(&mut node, "schema", format_schema_key(&observation.schema));
    if !observation.attributes.is_empty() {
        node.push(attributes_node(&observation.attributes));
    }
    node
}

fn event_node<M>(event: &Event<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("EventRecorded");
    push_field(&mut node, "id", format_id(&event.id, false));
    if let Some(session) = &event.session {
        push_field(&mut node, "session", format_id(session, false));
    }
    if let Some(actor) = &event.actor {
        push_field(&mut node, "actor", format_id(actor, false));
    }
    add_id_group(&mut node, "parents", event.parents.iter(), false);
    add_entity_group(&mut node, "subjects", event.subjects.iter(), false);
    if !event.relations.is_empty() {
        let mut relations = TreeNode::new(format!("relations ({})", event.relations.len()));
        for relation in &event.relations {
            relations.push(relation_node(relation));
        }
        node.push(relations);
    }
    if !event.requires.is_empty() {
        let mut requires = TreeNode::new(format!("requires ({})", event.requires.len()));
        for requirement in &event.requires {
            requires.push(requirement_node(requirement));
        }
        node.push(requires);
    }
    if !event.attributes.is_empty() {
        node.push(attributes_node(&event.attributes));
    }
    node
}

fn relation_node<M>(relation: &Relation<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new(format_relation_type(&relation.relation_type));
    push_field(&mut node, "schema", format_schema_key(&relation.schema));
    push_field(&mut node, "from", format_entity_ref(&relation.from, false));
    push_field(&mut node, "to", format_entity_ref(&relation.to, false));
    if !relation.attributes.is_empty() {
        node.push(attributes_node(&relation.attributes));
    }
    node
}

fn requirement_node<M>(requirement: &ResourceRequirement<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new(format_entity_ref(&requirement.resource.entity, false));
    push_field(
        &mut node,
        "strength",
        format_retention_strength(requirement.strength),
    );
    node
}

fn replica_node<M>(replica: &Replica<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("ReplicaDeclared");
    push_field(&mut node, "id", format_id(&replica.id, false));
    if !replica.attributes.is_empty() {
        node.push(attributes_node(&replica.attributes));
    }
    node
}

fn retention_claim_node<M>(claim: &RetentionClaim<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("RetentionClaimed");
    push_field(&mut node, "id", format_id(&claim.id, false));
    push_field(
        &mut node,
        "owner",
        format_retention_owner(&claim.owner, false),
    );
    push_field(
        &mut node,
        "resource",
        format_entity_ref(&claim.resource.entity, false),
    );
    push_field(
        &mut node,
        "strength",
        format_retention_strength(claim.strength),
    );
    node
}

fn resource_observed_node<M>(
    resource: &Resource<M>,
    observation: &ResourceObservation<M>,
) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new("ResourceObserved");
    push_field(
        &mut node,
        "resource",
        format_entity_ref(&resource.entity, false),
    );
    push_field(
        &mut node,
        "availability",
        format_availability(&observation.availability, false),
    );
    push_field(
        &mut node,
        "integrity",
        format_integrity(observation.integrity),
    );
    if let Some(retention) = observation.observed_retention {
        push_field(
            &mut node,
            "observed-retention",
            format_retention_strength(retention),
        );
    }
    node
}

fn attributes_node<M>(attributes: &Attributes<M>) -> TreeNode
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut node = TreeNode::new(format!("attributes ({})", attributes.len()));
    for (name, value) in attributes {
        node.push(TreeNode::new(format!(
            "{} = {}",
            name.as_str(),
            format_value(value, false)
        )));
    }
    node
}

fn add_id_group<'a, R, T, I>(parent: &mut TreeNode, label: &str, ids: I, shorten: bool)
where
    R: Display + 'a,
    T: 'a,
    I: IntoIterator<Item = &'a Id<R, T>>,
{
    let ids = ids.into_iter().collect::<Vec<_>>();
    if ids.is_empty() {
        return;
    }
    let mut group = TreeNode::new(format!("{} ({})", label, ids.len()));
    for id in ids {
        group.push(TreeNode::new(format_id(id, shorten)));
    }
    parent.push(group);
}

fn add_entity_group<'a, M, I>(parent: &mut TreeNode, label: &str, entities: I, shorten: bool)
where
    M: Model + 'a,
    M::Id: Display,
    M::ExternalId: Display,
    I: IntoIterator<Item = &'a EntityRef<M>>,
{
    let entities = entities.into_iter().collect::<Vec<_>>();
    if entities.is_empty() {
        return;
    }
    let mut group = TreeNode::new(format!("{} ({})", label, entities.len()));
    for entity in entities {
        group.push(TreeNode::new(format_entity_ref(entity, shorten)));
    }
    parent.push(group);
}

fn push_field(parent: &mut TreeNode, name: &str, value: impl Display) {
    parent.push(TreeNode::new(format!("{} {}", name, value)));
}

fn format_id<R, T>(id: &Id<R, T>, shorten: bool) -> String
where
    R: Display,
{
    let value = clean_text(&id.raw().to_string());
    if shorten { short_id(&value) } else { value }
}

fn format_address<M>(address: &EntityAddress<M>, shorten: bool) -> String
where
    M: Model,
    M::ExternalId: Display,
{
    format!(
        "{}/{}/{}",
        address.namespace.as_str(),
        address.kind.as_str(),
        if shorten {
            short_id(&address.id.to_string())
        } else {
            clean_text(&address.id.to_string())
        }
    )
}

fn format_entity_ref<M>(entity: &EntityRef<M>, shorten: bool) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    match entity {
        EntityRef::Internal(internal) => format_internal_ref(internal, shorten),
        EntityRef::External(address) => format_address(address, shorten),
    }
}

fn format_internal_ref<M>(internal: &InternalEntityRef<M>, shorten: bool) -> String
where
    M: Model,
    M::Id: Display,
{
    match internal {
        InternalEntityRef::Operation(id) => format!("operation/{}", format_id(id, shorten)),
        InternalEntityRef::Event(id) => format!("event/{}", format_id(id, shorten)),
        InternalEntityRef::Session(id) => format!("session/{}", format_id(id, shorten)),
        InternalEntityRef::Actor(id) => format!("actor/{}", format_id(id, shorten)),
        InternalEntityRef::Object(id) => format!("object/{}", format_id(id, shorten)),
        InternalEntityRef::Replica(id) => format!("replica/{}", format_id(id, shorten)),
    }
}

fn format_node_ref<M>(node: &NodeRef<M>, shorten: bool) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    match node {
        NodeRef::Entity(id) => format!("entity/{}", format_id(id, shorten)),
        NodeRef::Activity(id) => format!("activity/{}", format_id(id, shorten)),
        NodeRef::Agent(id) => format!("agent/{}", format_id(id, shorten)),
        NodeRef::External(address) => format_address(address, shorten),
        NodeRef::Assertion(id) => format!("assertion/{}", format_id(id, shorten)),
    }
}

fn format_value<M>(value: &Value<M>, shorten_ids: bool) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let rendered = match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Decimal(value) => compact_text(value, VALUE_TEXT_LENGTH),
        Value::String(value) => format!("{:?}", compact_text(value, VALUE_TEXT_LENGTH)),
        Value::Bytes(bytes) => format!("bytes[{}]", bytes.len()),
        Value::Entity(entity) => format_entity_ref(entity, shorten_ids),
        Value::Ref(node) => format_node_ref(node, shorten_ids),
        Value::List(values) => {
            let mut output = String::from("[");
            for (index, value) in values.iter().take(VALUE_ITEM_LIMIT).enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format_value(value, shorten_ids));
            }
            if values.len() > VALUE_ITEM_LIMIT {
                write!(output, ", … +{}", values.len() - VALUE_ITEM_LIMIT)
                    .expect("writing to String cannot fail");
            }
            output.push(']');
            output
        }
        Value::Map(values) => {
            let mut output = String::from("{");
            for (index, (name, value)) in values.iter().take(VALUE_ITEM_LIMIT).enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                write!(
                    output,
                    "{}: {}",
                    name.as_str(),
                    format_value(value, shorten_ids)
                )
                .expect("writing to String cannot fail");
            }
            if values.len() > VALUE_ITEM_LIMIT {
                write!(output, ", … +{}", values.len() - VALUE_ITEM_LIMIT)
                    .expect("writing to String cannot fail");
            }
            output.push('}');
            output
        }
    };
    compact_text(&rendered, VALUE_TEXT_LENGTH)
}

fn format_schema_key(key: &SchemaKey) -> String {
    format!("{}@{}", key.namespace.as_str(), key.version.as_str())
}

fn format_relation_type(relation: &RelationType) -> String {
    format!("{}/{}", relation.namespace.as_str(), relation.name.as_str())
}

fn format_query_key(key: &provenance_core::QueryKey) -> String {
    format!(
        "{}@{}/{}",
        key.namespace.as_str(),
        key.version.as_str(),
        key.name.as_str()
    )
}

fn format_query_template(template: &provenance_core::QueryTemplate) -> String {
    match template {
        provenance_core::QueryTemplate::History => "history".to_owned(),
        provenance_core::QueryTemplate::Explain { max_depth, roles } => format!(
            "explain(depth={}, roles={})",
            format_optional_usize(*max_depth),
            format_roles(roles)
        ),
        provenance_core::QueryTemplate::Traverse {
            direction,
            relations,
            max_depth,
        } => format!(
            "traverse(direction={}, relations={}, depth={})",
            format_direction(*direction),
            format_relation_selector(relations),
            format_optional_usize(*max_depth)
        ),
    }
}

fn format_relation_selector(selector: &provenance_core::RelationSelector) -> String {
    match selector {
        provenance_core::RelationSelector::Any => "any".to_owned(),
        provenance_core::RelationSelector::Explanatory => "explanatory".to_owned(),
        provenance_core::RelationSelector::Types(types) => {
            let mut output = String::from("types[");
            for (index, relation) in types.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format_relation_type(relation));
            }
            output.push(']');
            output
        }
    }
}

fn format_optional_usize(value: Option<usize>) -> String {
    value.map_or_else(|| "unbounded".to_owned(), |value| value.to_string())
}

fn format_roles(roles: &std::collections::BTreeSet<provenance_core::ExplanationRole>) -> String {
    let mut output = String::from("[");
    for (index, role) in roles.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(format_explanation_role(*role));
    }
    output.push(']');
    output
}

fn format_direction(direction: provenance_core::Direction) -> &'static str {
    match direction {
        provenance_core::Direction::Incoming => "incoming",
        provenance_core::Direction::Outgoing => "outgoing",
        provenance_core::Direction::Both => "both",
    }
}

fn format_value_type(value_type: &ValueType) -> String {
    match value_type {
        ValueType::Null => "null".to_owned(),
        ValueType::Bool => "bool".to_owned(),
        ValueType::Integer => "integer".to_owned(),
        ValueType::Decimal => "decimal".to_owned(),
        ValueType::String => "string".to_owned(),
        ValueType::Bytes => "bytes".to_owned(),
        ValueType::Entity(pattern) => format!("entity<{}>", format_entity_type_pattern(pattern)),
        ValueType::Ref(node_type) => format!("ref<{}>", format_node_type(node_type)),
        ValueType::List(value_type) => format!("list<{}>", format_value_type(value_type)),
        ValueType::Map {
            fields,
            allow_unknown,
        } => format!(
            "map(fields={}, allow-unknown={})",
            fields.len(),
            allow_unknown
        ),
        ValueType::Any => "any".to_owned(),
    }
}

fn format_node_type(node_type: &NodeType) -> String {
    match node_type {
        NodeType::Entity { namespace, kind } => format_scoped_kind(namespace.as_ref(), kind),
        NodeType::Activity { namespace, kind } => {
            format!("activity:{}", format_scoped_kind(namespace.as_ref(), kind))
        }
        NodeType::Agent { namespace, kind } => {
            format!("agent:{}", format_scoped_kind(namespace.as_ref(), kind))
        }
        NodeType::Assertion => "assertion".to_owned(),
        NodeType::External(entity_type) => format!(
            "external:{}/{}",
            entity_type.namespace.as_str(),
            entity_type.kind.as_str()
        ),
        NodeType::Any => "any".to_owned(),
    }
}

fn format_scoped_kind(
    namespace: Option<&provenance_data_model::Namespace>,
    kind: &provenance_data_model::Kind,
) -> String {
    namespace.map_or_else(
        || kind.as_str().to_owned(),
        |namespace| format!("{}/{}", namespace.as_str(), kind.as_str()),
    )
}

fn format_entity_type_pattern(pattern: &EntityTypePattern) -> String {
    match pattern {
        EntityTypePattern::Internal(kind) => {
            format!("internal:{}", format_internal_entity_kind(*kind))
        }
        EntityTypePattern::External(entity_type) => format!(
            "{}/{}",
            entity_type.namespace.as_str(),
            entity_type.kind.as_str()
        ),
        EntityTypePattern::Any => "any".to_owned(),
    }
}

fn format_internal_entity_kind(kind: InternalEntityKind) -> &'static str {
    match kind {
        InternalEntityKind::Operation => "operation",
        InternalEntityKind::Event => "event",
        InternalEntityKind::Session => "session",
        InternalEntityKind::Actor => "actor",
        InternalEntityKind::Object => "object",
        InternalEntityKind::Replica => "replica",
    }
}

fn format_explanation_role(role: provenance_core::ExplanationRole) -> &'static str {
    match role {
        provenance_core::ExplanationRole::Primary => "primary",
        provenance_core::ExplanationRole::Supporting => "supporting",
        provenance_core::ExplanationRole::Contextual => "contextual",
        provenance_core::ExplanationRole::Causal => "causal",
        provenance_core::ExplanationRole::Contradicting => "contradicting",
        provenance_core::ExplanationRole::Invalidating => "invalidating",
        provenance_core::ExplanationRole::Attribution => "attribution",
        provenance_core::ExplanationRole::Influence => "influence",
    }
}

fn format_explanation_direction(direction: provenance_core::ExplanationDirection) -> &'static str {
    match direction {
        provenance_core::ExplanationDirection::FromExplainedByTo => "from-explained-by-to",
        provenance_core::ExplanationDirection::ToExplainedByFrom => "to-explained-by-from",
        provenance_core::ExplanationDirection::Symmetric => "symmetric",
        provenance_core::ExplanationDirection::SubjectExplainedByObject => {
            "subject-explained-by-object"
        }
        provenance_core::ExplanationDirection::ObjectExplainedBySubject => {
            "object-explained-by-subject"
        }
    }
}

fn format_retention_strength(strength: RetentionStrength) -> &'static str {
    match strength {
        RetentionStrength::Referenced => "referenced",
        RetentionStrength::Pinned => "pinned",
        RetentionStrength::Escrowed => "escrowed",
    }
}

fn format_retention_owner<M>(owner: &RetentionOwner<M>, shorten: bool) -> String
where
    M: Model,
    M::Id: Display,
{
    match owner {
        RetentionOwner::Event(id) => format!("event/{}", format_id(id, shorten)),
        RetentionOwner::Session(id) => format!("session/{}", format_id(id, shorten)),
        RetentionOwner::Actor(id) => format!("actor/{}", format_id(id, shorten)),
    }
}

fn format_availability<M>(availability: &Availability<M>, shorten: bool) -> String
where
    M: Model,
    M::Id: Display,
{
    match availability {
        Availability::Unknown => "unknown".to_owned(),
        Availability::Local => "local".to_owned(),
        Availability::Missing => "missing".to_owned(),
        Availability::Remote(replicas) => {
            let mut output = String::from("remote[");
            for (index, replica) in replicas.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format_id(replica, shorten));
            }
            output.push(']');
            output
        }
    }
}

fn format_integrity(integrity: Integrity) -> &'static str {
    match integrity {
        Integrity::Unknown => "unknown",
        Integrity::Unverified => "unverified",
        Integrity::Verified => "verified",
        Integrity::Invalid => "invalid",
    }
}

fn fact_label<M: Model>(fact: &Fact<M>) -> &'static str {
    match fact {
        Fact::SchemaRegistered(_) => "SchemaRegistered",
        Fact::NamedQueryRegistered(_) => "NamedQueryRegistered",
        Fact::SourceAnchored(_) => "SourceAnchored",
        Fact::SessionOpened(_) => "SessionOpened",
        Fact::SessionEnded { .. } => "SessionEnded",
        Fact::ActorDeclared(_) => "ActorDeclared",
        Fact::ObjectDeclared(_) => "ObjectDeclared",
        Fact::EntityObserved(_) => "EntityObserved",
        Fact::EventRecorded(_) => "EventRecorded",
        Fact::ReplicaDeclared(_) => "ReplicaDeclared",
        Fact::RetentionClaimed(_) => "RetentionClaimed",
        Fact::RetentionReleased { .. } => "RetentionReleased",
        Fact::ResourceObserved { .. } => "ResourceObserved",
    }
}

fn attribute_string<M: Model>(attributes: &Attributes<M>, name: &str) -> Option<String> {
    match attributes.get(&FieldName::from(name)) {
        Some(Value::String(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn short_id(value: &str) -> String {
    let value = clean_text(value);
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(COMPACT_ID_LENGTH).collect::<String>();
    if chars.next().is_some() {
        format!("{}…", prefix)
    } else {
        prefix
    }
}

fn compact_text(value: &str, limit: usize) -> String {
    let value = clean_text(value);
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        format!("{}…", prefix)
    } else {
        prefix
    }
}

fn clean_text(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\n' => '↵',
            '\r' => '␍',
            '\t' => '⇥',
            character if character.is_control() => '�',
            character => character,
        })
        .collect()
}

#[derive(Debug)]
struct TreeNode {
    label: String,
    children: Vec<TreeNode>,
}

impl TreeNode {
    fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            children: Vec::new(),
        }
    }

    fn push(&mut self, child: TreeNode) {
        self.children.push(child);
    }

    fn render(&self) -> String {
        let mut output = String::new();
        self.render_into(&mut output, "", None);
        output.pop();
        output
    }

    fn render_into(&self, output: &mut String, prefix: &str, branch: Option<bool>) {
        if let Some(is_last) = branch {
            output.push_str(prefix);
            output.push_str(if is_last { "╰─ " } else { "├─ " });
        }
        output.push_str(&self.label);
        output.push('\n');

        for (index, child) in self.children.iter().enumerate() {
            let is_last = index + 1 == self.children.len();
            let child_prefix = match branch {
                None => String::new(),
                Some(true) => format!("{}   ", prefix),
                Some(false) => format!("{}│  ", prefix),
            };
            child.render_into(output, &child_prefix, Some(is_last));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use provenance_core::{
        EntityAddress, EntityRef, EventId, OperationId, Relation, SchemaKey, Value,
    };
    use std::collections::{BTreeMap, BTreeSet};

    fn text(value: &str) -> Value<provenance_core::PluginModel> {
        Value::String(value.to_owned().into_boxed_str())
    }

    fn fixture_operation() -> Operation<provenance_core::PluginModel> {
        let mut attributes = BTreeMap::new();
        attributes.insert(FieldName::from("name"), text("codex-turn"));
        attributes.insert(FieldName::from("scope-category"), text("start"));
        attributes.insert(
            FieldName::from("timestamp"),
            text("2026-08-30T21:53:06.852175+00:00"),
        );
        attributes.insert(FieldName::from("prompt"), text("private prompt text"));
        attributes.insert(FieldName::from("telemetry-provider"), text("phoenix"));
        attributes.insert(
            FieldName::from("telemetry-trace-id"),
            text("01a054a975247b408ec3120ed3303b72"),
        );
        attributes.insert(
            FieldName::from("telemetry-span-id"),
            text("8ec3120ed3303b72"),
        );
        attributes.insert(
            FieldName::from("telemetry-session-id"),
            text("01a054a9-716a-7da3-8469-acdbc59948f5"),
        );

        let event: Event<provenance_core::PluginModel> = Event {
            id: EventId::<provenance_core::PluginModel>::new("event-0123456789abcdef".to_owned()),
            session: Some(
                provenance_core::SessionId::<provenance_core::PluginModel>::new(
                    "session-0123456789abcdef".to_owned(),
                ),
            ),
            actor: Some(
                provenance_core::ActorId::<provenance_core::PluginModel>::new(
                    "actor-0123456789abcdef".to_owned(),
                ),
            ),
            parents: BTreeSet::new(),
            subjects: BTreeSet::from([EntityRef::External(EntityAddress::new(
                "telemetry",
                "trace",
                "phoenix:01a054a975247b408ec3120ed3303b72".to_owned(),
            ))]),
            relations: BTreeSet::new(),
            requires: BTreeSet::new(),
            attributes,
        };

        Operation {
            id: OperationId::<provenance_core::PluginModel>::new(
                "abcdef1234567890abcdef".to_owned(),
            ),
            parents: BTreeSet::new(),
            source: Some(EntityAddress::new(
                "agent",
                "event",
                "01a054a975247b408ec3120ed3303b72/start".to_owned(),
            )),
            facts: vec![Fact::EventRecorded(event)],
            attributes: BTreeMap::new(),
        }
    }

    #[test]
    fn default_output_is_narrow_and_omits_arbitrary_attributes() {
        let output = render_operation(&fixture_operation());

        assert!(output.contains("Operation abcdef123456…"));
        assert!(output.contains("source agent/event/01a054a97524…"));
        assert!(output.contains("Event codex-turn · phase start"));
        assert!(output.contains("telemetry phoenix trace 01a054a97524…"));
        assert!(!output.contains("private prompt text"));
        assert!(!output.contains("telemetry-provider"));
        assert!(!output.contains("_tag"));
        assert_eq!(output.lines().count(), 2);
    }

    #[test]
    fn verbose_output_contains_tree_metadata_and_typed_attributes() {
        let mut operation = fixture_operation();
        let event = match operation.facts.first_mut().expect("event fact") {
            Fact::EventRecorded(event) => event,
            _ => unreachable!("fixture fact is an event"),
        };
        event
            .attributes
            .insert(FieldName::from("attempt"), Value::Integer(3));
        event
            .attributes
            .insert(FieldName::from("payload"), Value::Bytes(vec![1, 2, 3]));
        event.relations.insert(Relation {
            schema: SchemaKey::new("agent", "1"),
            relation_type: RelationType::new("agent", "caused-by"),
            from: EntityRef::External(EntityAddress::new(
                "agent",
                "event",
                "event-0123456789abcdef".to_owned(),
            )),
            to: EntityRef::External(EntityAddress::new(
                "telemetry",
                "trace",
                "phoenix:01a054a975247b408ec3120ed3303b72".to_owned(),
            )),
            attributes: BTreeMap::new(),
        });

        let output = render_operation_verbose(&operation);

        assert!(output.contains("Operation abcdef1234567890abcdef"));
        assert!(output.contains("├─ source agent/event/01a054a975247b408ec3120ed3303b72/start"));
        assert!(output.contains("EventRecorded"));
        assert!(output.contains("relations (1)"));
        assert!(output.contains("agent/caused-by"));
        assert!(output.contains("telemetry-provider = \"phoenix\""));
        assert!(output.contains("attempt = 3"));
        assert!(output.contains("payload = bytes[3]"));
        assert!(output.contains("prompt = \"private prompt text\""));
        assert!(!output.contains("_tag"));
        assert!(!output.contains("raw:"));
    }
}
