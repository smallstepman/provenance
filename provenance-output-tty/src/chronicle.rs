//! Chronicle-style causal output over the authoritative provenance state.
//!
//! A chronicle is deliberately different from operation rendering: it starts
//! with a `why` query, follows schema-declared explanation edges, folds the
//! selected graph into bounded counts, and then prints a chronological set of
//! records. Payloads and arbitrary attributes never become terminal output.

use provenance_core::{
    Attributes, EntityAddress, EntityRef, Event, ExplanationRole, Fact, FieldName, GraphEdge,
    Model, Operation, Query, State, Value,
};
use std::collections::BTreeSet;
use std::fmt::{Display, Write as _};

use super::{
    attribute_string, clean_text, format_availability, format_entity_ref, format_id,
    format_integrity, format_relation_type, format_retention_strength, format_schema_key,
    format_value,
};

const CHRONICLE_ATTRIBUTE_PRIORITY: &[&str] = &[
    "summary",
    "title",
    "status",
    "description",
    "finding",
    "stats",
    "evidence",
    "result",
    "metrics",
    "baseline",
    "constraints",
    "children",
    "in",
    "out",
    "decision",
    "change",
    "observations",
    "delta",
    "attribution",
    "caveat",
    "work",
    "sessions",
    "subagents",
    "edits",
    "files",
    "jj-range",
    "detail",
    "path",
    "config",
    "schema",
    "parent-id",
    "parent_id",
    "issue-id",
    "issue_id",
    "issue-type",
    "issue_type",
    "priority",
    "assignee",
    "labels",
    "created-at",
    "created_at",
    "updated-at",
    "updated_at",
    "commit-id",
    "commit_id",
    "change-id",
    "change_id",
    "current-commit",
    "current_commit",
    "author",
    "operation-id",
    "operation_id",
    "workspace-id",
    "workspace_id",
    "event-id",
    "event_id",
    "name",
    "event-kind",
    "timestamp",
    "occurred-at",
    "outcome",
    "harness",
    "harness-id",
    "installation-id",
    "conversation-ref",
    "evidence-ref",
    "evidence-digest",
    "telemetry-provider",
    "trace-provider",
    "provider",
    "telemetry-trace-id",
    "trace-id",
    "telemetry-span-id",
    "span-id",
    "telemetry-session-id",
    "telemetry-session-instance-id",
    "session-id",
    "actor-id",
    "scope-category",
    "category",
    "scope-attributes",
    "data-present",
    "data-size",
    "metadata-present",
    "metadata-size",
    "hook",
    "operation-kind",
    "projection",
    "source-system",
    "reason",
    "role",
    "phase",
];

/// A bounded `why` query rendered in chronicle form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChronicleQuery<M: Model> {
    /// Entity at the center of the explanation.
    pub target: EntityRef<M>,
    /// Maximum number of explanation edges followed from the target.
    pub max_depth: usize,
}

impl<M: Model> ChronicleQuery<M> {
    /// Creates the standard causal explanation query.
    pub fn why(target: EntityRef<M>, max_depth: usize) -> Self {
        Self { target, max_depth }
    }
}

/// Renders a bounded causal explanation as a chronicle.
///
/// The query is executed against [`State::query`], so the output follows only
/// relation schemas that declare explanation semantics. The returned text is
/// deterministic for a given state and query. Conversation payloads remain
/// outside the output; references to retained evidence, session logs, and
/// telemetry are preserved when they are present in typed attributes.
pub fn render_chronicle<M>(
    state: &State<M>,
    query: &ChronicleQuery<M>,
) -> provenance_core::Result<String>
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let result = state.query(&Query::Explain {
        root: query.target.clone(),
        max_depth: Some(query.max_depth),
        roles: BTreeSet::new(),
    })?;
    let operation_ids = collect_operation_ids(state, &query.target, &result.operations);
    let records = collect_records(state, &operation_ids);
    let selected_entities = result.entities.len();
    let selected_events = result.events.len();
    let selected_edges = result.edges.len();

    let mut output = String::new();
    writeln!(output, "@chronicle 1").expect("writing to String cannot fail");
    writeln!(
        output,
        "query: why {}",
        format_chronicle_entity(&query.target, false)
    )
    .expect("writing to String cannot fail");
    write!(
        output,
        "target: {}",
        format_chronicle_entity(&query.target, false)
    )
    .expect("writing to String cannot fail");
    if let Some(title) = target_title(state, &query.target) {
        write!(output, " {:?}", clean_text(&title)).expect("writing to String cannot fail");
    }
    output.push('\n');
    writeln!(output, "timebase: UTC").expect("writing to String cannot fail");
    writeln!(output, "scope: causal depth={}", query.max_depth)
        .expect("writing to String cannot fail");
    writeln!(
        output,
        "summary: {}",
        chronicle_summary(
            state,
            &query.target,
            selected_entities,
            selected_events,
            selected_edges,
            records.len(),
        )
    )
    .expect("writing to String cannot fail");
    writeln!(
        output,
        "path: {}",
        causal_path(state, &query.target, &result.edges, query.max_depth)
    )
    .expect("writing to String cannot fail");
    writeln!(
        output,
        "folded: {}",
        folded_summary(state, &operation_ids, &result)
    )
    .expect("writing to String cannot fail");
    writeln!(output, "detail: {}", detail_uri(&query.target))
        .expect("writing to String cannot fail");

    render_records(
        &mut output,
        &query.target,
        &records,
        &result.entities,
        &result.edges,
    );
    render_footer(
        &mut output,
        state,
        &query.target,
        records.len(),
        &operation_ids,
        &result.entities,
        result.edges.len(),
    );
    Ok(output)
}

fn collect_operation_ids<M: Model>(
    state: &State<M>,
    target: &EntityRef<M>,
    initial: &BTreeSet<provenance_core::OperationId<M>>,
) -> BTreeSet<provenance_core::OperationId<M>> {
    let mut ids = initial.clone();
    if let EntityRef::External(address) = target
        && let Some(history) = state.projection.entity_history.get(address)
    {
        ids.extend(history.iter().cloned());
    }

    let mut pending = ids.iter().cloned().collect::<Vec<_>>();
    while let Some(id) = pending.pop() {
        let Some(operation) = state.operation(&id) else {
            continue;
        };
        for parent in &operation.parents {
            if ids.insert(parent.clone()) {
                pending.push(parent.clone());
            }
        }
    }
    ids
}

struct ChronicleRecord<'a, M: Model> {
    operation: &'a Operation<M>,
    event: Option<&'a Event<M>>,
    timestamp: Option<String>,
    sort_key: String,
}

fn collect_records<'a, M: Model>(
    state: &'a State<M>,
    operation_ids: &BTreeSet<provenance_core::OperationId<M>>,
) -> Vec<ChronicleRecord<'a, M>>
where
    M::Id: Display,
{
    let mut records = Vec::new();
    for operation_id in operation_ids {
        let Some(operation) = state.operation(operation_id) else {
            continue;
        };
        let events = operation
            .facts
            .iter()
            .filter_map(|fact| match fact {
                Fact::EventRecorded(event) => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();
        if events.is_empty()
            && !operation
                .facts
                .iter()
                .any(|fact| matches!(fact, Fact::EntityObserved(_)))
        {
            continue;
        }

        if events.is_empty() {
            let timestamp = operation_timestamp(operation, None);
            records.push(ChronicleRecord {
                operation,
                event: None,
                sort_key: record_sort_key(timestamp.as_deref(), operation),
                timestamp,
            });
        } else {
            for event in events {
                let timestamp = operation_timestamp(operation, Some(event));
                records.push(ChronicleRecord {
                    operation,
                    event: Some(event),
                    sort_key: record_sort_key(timestamp.as_deref(), operation),
                    timestamp,
                });
            }
        }
    }
    records.sort_by(|left, right| {
        left.sort_key.cmp(&right.sort_key).then_with(|| {
            format_id(&left.operation.id, false).cmp(&format_id(&right.operation.id, false))
        })
    });
    records
}

fn record_sort_key<M: Model>(timestamp: Option<&str>, operation: &Operation<M>) -> String
where
    M::Id: Display,
{
    let timestamp = timestamp
        .map(timestamp_parts)
        .map(|(date, time)| format!("{}T{}", date, time))
        .unwrap_or_else(|| "~undated".to_owned());
    format!("{}:{}", timestamp, format_id(&operation.id, false))
}

fn operation_timestamp<M: Model>(
    operation: &Operation<M>,
    event: Option<&Event<M>>,
) -> Option<String> {
    event
        .and_then(|event| timestamp_attribute(&event.attributes))
        .or_else(|| {
            operation.facts.iter().find_map(|fact| match fact {
                Fact::EntityObserved(observation) => timestamp_attribute(&observation.attributes),
                Fact::SessionOpened(session) => timestamp_attribute(&session.attributes),
                Fact::ActorDeclared(actor) => timestamp_attribute(&actor.attributes),
                Fact::ObjectDeclared(object) => timestamp_attribute(&object.attributes),
                Fact::ReplicaDeclared(replica) => timestamp_attribute(&replica.attributes),
                _ => None,
            })
        })
        .or_else(|| timestamp_attribute(&operation.attributes))
}

fn timestamp_attribute<M: Model>(attributes: &Attributes<M>) -> Option<String> {
    for name in [
        "timestamp",
        "occurred-at",
        "occurred_at",
        "updated-at",
        "updated_at",
        "created-at",
        "created_at",
    ] {
        if let Some(value) = attribute_string(attributes, name) {
            return Some(value);
        }
    }
    None
}

fn timestamp_parts(timestamp: &str) -> (String, String) {
    let timestamp = timestamp.split('@').next().unwrap_or(timestamp);
    let Some((date, rest)) = timestamp.split_once('T') else {
        return ("undated".to_owned(), "--:--:--".to_owned());
    };
    let time = rest.split_once('Z').map_or(rest, |(time, _)| time);
    let time = time.split_once('+').map_or(time, |(time, _)| time);
    let time = time.chars().take(8).collect::<String>();
    if time.len() == 8 {
        (date.to_owned(), time)
    } else {
        (date.to_owned(), "--:--:--".to_owned())
    }
}

fn target_title<M: Model>(state: &State<M>, target: &EntityRef<M>) -> Option<String> {
    let EntityRef::External(address) = target else {
        return None;
    };
    state
        .projection
        .entity(address)
        .and_then(|observation| primary_text(&observation.attributes))
}

fn primary_text<M: Model>(attributes: &Attributes<M>) -> Option<String> {
    ["title", "name", "description", "event-kind"]
        .iter()
        .find_map(|name| attribute_string(attributes, name))
}

fn chronicle_summary<M: Model>(
    state: &State<M>,
    target: &EntityRef<M>,
    entities: usize,
    events: usize,
    edges: usize,
    records: usize,
) -> String {
    let mut summary = String::new();
    if let Some(status) = target_status(state, target) {
        write!(summary, "status={:?}; ", clean_text(&status))
            .expect("writing to String cannot fail");
    }
    if let Some(title) = target_title(state, target) {
        write!(summary, "title={:?}; ", clean_text(&title)).expect("writing to String cannot fail");
    }
    write!(
        summary,
        "causal scope contains {} entities, {} events, and {} explanatory edges across {} records",
        entities, events, edges, records
    )
    .expect("writing to String cannot fail");
    summary
}

fn target_status<M: Model>(state: &State<M>, target: &EntityRef<M>) -> Option<String> {
    let EntityRef::External(address) = target else {
        return None;
    };
    state
        .projection
        .entity(address)
        .and_then(|observation| attribute_string(&observation.attributes, "status"))
}

fn causal_path<M>(
    state: &State<M>,
    target: &EntityRef<M>,
    edges: &BTreeSet<GraphEdge<M>>,
    max_depth: usize,
) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut path = format_chronicle_entity(target, false);
    let mut current = target.clone();
    let mut visited = BTreeSet::from([target.clone()]);

    for _ in 0..max_depth {
        let mut candidates = edges
            .iter()
            .filter_map(|edge| {
                let next = if edge.relation.from == current {
                    Some((&edge.relation.to, true))
                } else if edge.relation.to == current {
                    Some((&edge.relation.from, false))
                } else {
                    None
                }?;
                if visited.contains(next.0) {
                    return None;
                }
                Some((edge, next.0.clone(), next.1))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            explanation_priority(state, left.0)
                .cmp(&explanation_priority(state, right.0))
                .then_with(|| {
                    format_relation_type(&left.0.relation.relation_type)
                        .cmp(&format_relation_type(&right.0.relation.relation_type))
                })
                .then_with(|| {
                    format_chronicle_entity(&left.1, false)
                        .cmp(&format_chronicle_entity(&right.1, false))
                })
        });
        let Some((edge, next, forward)) = candidates.into_iter().next() else {
            break;
        };
        if forward {
            write!(
                path,
                " -[{}]-> {}",
                format_relation_type(&edge.relation.relation_type),
                format_chronicle_entity(&next, false)
            )
            .expect("writing to String cannot fail");
        } else {
            write!(
                path,
                " <-[{}]- {}",
                format_relation_type(&edge.relation.relation_type),
                format_chronicle_entity(&next, false)
            )
            .expect("writing to String cannot fail");
        }
        visited.insert(next.clone());
        current = next;
    }
    path
}

fn explanation_priority<M: Model>(state: &State<M>, edge: &GraphEdge<M>) -> u8 {
    state
        .projection
        .schema(&edge.relation.schema)
        .and_then(|schema| schema.relations.get(&edge.relation.relation_type.name))
        .and_then(|schema| schema.explanation)
        .map_or(5, |explanation| match explanation.role {
            ExplanationRole::Causal => 0,
            ExplanationRole::Primary => 1,
            ExplanationRole::Supporting => 2,
            ExplanationRole::Contextual => 3,
            ExplanationRole::Attribution => 4,
            ExplanationRole::Influence => 4,
            ExplanationRole::Contradicting => 4,
            ExplanationRole::Invalidating => 4,
        })
}

fn folded_summary<M: Model>(
    state: &State<M>,
    operation_ids: &BTreeSet<provenance_core::OperationId<M>>,
    result: &provenance_core::QueryResult<M>,
) -> String
where
    M::Id: Display,
{
    let mut sessions = BTreeSet::new();
    let mut actors = BTreeSet::new();
    let mut edits = 0u64;
    let mut files = 0u64;
    let mut explicit_sessions = 0u64;
    let mut explicit_subagents = 0u64;
    let mut has_edits = false;
    let mut has_files = false;
    let mut has_explicit_sessions = false;
    let mut has_explicit_subagents = false;

    for operation_id in operation_ids {
        let Some(operation) = state.operation(operation_id) else {
            continue;
        };
        for fact in &operation.facts {
            match fact {
                Fact::SessionOpened(session) => {
                    sessions.insert(format_id(&session.id, false));
                    collect_counter(&session.attributes, "edits", &mut edits, &mut has_edits);
                    collect_counter(&session.attributes, "files", &mut files, &mut has_files);
                }
                Fact::ActorDeclared(actor) => {
                    actors.insert(format_id(&actor.id, false));
                }
                Fact::EventRecorded(event) => {
                    if let Some(session) = &event.session {
                        sessions.insert(format_id(session, false));
                    }
                    if let Some(actor) = &event.actor {
                        actors.insert(format_id(actor, false));
                    }
                    collect_attribute_id(&event.attributes, "session-id", &mut sessions);
                    collect_attribute_id(&event.attributes, "actor-id", &mut actors);
                    collect_counter_from_aliases(
                        &event.attributes,
                        &["edits", "edit-count", "edit_count"],
                        &mut edits,
                        &mut has_edits,
                    );
                    collect_counter_from_aliases(
                        &event.attributes,
                        &["files", "file-count", "file_count"],
                        &mut files,
                        &mut has_files,
                    );
                }
                _ => {}
            }
        }
        collect_counter_from_aliases(
            &operation.attributes,
            &["edits", "edit-count", "edit_count"],
            &mut edits,
            &mut has_edits,
        );
        collect_counter_from_aliases(
            &operation.attributes,
            &["files", "file-count", "file_count"],
            &mut files,
            &mut has_files,
        );
        collect_counter_from_aliases(
            &operation.attributes,
            &["sessions", "session-count", "session_count"],
            &mut explicit_sessions,
            &mut has_explicit_sessions,
        );
        collect_counter_from_aliases(
            &operation.attributes,
            &["subagents", "subagent-count", "subagent_count"],
            &mut explicit_subagents,
            &mut has_explicit_subagents,
        );
    }

    let operation_count = meaningful_operation_count(state, operation_ids);
    let session_count = if has_explicit_sessions {
        explicit_sessions
    } else {
        sessions.len() as u64
    };
    let subagent_count = if has_explicit_subagents {
        explicit_subagents
    } else {
        actors.len() as u64
    };
    let mut output = format!(
        "sessions={} subagents={} operations={} entities={} events={} edges={}",
        session_count,
        subagent_count,
        operation_count,
        result.entities.len(),
        result.events.len(),
        result.edges.len()
    );
    if has_edits {
        write!(output, " edits={edits}").expect("writing to String cannot fail");
    }
    if has_files {
        write!(output, " files={files}").expect("writing to String cannot fail");
    }
    output
}

fn collect_attribute_id<M: Model>(
    attributes: &Attributes<M>,
    name: &str,
    output: &mut BTreeSet<String>,
) {
    if let Some(value) = attribute_string(attributes, name) {
        output.insert(value);
    }
}

fn collect_counter<M: Model>(
    attributes: &Attributes<M>,
    name: &str,
    output: &mut u64,
    present: &mut bool,
) {
    if let Some(Value::Integer(value)) = attributes.get(&FieldName::from(name))
        && let Ok(value) = u64::try_from(*value)
    {
        *output = output.saturating_add(value);
        *present = true;
    }
}

fn collect_counter_from_aliases<M: Model>(
    attributes: &Attributes<M>,
    aliases: &[&str],
    output: &mut u64,
    present: &mut bool,
) {
    for alias in aliases {
        if attributes.contains_key(&FieldName::from(*alias)) {
            collect_counter(attributes, alias, output, present);
            break;
        }
    }
}

fn render_records<M>(
    output: &mut String,
    target: &EntityRef<M>,
    records: &[ChronicleRecord<'_, M>],
    selected_entities: &BTreeSet<EntityRef<M>>,
    selected_edges: &BTreeSet<GraphEdge<M>>,
) where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut current_date = None;
    for record in records {
        let (date, time) = record
            .timestamp
            .as_deref()
            .map(timestamp_parts)
            .unwrap_or_else(|| ("undated".to_owned(), "--:--:--".to_owned()));
        if current_date.as_deref() != Some(date.as_str()) {
            output.push('\n');
            output.push_str(&date);
            current_date = Some(date);
        }
        let label = record_label(record.operation, record.event);
        let address = record_address(record.operation, target);
        write!(output, "\n{} {} {}", time, label, address).expect("writing to String cannot fail");
        if let Some(title) = record_title(record.operation, record.event, target) {
            write!(output, " {:?}", clean_text(&title)).expect("writing to String cannot fail");
        }
        write!(
            output,
            "\n  operation: prov://operation/{}",
            format_id(&record.operation.id, false)
        )
        .expect("writing to String cannot fail");
        if !record.operation.parents.is_empty() {
            let parents = record
                .operation
                .parents
                .iter()
                .map(|parent| format!("prov://operation/{}", format_id(parent, false)))
                .collect::<Vec<_>>();
            write!(output, "\n  parents: [{}]", parents.join(", "))
                .expect("writing to String cannot fail");
        }
        if let Some(source) = &record.operation.source {
            write!(
                output,
                "\n  source: {}",
                format_chronicle_address(source, false)
            )
            .expect("writing to String cannot fail");
        }

        append_record_entities(output, record.operation, selected_entities);
        append_record_attributes(output, record.operation, record.event, target);
        if let Some(event) = record.event {
            append_event_details(output, event, selected_edges);
        }
        append_record_resources(output, record.operation);
    }
}

fn render_footer<M>(
    output: &mut String,
    state: &State<M>,
    target: &EntityRef<M>,
    records: usize,
    operation_ids: &BTreeSet<provenance_core::OperationId<M>>,
    selected_entities: &BTreeSet<EntityRef<M>>,
    edges: usize,
) where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    output.push_str("\n@end");
    write!(
        output,
        "\nresult: {} records={} operations={} entities={} edges={}",
        format_chronicle_entity(target, false),
        records,
        meaningful_operation_count(state, operation_ids),
        selected_entities.len(),
        edges
    )
    .expect("writing to String cannot fail");

    let open = open_entities(state, selected_entities);
    if !open.is_empty() {
        write!(output, "\nopen: {}", open.join(", ")).expect("writing to String cannot fail");
    }
}

fn meaningful_operation_count<M: Model>(
    state: &State<M>,
    operation_ids: &BTreeSet<provenance_core::OperationId<M>>,
) -> usize {
    operation_ids
        .iter()
        .filter(|operation_id| {
            state.operation(operation_id).is_some_and(|operation| {
                operation
                    .facts
                    .iter()
                    .any(|fact| matches!(fact, Fact::EventRecorded(_) | Fact::EntityObserved(_)))
            })
        })
        .count()
}

fn open_entities<M>(state: &State<M>, selected_entities: &BTreeSet<EntityRef<M>>) -> Vec<String>
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut open = selected_entities
        .iter()
        .filter_map(|entity| {
            let EntityRef::External(address) = entity else {
                return None;
            };
            let observation = state.projection.entity(address)?;
            let status = attribute_string(&observation.attributes, "status")?;
            if is_terminal_status(&status) {
                return None;
            }
            Some(format!(
                "{} status={:?}",
                format_chronicle_entity(entity, false),
                clean_text(&status)
            ))
        })
        .collect::<Vec<_>>();
    open.sort();
    open
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().as_str(),
        "closed"
            | "done"
            | "completed"
            | "success"
            | "deployed"
            | "resolved"
            | "cancelled"
            | "canceled"
    )
}

fn record_label<M: Model>(operation: &Operation<M>, event: Option<&Event<M>>) -> &'static str {
    if let Some(source) = &operation.source {
        return match source.namespace.as_str() {
            "beads" if source.kind.as_str() == "hook" => "FEATURE",
            "beads" => "BEADS",
            "jj" if source.kind.as_str() == "operation" => "OPERATION",
            "jj" => "JJ",
            "agent" => "AGENT",
            _ => "RECORD",
        };
    }
    if let Some(EntityRef::External(address)) = event.and_then(|event| event.subjects.iter().next())
    {
        return match address.namespace.as_str() {
            "beads" => "FEATURE",
            "jj" => "JJ",
            "agent" => "AGENT",
            _ => "RECORD",
        };
    }
    "RECORD"
}

fn record_address<M>(operation: &Operation<M>, target: &EntityRef<M>) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    if let EntityRef::External(target) = target
        && operation.facts.iter().any(|fact| {
            matches!(fact, Fact::EntityObserved(observation) if &observation.entity == target)
        })
    {
        return format_chronicle_address(target, false);
    }
    operation.source.as_ref().map_or_else(
        || format!("prov://operation/{}", format_id(&operation.id, false)),
        |source| format_chronicle_address(source, false),
    )
}

fn record_title<M: Model>(
    operation: &Operation<M>,
    event: Option<&Event<M>>,
    target: &EntityRef<M>,
) -> Option<String> {
    if let EntityRef::External(target) = target
        && let Some(title) = operation.facts.iter().find_map(|fact| match fact {
            Fact::EntityObserved(observation) if &observation.entity == target => {
                primary_text(&observation.attributes)
            }
            _ => None,
        })
    {
        return Some(title);
    }
    if let Some(source) = &operation.source
        && let Some(title) = operation.facts.iter().find_map(|fact| match fact {
            Fact::EntityObserved(observation) if &observation.entity == source => {
                primary_text(&observation.attributes)
            }
            _ => None,
        })
    {
        return Some(title);
    }
    event.and_then(|event| primary_text(&event.attributes))
}

fn append_record_entities<M>(
    output: &mut String,
    operation: &Operation<M>,
    selected_entities: &BTreeSet<EntityRef<M>>,
) where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    let mut entities = Vec::new();
    for fact in &operation.facts {
        let Fact::EntityObserved(observation) = fact else {
            continue;
        };
        let entity = EntityRef::External(observation.entity.clone());
        if selected_entities.contains(&entity) {
            entities.push(format_chronicle_entity(&entity, false));
        }
    }
    entities.sort();
    entities.dedup();
    if !entities.is_empty() {
        write!(output, "\n  entities: [{}]", entities.join(", "))
            .expect("writing to String cannot fail");
    }
}

fn append_record_attributes<M>(
    output: &mut String,
    operation: &Operation<M>,
    event: Option<&Event<M>>,
    target: &EntityRef<M>,
) where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    if let EntityRef::External(target) = target
        && let Some(observation) = operation.facts.iter().find_map(|fact| match fact {
            Fact::EntityObserved(observation) if &observation.entity == target => Some(observation),
            _ => None,
        })
    {
        append_chronicle_attributes(output, "attributes", &observation.attributes, &["title"]);
    } else if event.is_none()
        && let Some(source) = &operation.source
        && let Some(observation) = operation.facts.iter().find_map(|fact| match fact {
            Fact::EntityObserved(observation) if &observation.entity == source => Some(observation),
            _ => None,
        })
    {
        append_chronicle_attributes(output, "attributes", &observation.attributes, &[]);
    }

    if let Some(event) = event {
        append_chronicle_attributes(
            output,
            "event-attributes",
            &event.attributes,
            &[
                "name",
                "event-kind",
                "timestamp",
                "occurred-at",
                "session-id",
                "actor-id",
            ],
        );
        if let Some(session) = &event.session {
            write!(output, "\n  session: {}", format_id(session, false))
                .expect("writing to String cannot fail");
        }
        if let Some(actor) = &event.actor {
            write!(output, "\n  actor: {}", format_id(actor, false))
                .expect("writing to String cannot fail");
        }
        if let Some(telemetry) = super::compact_telemetry(&event.attributes) {
            write!(output, "\n  {}", telemetry).expect("writing to String cannot fail");
        }
    }

    if !operation.attributes.is_empty() {
        append_chronicle_attributes(output, "operation-attributes", &operation.attributes, &[]);
    }
}

fn append_chronicle_attributes<M>(
    output: &mut String,
    label: &str,
    attributes: &Attributes<M>,
    excluded: &[&str],
) where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    if attributes.is_empty() {
        return;
    }
    let considered = attributes
        .iter()
        .filter(|(name, _)| !excluded.contains(&name.as_str()))
        .count();
    let mut selected = 0usize;
    for candidate in CHRONICLE_ATTRIBUTE_PRIORITY.iter().copied() {
        if selected >= super::COMPACT_ATTRIBUTE_LIMIT || excluded.contains(&candidate) {
            continue;
        }
        let Some((name, value)) = attributes
            .iter()
            .find(|(name, _)| name.as_str() == candidate)
        else {
            continue;
        };
        write!(
            output,
            "\n  {}: {}",
            name.as_str(),
            format_value(value, false)
        )
        .expect("writing to String cannot fail");
        selected += 1;
    }
    let omitted = considered.saturating_sub(selected);
    if omitted > 0 {
        write!(output, "\n  {}: +{} attrs omitted", label, omitted)
            .expect("writing to String cannot fail");
    }
}

fn append_event_details<M>(
    output: &mut String,
    event: &Event<M>,
    selected_edges: &BTreeSet<GraphEdge<M>>,
) where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    write!(output, "\n  event: {}", format_id(&event.id, false))
        .expect("writing to String cannot fail");
    let subjects = event
        .subjects
        .iter()
        .map(|subject| format_chronicle_entity(subject, false))
        .collect::<Vec<_>>();
    if !subjects.is_empty() {
        write!(output, "\n  subjects: [{}]", subjects.join(", "))
            .expect("writing to String cannot fail");
    }

    let mut relations = event
        .relations
        .iter()
        .filter(|relation| {
            selected_edges
                .iter()
                .any(|edge| edge.event == event.id && edge.relation == **relation)
        })
        .collect::<Vec<_>>();
    relations.sort_by_key(|relation| {
        (
            format_relation_type(&relation.relation_type),
            format_chronicle_entity(&relation.from, false),
            format_chronicle_entity(&relation.to, false),
        )
    });
    for relation in relations {
        write!(
            output,
            "\n  relation: {} -[{} {}]-> {}",
            format_chronicle_entity(&relation.from, false),
            format_relation_type(&relation.relation_type),
            format_schema_key(&relation.schema),
            format_chronicle_entity(&relation.to, false)
        )
        .expect("writing to String cannot fail");
    }
}

fn append_record_resources<M>(output: &mut String, operation: &Operation<M>)
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    for fact in &operation.facts {
        match fact {
            Fact::RetentionClaimed(claim) => {
                write!(
                    output,
                    "\n  retention: {} owner={} strength={}",
                    format_chronicle_entity(&claim.resource.entity, false),
                    super::format_retention_owner(&claim.owner, false),
                    format_retention_strength(claim.strength)
                )
                .expect("writing to String cannot fail");
            }
            Fact::ResourceObserved {
                resource,
                observation,
            } => {
                write!(
                    output,
                    "\n  resource: {} availability={} integrity={}",
                    format_chronicle_entity(&resource.entity, false),
                    format_availability(&observation.availability, false),
                    format_integrity(observation.integrity)
                )
                .expect("writing to String cannot fail");
                if let Some(retention) = observation.observed_retention {
                    write!(
                        output,
                        " retention={}",
                        format_retention_strength(retention)
                    )
                    .expect("writing to String cannot fail");
                }
            }
            _ => {}
        }
    }
}

fn format_chronicle_entity<M>(entity: &EntityRef<M>, shorten: bool) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    match entity {
        EntityRef::Internal(_internal) => format!("prov://{}", format_entity_ref(entity, shorten)),
        EntityRef::External(address) => format_chronicle_address(address, shorten),
    }
}

fn format_chronicle_address<M>(address: &EntityAddress<M>, shorten: bool) -> String
where
    M: Model,
    M::ExternalId: Display,
{
    let id = if shorten {
        super::short_id(&clean_text(&address.id.to_string()))
    } else {
        clean_text(&address.id.to_string())
    };
    match address.namespace.as_str() {
        "beads" if address.kind.as_str() == "issue" => format!("bd://{id}"),
        "jj" => format!("jj://{id}"),
        "agent" => format!("agent://{}/{}", address.kind.as_str(), id),
        "telemetry" => format!("telemetry://{}/{}", address.kind.as_str(), id),
        namespace => format!("{}://{}/{}", namespace, address.kind.as_str(), id),
    }
}

fn detail_uri<M>(target: &EntityRef<M>) -> String
where
    M: Model,
    M::Id: Display,
    M::ExternalId: Display,
{
    match target {
        EntityRef::External(address) => {
            let id = clean_text(&address.id.to_string());
            match address.namespace.as_str() {
                "beads" if address.kind.as_str() == "issue" => format!("prov://bd/{id}"),
                "jj" => format!("prov://jj/{id}"),
                "agent" => format!("prov://agent/{}/{}", address.kind.as_str(), id),
                namespace => format!("prov://{}/{}/{}", namespace, address.kind.as_str(), id),
            }
        }
        EntityRef::Internal(_internal) => format!("prov://{}", format_entity_ref(target, false)),
    }
}

#[cfg(test)]
mod tests {
    use super::timestamp_parts;

    #[test]
    fn timestamp_parts_strip_offsets_and_fractional_seconds() {
        assert_eq!(
            timestamp_parts("2026-08-31T11:59:58.123Z@+00:00"),
            ("2026-08-31".to_owned(), "11:59:58".to_owned())
        );
    }
}
