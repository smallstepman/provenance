#![no_std]
#![allow(clippy::too_many_arguments)]

//! The example host contract is intentionally narrow: `bd` runs the
//! `.beads/hooks/on_create`, `.beads/hooks/on_update`, and
//! `.beads/hooks/on_close` scripts, writes the issue JSON snapshot to the
//! script's standard input, and the host supplies that snapshot as an
//! observation request. The request source is `beads:hook:<event>/<issue-id>/<updated-at>`;
//! the cursor is the hook event (`create`, `update`, or `close`), and scalar or
//! list issue fields become request attributes. The plugin emits the canonical
//! `beads:issue` observation plus dependency, parent, and relation edges.

extern crate alloc;

use alloc::{
    collections::BTreeMap,
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

const NAMESPACE: &str = "beads";
const SCHEMA_VERSION: &str = "1";
const ISSUE_KIND: &str = "issue";

struct BeadsPlugin;

impl Guest for BeadsPlugin {
    fn manifest() -> guest::PluginManifest {
        guest::PluginManifest {
            id: "beads".into(),
            abi_version: "0.2.0".into(),
            namespace: NAMESPACE.into(),
            schemas: vec![guest::SchemaKey {
                namespace: NAMESPACE.into(),
                version: SCHEMA_VERSION.into(),
            }],
            required_schemas: Vec::new(),
            capabilities: vec!["emit-observations".into()],
        }
    }

    fn observe(
        request: guest::ObservationRequest,
    ) -> Result<guest::Transaction, guest::PluginError> {
        let cursor = request.cursor;
        let mut attributes = BTreeMap::new();
        for attribute in request.attributes {
            if attributes
                .insert(attribute.name.clone(), attribute.value)
                .is_some()
            {
                return Err(invalid_request("duplicate observation attribute"));
            }
        }

        let issue_id = required_string(&mut attributes, "issue-id")?;
        let title = required_string(&mut attributes, "title")?;
        let status = required_string(&mut attributes, "status")?;
        let priority = required_integer(&mut attributes, "priority")?;
        let issue_type = required_string(&mut attributes, "issue-type")?;
        let created_at = required_string(&mut attributes, "created-at")?;
        let updated_at = required_string(&mut attributes, "updated-at")?;
        let description = optional_string(&mut attributes, "description")?;
        let created_by = optional_string(&mut attributes, "created-by")?;
        let assignee = optional_string(&mut attributes, "assignee")?;
        let started_at = optional_string(&mut attributes, "started-at")?;
        let closed_at = optional_string(&mut attributes, "closed-at")?;
        let close_reason = optional_string(&mut attributes, "close-reason")?;
        let external_ref = optional_string(&mut attributes, "external-ref")?;
        let parent_id = optional_string(&mut attributes, "parent-id")?;
        let labels = optional_string_list(&mut attributes, "labels")?;
        let depends_on = optional_string_list(&mut attributes, "depends-on")?;
        let relates_to = optional_string_list(&mut attributes, "relates-to")?;
        if !attributes.is_empty() {
            return Err(invalid_request("unknown observation attribute"));
        }

        let schema_key = guest::SchemaKey {
            namespace: NAMESPACE.into(),
            version: SCHEMA_VERSION.into(),
        };
        let issue_type_ref = guest::EntityType {
            namespace: NAMESPACE.into(),
            kind: ISSUE_KIND.into(),
        };
        let schema = schema_definition(schema_key.clone(), issue_type_ref.clone());
        let issue = guest::EntityAddress {
            namespace: NAMESPACE.into(),
            kind: ISSUE_KIND.into(),
            id: issue_id,
        };
        let issue_ref = guest::EntityRef::External(issue.clone());

        let mut observed_attributes = vec![
            text_attribute("issue_id", issue.id.clone()),
            text_attribute("title", title),
            text_attribute("status", status),
            integer_attribute("priority", priority),
            text_attribute("issue_type", issue_type),
            text_attribute("created_at", created_at),
            text_attribute("updated_at", updated_at),
        ];
        push_optional(&mut observed_attributes, "description", description);
        push_optional(&mut observed_attributes, "created_by", created_by);
        push_optional(&mut observed_attributes, "assignee", assignee);
        push_optional(&mut observed_attributes, "started_at", started_at);
        push_optional(&mut observed_attributes, "closed_at", closed_at);
        push_optional(&mut observed_attributes, "close_reason", close_reason);
        push_optional(&mut observed_attributes, "external_ref", external_ref);
        push_optional(&mut observed_attributes, "parent_id", parent_id.clone());
        if !labels.is_empty() {
            observed_attributes.push(guest::Attribute {
                name: "labels".into(),
                value: guest::Value::ListValue(
                    labels
                        .iter()
                        .cloned()
                        .map(guest::ScalarValue::StringValue)
                        .collect(),
                ),
            });
        }

        let mut subjects = vec![issue_ref.clone()];
        let mut relations = Vec::new();
        for dependency_id in depends_on {
            if dependency_id == issue.id {
                return Err(invalid_request("issue cannot depend on itself"));
            }
            let dependency = issue_address(dependency_id);
            subjects.push(guest::EntityRef::External(dependency.clone()));
            relations.push(relation(
                schema_key.clone(),
                "depends-on",
                issue_ref.clone(),
                guest::EntityRef::External(dependency),
            ));
        }
        if let Some(parent_id) = parent_id {
            if parent_id == issue.id {
                return Err(invalid_request("issue cannot belong to itself"));
            }
            let parent = issue_address(parent_id);
            subjects.push(guest::EntityRef::External(parent.clone()));
            relations.push(relation(
                schema_key.clone(),
                "belongs-to",
                issue_ref.clone(),
                guest::EntityRef::External(parent),
            ));
        }
        for related_id in relates_to {
            if related_id == issue.id {
                return Err(invalid_request("issue cannot relate to itself"));
            }
            let related = issue_address(related_id);
            subjects.push(guest::EntityRef::External(related.clone()));
            relations.push(relation(
                schema_key.clone(),
                "relates-to",
                issue_ref.clone(),
                guest::EntityRef::External(related),
            ));
        }

        let mut event_attributes = Vec::new();
        if let Some(event) = cursor.as_deref() {
            event_attributes.push(text_attribute("event", event.to_string()));
        }
        let seed = observation_seed(&request.source, cursor.as_deref());
        Ok(guest::Transaction {
            seed,
            source: Some(guest::SourceOperation {
                id: request.source,
                parents: Vec::new(),
            }),
            intents: vec![
                guest::Intent::RegisterSchema(schema),
                guest::Intent::ObserveEntity(guest::EntityObservation {
                    entity: issue,
                    schema: schema_key,
                    attributes: observed_attributes,
                }),
                guest::Intent::RecordEvent(guest::EventIntent {
                    session: None,
                    actor: None,
                    additional_parents: Vec::new(),
                    subjects,
                    relations,
                    requires: Vec::new(),
                    attributes: event_attributes,
                }),
            ],
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

fn schema_definition(
    schema_key: guest::SchemaKey,
    issue_type: guest::EntityType,
) -> guest::SchemaDefinition {
    guest::SchemaDefinition {
        key: schema_key,
        requires: Vec::new(),
        entities: vec![guest::EntitySchema {
            kind: ISSUE_KIND.into(),
            fields: vec![
                field("issue_id", guest::ValueType::StringValue, true),
                field("title", guest::ValueType::StringValue, true),
                field("description", guest::ValueType::StringValue, false),
                field("status", guest::ValueType::StringValue, true),
                field("priority", guest::ValueType::Integer, true),
                field("issue_type", guest::ValueType::StringValue, true),
                field("assignee", guest::ValueType::StringValue, false),
                field("created_at", guest::ValueType::StringValue, true),
                field("created_by", guest::ValueType::StringValue, false),
                field("updated_at", guest::ValueType::StringValue, true),
                field("started_at", guest::ValueType::StringValue, false),
                field("closed_at", guest::ValueType::StringValue, false),
                field("close_reason", guest::ValueType::StringValue, false),
                field("external_ref", guest::ValueType::StringValue, false),
                field("parent_id", guest::ValueType::StringValue, false),
                field(
                    "labels",
                    guest::ValueType::ListValue(guest::ScalarValueType::StringValue),
                    false,
                ),
            ],
            allow_unknown_fields: false,
        }],
        relations: vec![
            relation_schema(
                "depends-on",
                issue_type.clone(),
                issue_type.clone(),
                guest::ExplanationRole::Supporting,
                guest::ExplanationDirection::SourceExplainsTarget,
            ),
            relation_schema(
                "belongs-to",
                issue_type.clone(),
                issue_type.clone(),
                guest::ExplanationRole::Supporting,
                guest::ExplanationDirection::SourceExplainsTarget,
            ),
            relation_schema(
                "relates-to",
                issue_type,
                guest::EntityType {
                    namespace: NAMESPACE.into(),
                    kind: ISSUE_KIND.into(),
                },
                guest::ExplanationRole::Contextual,
                guest::ExplanationDirection::Symmetric,
            ),
        ],
    }
}

fn relation_schema(
    name: &str,
    source: guest::EntityType,
    target: guest::EntityType,
    role: guest::ExplanationRole,
    direction: guest::ExplanationDirection,
) -> guest::RelationSchema {
    guest::RelationSchema {
        relation_type: guest::RelationType {
            namespace: NAMESPACE.into(),
            name: name.into(),
        },
        source: guest::EntityTypePattern::External(source),
        target: guest::EntityTypePattern::External(target),
        explanation: Some(guest::ExplanationSemantics { role, direction }),
    }
}

fn relation(
    schema: guest::SchemaKey,
    name: &str,
    source: guest::EntityRef,
    target: guest::EntityRef,
) -> guest::Relation {
    guest::Relation {
        schema,
        relation_type: guest::RelationType {
            namespace: NAMESPACE.into(),
            name: name.into(),
        },
        source,
        target,
        attributes: Vec::new(),
    }
}

fn issue_address(id: String) -> guest::EntityAddress {
    guest::EntityAddress {
        namespace: NAMESPACE.into(),
        kind: ISSUE_KIND.into(),
        id,
    }
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

fn push_optional(attributes: &mut Vec<guest::Attribute>, name: &str, value: Option<String>) {
    if let Some(value) = value {
        attributes.push(text_attribute(name, value));
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

fn required_integer(
    attributes: &mut BTreeMap<String, guest::Value>,
    name: &str,
) -> Result<String, guest::PluginError> {
    match attributes.remove(name) {
        Some(guest::Value::IntegerText(value)) => Ok(value),
        Some(guest::Value::StringValue(value)) => Ok(value),
        Some(_) => Err(invalid_request("integer attribute must be integer text")),
        None => Err(invalid_request("required attribute is missing")),
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
    let mut seed = String::from("beads-observation-v1\0");
    seed.push_str(&source.namespace);
    seed.push('\0');
    seed.push_str(&source.kind);
    seed.push('\0');
    seed.push_str(&source.id);
    seed.push('\0');
    if let Some(cursor) = cursor {
        seed.push_str(cursor);
    }
    seed
}

fn invalid_request(message: &str) -> guest::PluginError {
    guest::PluginError {
        kind: guest::PluginErrorKind::InvalidRequest,
        message: message.to_string(),
    }
}

export!(BeadsPlugin);
