#![no_std]
#![allow(clippy::too_many_arguments)]

//! Provenance adapter for DecisionGraph (`dg`) document hooks.
//!
//! The `dg` CLI invokes `.dg/hooks/on_create`, `.dg/hooks/on_update`, and
//! `.dg/hooks/on_delete` with `<document-id> <event>` arguments and a JSON
//! document notification on standard input. A host maps that notification to
//! an [`ObservationRequest`]: the source is `dg:hook:<event>/<id>/<revision>`,
//! the cursor is the hook event, and the document fields below are typed
//! attributes. The revision component must be stable for one payload and
//! distinct for successive payloads of the same document.
//!
//! The host may provide `document-id`, `document-type`, `path`, `title`, `body`,
//! `status`, `author`, `date`, `tags`, `frontmatter`, `frontmatter-json`,
//! `sections-json`, `payload-json`, and `revision`. DG relation fields use
//! `supersedes`, `enables`, `triggers`, `depends-on`, `implements`,
//! `conflicts-with`, and `related`; update hooks may additionally provide
//! `before-json`, `after-json`, and `diff-json`.
//!
//! The plugin emits one `dg:document` observation and one event per hook. The
//! observation carries the current document snapshot (the pre-delete snapshot
//! for delete hooks), while update-only `before-json`, `after-json`, and
//! `diff-json` attributes remain attached to the event for auditability.

extern crate alloc;

use alloc::{
    collections::{BTreeMap, BTreeSet},
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

const NAMESPACE: &str = "dg";
const HOOK_KIND: &str = "hook";
const SCHEMA_VERSION: &str = "1";
const DOCUMENT_KIND: &str = "document";

struct DgPlugin;

impl Guest for DgPlugin {
    fn manifest() -> guest::PluginManifest {
        guest::PluginManifest {
            id: "dg".into(),
            abi_version: "0.2.0".into(),
            namespace: NAMESPACE.into(),
            schemas: vec![guest::SchemaKey {
                namespace: NAMESPACE.into(),
                version: SCHEMA_VERSION.into(),
            }],
            required_schemas: Vec::new(),
            capabilities: vec![
                "emit-observations".into(),
                "emit-document-events".into(),
                "install-hooks".into(),
            ],
        }
    }

    fn observe(
        request: guest::ObservationRequest,
    ) -> Result<guest::Transaction, guest::PluginError> {
        let source = request.source;
        if source.namespace != NAMESPACE {
            return Err(invalid_request(
                "observation source must use the dg namespace",
            ));
        }
        if source.kind != HOOK_KIND {
            return Err(invalid_request("observation source kind must be hook"));
        }
        if source.id.is_empty() {
            return Err(invalid_request("observation source id must not be empty"));
        }

        let event = match request.cursor.as_deref() {
            Some("create") => "create",
            Some("update") => "update",
            Some("delete") => "delete",
            Some(_) => return Err(invalid_request("cursor must be create, update, or delete")),
            None => return Err(invalid_request("hook event cursor is required")),
        };

        let mut attributes = BTreeMap::new();
        for attribute in request.attributes {
            if attributes
                .insert(attribute.name.clone(), attribute.value)
                .is_some()
            {
                return Err(invalid_request("duplicate observation attribute"));
            }
        }

        let document_id = required_string(&mut attributes, "document-id")?;
        if document_id.is_empty() {
            return Err(invalid_request("document id must not be empty"));
        }
        let document_type = optional_string(&mut attributes, "document-type")?
            .unwrap_or_else(|| infer_document_type(&document_id));
        if document_type.is_empty() {
            return Err(invalid_request("document type must not be empty"));
        }
        let path = optional_string(&mut attributes, "path")?;
        let title = optional_string(&mut attributes, "title")?;
        let body = optional_string(&mut attributes, "body")?;
        let status = optional_string(&mut attributes, "status")?;
        let author = optional_string(&mut attributes, "author")?;
        let date = optional_string(&mut attributes, "date")?;
        let tags = optional_string_list(&mut attributes, "tags")?;
        let frontmatter = optional_scalar_map(&mut attributes, "frontmatter")?;
        let frontmatter_json = optional_string(&mut attributes, "frontmatter-json")?;
        let sections_json = optional_string(&mut attributes, "sections-json")?;
        let payload_json = optional_string(&mut attributes, "payload-json")?;
        let revision = optional_string(&mut attributes, "revision")?;
        let before_json = optional_string(&mut attributes, "before-json")?;
        let after_json = optional_string(&mut attributes, "after-json")?;
        let diff_json = optional_string(&mut attributes, "diff-json")?;

        let supersedes = optional_string_list(&mut attributes, "supersedes")?;
        let enables = optional_string_list(&mut attributes, "enables")?;
        let triggers = optional_string_list(&mut attributes, "triggers")?;
        let depends_on = optional_string_list(&mut attributes, "depends-on")?;
        let implements = optional_string_list(&mut attributes, "implements")?;
        let conflicts_with = optional_string_list(&mut attributes, "conflicts-with")?;
        let related = optional_string_list(&mut attributes, "related")?;
        if !attributes.is_empty() {
            return Err(invalid_request("unknown observation attribute"));
        }

        let schema_key = guest::SchemaKey {
            namespace: NAMESPACE.into(),
            version: SCHEMA_VERSION.into(),
        };
        let document = document_address(document_id.clone());
        let document_ref = guest::EntityRef::External(document.clone());
        let deleted = event == "delete";

        let mut observed_attributes = vec![
            text_attribute("document_id", document_id.clone()),
            text_attribute("document_type", document_type.clone()),
            text_attribute("event", event.to_string()),
            bool_attribute("deleted", deleted),
        ];
        push_optional(&mut observed_attributes, "path", path.clone());
        push_optional(&mut observed_attributes, "title", title.clone());
        push_optional(&mut observed_attributes, "body", body.clone());
        push_optional(&mut observed_attributes, "status", status.clone());
        push_optional(&mut observed_attributes, "author", author.clone());
        push_optional(&mut observed_attributes, "date", date.clone());
        if !tags.is_empty() {
            observed_attributes.push(string_list_attribute("tags", &tags));
        }
        if let Some(frontmatter) = frontmatter {
            observed_attributes.push(map_attribute("frontmatter", frontmatter));
        }
        push_optional(
            &mut observed_attributes,
            "frontmatter_json",
            frontmatter_json.clone(),
        );
        push_optional(
            &mut observed_attributes,
            "sections_json",
            sections_json.clone(),
        );
        push_optional(
            &mut observed_attributes,
            "payload_json",
            payload_json.clone(),
        );
        push_optional(&mut observed_attributes, "revision", revision.clone());

        let mut subjects = vec![document_ref.clone()];
        let mut relations = Vec::new();
        for (relation_name, target_ids) in [
            ("supersedes", supersedes),
            ("enables", enables),
            ("triggers", triggers),
            ("depends-on", depends_on),
            ("implements", implements),
            ("conflicts-with", conflicts_with),
            ("related", related),
        ] {
            for target_id in target_ids {
                if target_id.is_empty() {
                    return Err(invalid_request("relation target id must not be empty"));
                }
                if target_id == document.id {
                    return Err(invalid_request("document cannot relate to itself"));
                }
                let target = document_address(target_id);
                subjects.push(guest::EntityRef::External(target.clone()));
                relations.push(relation(
                    schema_key.clone(),
                    relation_name,
                    document_ref.clone(),
                    guest::EntityRef::External(target),
                ));
            }
        }

        let mut event_attributes = vec![
            text_attribute("event", event.to_string()),
            text_attribute("document_id", document_id),
            text_attribute("document_type", document_type),
            bool_attribute("deleted", deleted),
            text_attribute("source_id", source.id.clone()),
        ];
        push_optional(&mut event_attributes, "path", path);
        push_optional(&mut event_attributes, "title", title);
        push_optional(&mut event_attributes, "revision", revision);
        push_optional(&mut event_attributes, "payload_json", payload_json);
        push_optional(&mut event_attributes, "before_json", before_json);
        push_optional(&mut event_attributes, "after_json", after_json);
        push_optional(&mut event_attributes, "diff_json", diff_json);

        Ok(guest::Transaction {
            seed: observation_seed(&source, event),
            source: Some(guest::SourceOperation {
                id: source,
                parents: Vec::new(),
            }),
            intents: vec![
                guest::Intent::RegisterSchema(schema_definition(schema_key.clone())),
                guest::Intent::ObserveEntity(guest::EntityObservation {
                    entity: document,
                    schema: schema_key.clone(),
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

    fn install(request: guest::InstallRequest) -> Result<guest::InstallPlan, guest::PluginError> {
        if request.executable.trim().is_empty() {
            return Err(invalid_request("install executable must not be empty"));
        }
        if request.component.trim().is_empty() {
            return Err(invalid_request("install component must not be empty"));
        }

        let mut operations = Vec::new();
        for event in ["create", "update", "delete"] {
            let mut path = String::from(".dg/hooks/on_");
            path.push_str(event);
            operations.push(guest::FileOperation {
                path,
                content: hook_script(&request.executable, &request.component),
                strategy: guest::FileStrategy::Create,
                executable: true,
                expected: None,
            });
        }
        Ok(guest::InstallPlan { operations })
    }
}

fn hook_script(executable: &str, component: &str) -> String {
    let mut script = String::from("#!/bin/sh\nset -eu\nexec ");
    shell_quote(&mut script, executable);
    script.push_str(" ingest --with-plugin ");
    shell_quote(&mut script, component);
    script.push_str(" --plugin-id dg --path \"$PWD\" \"$@\"\n");
    script
}

fn shell_quote(output: &mut String, value: &str) {
    output.push('\'');
    for character in value.chars() {
        if character == '\'' {
            output.push_str("'\\''");
        } else {
            output.push(character);
        }
    }
    output.push('\'');
}

fn schema_definition(schema_key: guest::SchemaKey) -> guest::SchemaDefinition {
    let document_type = guest::EntityType {
        namespace: NAMESPACE.into(),
        kind: DOCUMENT_KIND.into(),
    };

    guest::SchemaDefinition {
        key: schema_key,
        requires: Vec::new(),
        entities: vec![guest::EntitySchema {
            kind: DOCUMENT_KIND.into(),
            fields: vec![
                field("document_id", guest::ValueType::StringValue, true),
                field("document_type", guest::ValueType::StringValue, true),
                field("path", guest::ValueType::StringValue, false),
                field("title", guest::ValueType::StringValue, false),
                field("body", guest::ValueType::StringValue, false),
                field("status", guest::ValueType::StringValue, false),
                field("author", guest::ValueType::StringValue, false),
                field("date", guest::ValueType::StringValue, false),
                field(
                    "tags",
                    guest::ValueType::ListValue(guest::ScalarValueType::StringValue),
                    false,
                ),
                field(
                    "frontmatter",
                    guest::ValueType::MapValue(guest::MapType {
                        fields: Vec::new(),
                        allow_unknown: true,
                    }),
                    false,
                ),
                field("frontmatter_json", guest::ValueType::StringValue, false),
                field("sections_json", guest::ValueType::StringValue, false),
                field("payload_json", guest::ValueType::StringValue, false),
                field("revision", guest::ValueType::StringValue, false),
                field("event", guest::ValueType::StringValue, true),
                field("deleted", guest::ValueType::Boolean, true),
            ],
            allow_unknown_fields: false,
        }],
        relations: vec![
            relation_schema(
                "supersedes",
                document_type.clone(),
                document_type.clone(),
                guest::ExplanationRole::Supporting,
                guest::ExplanationDirection::SourceExplainsTarget,
            ),
            relation_schema(
                "enables",
                document_type.clone(),
                document_type.clone(),
                guest::ExplanationRole::Supporting,
                guest::ExplanationDirection::SourceExplainsTarget,
            ),
            relation_schema(
                "triggers",
                document_type.clone(),
                document_type.clone(),
                guest::ExplanationRole::Causal,
                guest::ExplanationDirection::SourceExplainsTarget,
            ),
            relation_schema(
                "depends-on",
                document_type.clone(),
                document_type.clone(),
                guest::ExplanationRole::Supporting,
                guest::ExplanationDirection::SourceExplainsTarget,
            ),
            relation_schema(
                "implements",
                document_type.clone(),
                document_type.clone(),
                guest::ExplanationRole::Supporting,
                guest::ExplanationDirection::SourceExplainsTarget,
            ),
            relation_schema(
                "conflicts-with",
                document_type.clone(),
                document_type.clone(),
                guest::ExplanationRole::Contradicting,
                guest::ExplanationDirection::Symmetric,
            ),
            relation_schema(
                "related",
                document_type.clone(),
                document_type,
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

fn document_address(id: String) -> guest::EntityAddress {
    guest::EntityAddress {
        namespace: NAMESPACE.into(),
        kind: DOCUMENT_KIND.into(),
        id,
    }
}

fn infer_document_type(document_id: &str) -> String {
    document_id
        .split_once('-')
        .map(|(prefix, _)| prefix.to_ascii_lowercase())
        .unwrap_or_else(|| "document".into())
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

fn bool_attribute(name: &str, value: bool) -> guest::Attribute {
    guest::Attribute {
        name: name.into(),
        value: guest::Value::Boolean(value),
    }
}

fn string_list_attribute(name: &str, values: &[String]) -> guest::Attribute {
    guest::Attribute {
        name: name.into(),
        value: guest::Value::ListValue(
            values
                .iter()
                .cloned()
                .map(guest::ScalarValue::StringValue)
                .collect(),
        ),
    }
}

fn map_attribute(name: &str, values: Vec<guest::ScalarAttribute>) -> guest::Attribute {
    guest::Attribute {
        name: name.into(),
        value: guest::Value::MapValue(values),
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

fn optional_scalar_map(
    attributes: &mut BTreeMap<String, guest::Value>,
    name: &str,
) -> Result<Option<Vec<guest::ScalarAttribute>>, guest::PluginError> {
    match attributes.remove(name) {
        None => Ok(None),
        Some(guest::Value::MapValue(values)) => {
            let mut names = BTreeSet::new();
            for value in &values {
                if !names.insert(value.name.clone()) {
                    return Err(invalid_request("map attribute contains duplicate fields"));
                }
            }
            Ok(Some(values))
        }
        Some(_) => Err(invalid_request("attribute must be a scalar map")),
    }
}

fn observation_seed(source: &guest::EntityAddress, event: &str) -> String {
    let mut seed = String::from("dg-observation-v1\0");
    seed.push_str(&source.namespace);
    seed.push('\0');
    seed.push_str(&source.kind);
    seed.push('\0');
    seed.push_str(&source.id);
    seed.push('\0');
    seed.push_str(event);
    seed
}

fn invalid_request(message: &str) -> guest::PluginError {
    guest::PluginError {
        kind: guest::PluginErrorKind::InvalidRequest,
        message: message.to_string(),
    }
}

export!(DgPlugin);
