#![no_std]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::{format, string::String, vec, vec::Vec};
wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

pub(crate) mod guest {
    pub(crate) use super::*;
}
struct HelloPlugin;

fn requested_commit(request: &guest::ObservationRequest) -> Option<guest::EntityAddress> {
    request
        .attributes
        .iter()
        .find_map(|attribute| match &attribute.value {
            guest::Value::EntityValue(guest::EntityRef::External(address))
                if address.namespace == "jj" && address.kind == "commit" =>
            {
                Some(address.clone())
            }
            _ => None,
        })
}
fn test_mode(request: &guest::ObservationRequest) -> Option<&str> {
    request.attributes.iter().find_map(|attribute| {
        (attribute.name == "test-mode").then_some(match &attribute.value {
            guest::Value::StringValue(value) => value.as_str(),
            _ => "",
        })
    })
}

impl Guest for HelloPlugin {
    fn manifest() -> guest::PluginManifest {
        guest::PluginManifest {
            id: "hello-tracker".into(),
            abi_version: "0.1.0".into(),
            namespace: "tracker".into(),
            schemas: vec![guest::SchemaKey {
                namespace: "tracker".into(),
                version: "1".into(),
            }],
            required_schemas: Vec::new(),
            capabilities: vec!["emit-observations".into()],
        }
    }

    fn observe(
        request: guest::ObservationRequest,
    ) -> Result<guest::Transaction, guest::PluginError> {
        let schema_key = guest::SchemaKey {
            namespace: "tracker".into(),
            version: "1".into(),
        };
        let task_type = guest::EntityType {
            namespace: "tracker".into(),
            kind: "task".into(),
        };
        let task_schema = guest::EntitySchema {
            kind: "task".into(),
            fields: vec![guest::FieldDefinition {
                name: "title".into(),
                schema: guest::FieldSchema {
                    value_type: guest::ValueType::StringValue,
                    required: true,
                },
            }],
            allow_unknown_fields: false,
        };
        let relation_type = guest::RelationType {
            namespace: "tracker".into(),
            name: "linked-to".into(),
        };
        let implemented_by_type = guest::RelationType {
            namespace: "tracker".into(),
            name: "implemented-by".into(),
        };
        let commit_type = guest::EntityType {
            namespace: "jj".into(),
            kind: "commit".into(),
        };
        let schema = guest::SchemaDefinition {
            key: schema_key.clone(),
            requires: Vec::new(),
            entities: vec![task_schema],
            relations: vec![
                guest::RelationSchema {
                    relation_type: relation_type.clone(),
                    source: guest::EntityTypePattern::External(task_type.clone()),
                    target: guest::EntityTypePattern::External(task_type.clone()),
                    explanation: Some(guest::ExplanationSemantics {
                        role: guest::ExplanationRole::Supporting,
                        direction: guest::ExplanationDirection::SourceExplainsTarget,
                    }),
                },
                guest::RelationSchema {
                    relation_type: implemented_by_type.clone(),
                    source: guest::EntityTypePattern::External(task_type),
                    target: guest::EntityTypePattern::External(commit_type.clone()),
                    explanation: Some(guest::ExplanationSemantics {
                        role: guest::ExplanationRole::Supporting,
                        direction: guest::ExplanationDirection::TargetExplainsSource,
                    }),
                },
            ],
        };

        let first = guest::EntityAddress {
            namespace: "tracker".into(),
            kind: "task".into(),
            id: "123".into(),
        };
        let second = guest::EntityAddress {
            namespace: "tracker".into(),
            kind: "task".into(),
            id: "456".into(),
        };
        let relation = guest::Relation {
            schema: schema_key.clone(),
            relation_type,
            source: guest::EntityRef::External(first.clone()),
            target: guest::EntityRef::External(second.clone()),
            attributes: Vec::new(),
        };
        let mut relations = vec![relation];
        if let Some(commit) = requested_commit(&request) {
            relations.push(guest::Relation {
                schema: schema_key.clone(),
                relation_type: implemented_by_type,
                source: guest::EntityRef::External(first.clone()),
                target: guest::EntityRef::External(commit),
                attributes: Vec::new(),
            });
        }
        let source = guest::EntityAddress {
            namespace: if test_mode(&request) == Some("bad-source") {
                "other".into()
            } else {
                request.source.namespace.clone()
            },
            kind: request.source.kind.clone(),
            id: request.source.id.clone(),
        };
        let seed = if test_mode(&request) == Some("empty-seed") {
            String::new()
        } else {
            format!(
                "hello-observation\0{}\0{}\0{}",
                source.namespace, source.kind, source.id
            )
        };
        let source_parents = if test_mode(&request) == Some("bad-parent") {
            vec![guest::EntityAddress {
                namespace: "other".into(),
                kind: "event".into(),
                id: "foreign-parent".into(),
            }]
        } else {
            Vec::new()
        };

        Ok(guest::Transaction {
            seed,
            source: if test_mode(&request) == Some("empty-source") {
                None
            } else {
                Some(guest::SourceOperation {
                    id: source,
                    parents: source_parents,
                })
            },
            intents: vec![
                guest::Intent::RegisterSchema(schema),
                guest::Intent::ObserveEntity(guest::EntityObservation {
                    entity: first.clone(),
                    schema: schema_key.clone(),
                    attributes: vec![guest::Attribute {
                        name: "title".into(),
                        value: guest::Value::StringValue("first task".into()),
                    }],
                }),
                guest::Intent::ObserveEntity(guest::EntityObservation {
                    entity: second,
                    schema: schema_key,
                    attributes: vec![guest::Attribute {
                        name: "title".into(),
                        value: guest::Value::StringValue("second task".into()),
                    }],
                }),
                guest::Intent::RecordEvent(guest::EventIntent {
                    session: None,
                    actor: None,
                    additional_parents: Vec::new(),
                    subjects: vec![guest::EntityRef::External(first)],
                    relations,
                    requires: Vec::new(),
                    attributes: Vec::new(),
                }),
            ],
            attributes: Vec::new(),
        })
    }
}

export!(HelloPlugin);
