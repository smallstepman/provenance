#![no_std]

extern crate alloc;

use alloc::{vec, vec::Vec};
wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

mod guest {
    pub use super::*;
}
struct HelloPlugin;

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
        _request: guest::ObservationRequest,
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
        let schema = guest::SchemaDefinition {
            key: schema_key.clone(),
            requires: Vec::new(),
            entities: vec![task_schema],
            relations: vec![guest::RelationSchema {
                relation_type: relation_type.clone(),
                source: guest::EntityTypePattern::External(task_type.clone()),
                target: guest::EntityTypePattern::External(task_type),
                explanation: Some(guest::ExplanationSemantics {
                    role: guest::ExplanationRole::Supporting,
                    direction: guest::ExplanationDirection::SourceExplainsTarget,
                }),
            }],
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

        Ok(guest::Transaction {
            seed: "hello-observation".into(),
            source: Some(guest::SourceOperation {
                id: guest::EntityAddress {
                    namespace: "tracker".into(),
                    kind: "event".into(),
                    id: "hello-1".into(),
                },
                parents: Vec::new(),
            }),
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
                    relations: vec![relation],
                    requires: Vec::new(),
                    attributes: Vec::new(),
                }),
            ],
            attributes: Vec::new(),
        })
    }
}

export!(HelloPlugin);
