use std::collections::{BTreeMap, BTreeSet};

use provenance_core::{
    Attributes, EntityAddress, EntityKind, EntityObservation, EntityRef, EntitySchema, EntityType,
    EntityTypePattern, Event, EventId, ExplanationDirection, ExplanationRole, ExplanationSemantics,
    Fact, FieldName, FieldSchema, Model, Namespace, Operation, OperationId, Predicate, Query,
    Relation, RelationName, RelationType, SchemaDefinition, SchemaKey, SchemaVersion, Value,
    ValueType,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TestModel;

impl Model for TestModel {
    type Id = String;
    type Seed = String;
    type ExternalId = String;
    type Payload = Vec<u8>;
}

pub fn entity(id: impl Into<String>) -> EntityRef<TestModel> {
    EntityRef::External(EntityAddress::new("fixture", "node", id.into()))
}

pub fn schema() -> SchemaDefinition {
    let node_type = EntityType {
        namespace: Namespace::from("fixture"),
        kind: EntityKind::from("node"),
    };
    let relation_type = RelationType::new("fixture", RelationName::from("precedes"));
    SchemaDefinition {
        key: SchemaKey {
            namespace: Namespace::from("fixture"),
            version: SchemaVersion::from("1"),
        },
        requires: BTreeSet::new(),
        entities: BTreeMap::from([(
            node_type.kind.clone(),
            EntitySchema {
                entity_type: node_type.clone(),
                fields: BTreeMap::from([(
                    FieldName::from("title"),
                    FieldSchema::required(ValueType::String),
                )]),
                allow_unknown_fields: false,
            },
        )]),
        relations: BTreeMap::from([(
            relation_type.name.clone(),
            provenance_core::RelationSchema {
                relation_type,
                from: EntityTypePattern::External(node_type.clone()),
                to: EntityTypePattern::External(node_type),
                explanation: Some(ExplanationSemantics {
                    role: ExplanationRole::Primary,
                    direction: ExplanationDirection::FromExplainedByTo,
                }),
            },
        )]),
    }
}

pub fn operation_with_nodes(node_count: usize) -> Operation<TestModel> {
    assert!(node_count >= 2);
    let schema = schema();
    let entities = (0..node_count)
        .map(|index| entity(index.to_string()))
        .collect::<Vec<_>>();
    let relation_type = RelationType::new("fixture", RelationName::from("precedes"));
    let relations = entities
        .windows(2)
        .map(|window| Relation {
            schema: schema.key.clone(),
            relation_type: relation_type.clone(),
            from: window[0].clone(),
            to: window[1].clone(),
            attributes: Attributes::new(),
        })
        .collect::<BTreeSet<_>>();
    let event = Event {
        id: EventId::<TestModel>::new(format!("event-{node_count}")),
        session: None,
        actor: None,
        parents: BTreeSet::new(),
        subjects: entities.iter().cloned().collect(),
        relations,
        requires: BTreeSet::new(),
        attributes: Attributes::new(),
    };

    let mut facts = vec![Fact::SchemaRegistered(schema)];
    facts.extend(entities.into_iter().enumerate().map(|(index, entity)| {
        let address = match entity {
            EntityRef::External(address) => address,
            EntityRef::Internal(_) => unreachable!(),
        };
        Fact::EntityObserved(EntityObservation {
            entity: address,
            schema: SchemaKey {
                namespace: Namespace::from("fixture"),
                version: SchemaVersion::from("1"),
            },
            attributes: BTreeMap::from([(
                FieldName::from("title"),
                Value::String(format!("node-{index}").into()),
            )]),
        })
    }));
    facts.push(Fact::EventRecorded(event));

    Operation {
        id: OperationId::<TestModel>::new(format!("operation-{node_count}")),
        parents: BTreeSet::new(),
        source: None,
        facts,
        attributes: Attributes::new(),
    }
}

pub fn sample_operations() -> Vec<Operation<TestModel>> {
    vec![operation_with_nodes(4)]
}

pub fn explain_query() -> Query<TestModel> {
    Query::Explain {
        root: entity("0"),
        max_depth: None,
        roles: [ExplanationRole::Primary].into_iter().collect(),
    }
}

pub fn history_query() -> Query<TestModel> {
    Query::History {
        entity: entity("2"),
    }
}

pub fn traverse_query() -> Query<TestModel> {
    Query::Traverse {
        roots: [entity("0")].into_iter().collect(),
        direction: provenance_core::Direction::Outgoing,
        relations: provenance_core::RelationSelector::Any,
        max_depth: Some(3),
        predicate: Predicate::PropertyEquals {
            field: FieldName::from("title"),
            value: Value::String("node-2".into()),
        },
    }
}
