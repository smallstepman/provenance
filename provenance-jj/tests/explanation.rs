mod support;

use std::collections::{BTreeMap, BTreeSet};

use provenance_core::{
    EntityKind, EntityObservation, EntityRef, EntitySchema, EntityType, EntityTypePattern,
    ExplanationDirection, ExplanationRole, ExplanationSemantics, FieldName, FieldSchema, Intent,
    Namespace, Relation, RelationName, RelationSchema, RelationType, SchemaDefinition, SchemaKey,
    SchemaVersion, Transaction, Value, ValueType,
};

use provenance_jj::{compile_why, external, jj_commit, why_jj_commit};

#[test]
fn why_uri_compiles_to_generic_explain_query() {
    assert_eq!(
        compile_why("jj://commit/abc123").expect("compile why URI"),
        why_jj_commit("abc123")
    );
}

#[test]
fn amend_records_commit_predecessor_and_why_explains_old_commit() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "before\n");
    fixture.commit("before");
    let old_commit = fixture.head_commit();
    let mut service = fixture.service();
    fixture.ingest(&mut service);

    fixture.write_file("README", "after\n");
    fixture.squash();
    let new_commit = fixture.head_commit();
    assert_ne!(old_commit, new_commit);
    fixture.ingest(&mut service);

    let result = service
        .state
        .query(&why_jj_commit(&new_commit))
        .expect("why new commit");
    assert!(result.entities.contains(&jj_commit(old_commit.clone())));
    assert!(result.edges.iter().any(|edge| {
        edge.relation.relation_type.name.as_str() == "predecessor-of"
            && edge.relation.from == jj_commit(&new_commit)
            && edge.relation.to == jj_commit(&old_commit)
    }));
}

#[test]
fn generic_why_crosses_external_tracker_relation() {
    let fixture = support::Fixture::new();
    fixture.write_file("README", "plugin\n");
    fixture.commit("plugin");
    let commit_id = fixture.head_commit();
    let mut service = fixture.service();
    fixture.ingest(&mut service);

    let tracker_schema = tracker_schema();
    let tracker = tracker_task("123");
    let relation = Relation {
        schema: tracker_schema.key.clone(),
        relation_type: RelationType {
            namespace: Namespace::from("tracker"),
            name: RelationName::from("implemented-by"),
        },
        from: tracker.clone(),
        to: jj_commit(&commit_id),
        attributes: BTreeMap::new(),
    };
    let transaction = Transaction {
        seed: "tracker-observation".to_owned(),
        source: None,
        intents: vec![
            Intent::RegisterSchema(tracker_schema),
            Intent::ObserveEntity(EntityObservation {
                entity: match &tracker {
                    EntityRef::External(address) => address.clone(),
                    EntityRef::Internal(_) => unreachable!(),
                },
                schema: SchemaKey {
                    namespace: Namespace::from("tracker"),
                    version: SchemaVersion::from("1"),
                },
                attributes: BTreeMap::from([(
                    FieldName::from("task_id"),
                    Value::String("123".into()),
                )]),
            }),
            Intent::RecordEvent(provenance_core::EventIntent {
                session: None,
                actor: None,
                additional_parents: BTreeSet::new(),
                subjects: BTreeSet::from([tracker.clone(), jj_commit(&commit_id)]),
                relations: BTreeSet::from([relation]),
                requires: BTreeSet::new(),
                attributes: BTreeMap::new(),
            }),
        ],
        attributes: BTreeMap::new(),
    };
    let plan = service
        .kernel
        .transact(&service.state, transaction)
        .expect("tracker transaction");
    service.state = plan.next_state;

    let result = service
        .state
        .query(&why_jj_commit(&commit_id))
        .expect("why JJ commit with tracker relation");
    assert!(result.entities.contains(&tracker));
}

fn tracker_task(id: &str) -> EntityRef<provenance_jj::JjModel> {
    external(provenance_core::EntityAddress {
        namespace: Namespace::from("tracker"),
        kind: EntityKind::from("task"),
        id: id.to_owned(),
    })
}

fn tracker_schema() -> SchemaDefinition {
    let tracker_task_type = EntityType {
        namespace: Namespace::from("tracker"),
        kind: EntityKind::from("task"),
    };
    SchemaDefinition {
        key: SchemaKey {
            namespace: Namespace::from("tracker"),
            version: SchemaVersion::from("1"),
        },
        requires: BTreeSet::new(),
        entities: BTreeMap::from([(
            EntityKind::from("task"),
            EntitySchema {
                entity_type: tracker_task_type.clone(),
                fields: BTreeMap::from([(
                    FieldName::from("task_id"),
                    FieldSchema {
                        value_type: ValueType::String,
                        required: true,
                    },
                )]),
                allow_unknown_fields: false,
            },
        )]),
        relations: BTreeMap::from([(
            RelationName::from("implemented-by"),
            RelationSchema {
                relation_type: RelationType {
                    namespace: Namespace::from("tracker"),
                    name: RelationName::from("implemented-by"),
                },
                from: EntityTypePattern::External(tracker_task_type),
                to: EntityTypePattern::External(EntityType {
                    namespace: Namespace::from(provenance_jj::JJ_NAMESPACE),
                    kind: EntityKind::from("commit"),
                }),
                explanation: Some(ExplanationSemantics {
                    role: ExplanationRole::Primary,
                    direction: ExplanationDirection::ToExplainedByFrom,
                }),
            },
        )]),
    }
}
