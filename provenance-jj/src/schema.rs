use std::collections::{BTreeMap, BTreeSet};

use provenance_core::{
    EntityKind, EntitySchema, EntityType, EntityTypePattern, ExplanationDirection, ExplanationRole,
    ExplanationSemantics, FieldSchema, NamedQueryDefinition, QueryKey, QueryTemplate, RelationName,
    RelationSchema, RelationType, SchemaDefinition, SchemaKey, ValueType,
};

use crate::{CHANGE_KIND, COMMIT_KIND, JJ_NAMESPACE, OPERATION_KIND, WORKSPACE_KIND};

pub const JJ_SCHEMA_VERSION: &str = "1";
pub const WHY_QUERY_NAME: &str = "why";

pub fn jj_schema_key() -> SchemaKey {
    SchemaKey::new(JJ_NAMESPACE, JJ_SCHEMA_VERSION)
}

fn entity_type(kind: impl Into<EntityKind>) -> EntityType {
    EntityType::new(JJ_NAMESPACE, kind)
}

fn external_pattern(kind: impl Into<EntityKind>) -> EntityTypePattern {
    EntityTypePattern::External(entity_type(kind))
}

fn entity_schema(
    kind: impl Into<EntityKind>,
    fields: impl IntoIterator<Item = (&'static str, FieldSchema)>,
) -> EntitySchema {
    EntitySchema::new(entity_type(kind), fields, false)
}

fn relation(
    name: impl Into<RelationName>,
    from: impl Into<EntityKind>,
    to: impl Into<EntityKind>,
    role: ExplanationRole,
    direction: ExplanationDirection,
) -> RelationSchema {
    RelationSchema::new(
        RelationType::new(JJ_NAMESPACE, name),
        external_pattern(from),
        external_pattern(to),
        Some(ExplanationSemantics::new(role, direction)),
    )
}

/// The complete JJ ontology registered by the adapter on first ingestion.
pub fn jj_schema() -> SchemaDefinition {
    SchemaDefinition {
        key: jj_schema_key(),
        requires: BTreeSet::new(),
        entities: BTreeMap::from([
            (
                EntityKind::from(COMMIT_KIND),
                entity_schema(
                    COMMIT_KIND,
                    [
                        ("commit_id", FieldSchema::required(ValueType::String)),
                        ("change_id", FieldSchema::required(ValueType::String)),
                        ("description", FieldSchema::required(ValueType::String)),
                        ("author", FieldSchema::required(ValueType::String)),
                        ("timestamp", FieldSchema::required(ValueType::String)),
                    ],
                ),
            ),
            (
                EntityKind::from(CHANGE_KIND),
                entity_schema(
                    CHANGE_KIND,
                    [
                        ("change_id", FieldSchema::required(ValueType::String)),
                        (
                            "current_commit",
                            FieldSchema::required(ValueType::Entity(external_pattern(COMMIT_KIND))),
                        ),
                    ],
                ),
            ),
            (
                EntityKind::from(OPERATION_KIND),
                entity_schema(
                    OPERATION_KIND,
                    [
                        ("operation_id", FieldSchema::required(ValueType::String)),
                        ("description", FieldSchema::required(ValueType::String)),
                    ],
                ),
            ),
            (
                EntityKind::from(WORKSPACE_KIND),
                entity_schema(
                    WORKSPACE_KIND,
                    [("workspace_id", FieldSchema::required(ValueType::String))],
                ),
            ),
        ]),
        relations: BTreeMap::from([
            (
                RelationName::from("belongs-to-change"),
                relation(
                    "belongs-to-change",
                    COMMIT_KIND,
                    CHANGE_KIND,
                    ExplanationRole::Primary,
                    ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("predecessor-of"),
                relation(
                    "predecessor-of",
                    COMMIT_KIND,
                    COMMIT_KIND,
                    ExplanationRole::Primary,
                    ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("produced"),
                relation(
                    "produced",
                    OPERATION_KIND,
                    COMMIT_KIND,
                    ExplanationRole::Primary,
                    ExplanationDirection::ToExplainedByFrom,
                ),
            ),
            (
                RelationName::from("follows"),
                relation(
                    "follows",
                    OPERATION_KIND,
                    OPERATION_KIND,
                    ExplanationRole::Supporting,
                    ExplanationDirection::FromExplainedByTo,
                ),
            ),
            (
                RelationName::from("contains"),
                relation(
                    "contains",
                    WORKSPACE_KIND,
                    COMMIT_KIND,
                    ExplanationRole::Contextual,
                    ExplanationDirection::ToExplainedByFrom,
                ),
            ),
            (
                RelationName::from("currently-at"),
                relation(
                    "currently-at",
                    WORKSPACE_KIND,
                    COMMIT_KIND,
                    ExplanationRole::Contextual,
                    ExplanationDirection::ToExplainedByFrom,
                ),
            ),
        ]),
    }
}

pub fn jj_why_query() -> NamedQueryDefinition {
    NamedQueryDefinition {
        key: QueryKey::new(JJ_NAMESPACE, JJ_SCHEMA_VERSION, WHY_QUERY_NAME),
        input: external_pattern(COMMIT_KIND),
        template: QueryTemplate::Explain {
            max_depth: None,
            roles: BTreeSet::from([ExplanationRole::Primary, ExplanationRole::Supporting]),
        },
    }
}
