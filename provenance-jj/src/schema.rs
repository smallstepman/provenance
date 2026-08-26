use std::collections::{BTreeMap, BTreeSet};

use provenance_core::{
    EntityKind, EntitySchema, EntityType, EntityTypePattern, ExplanationDirection, ExplanationRole,
    ExplanationSemantics, FieldName, FieldSchema, NamedQueryDefinition, Namespace, QueryKey,
    QueryName, QueryTemplate, RelationName, RelationSchema, RelationType, SchemaDefinition,
    SchemaKey, SchemaVersion, ValueType,
};

use crate::{CHANGE_KIND, COMMIT_KIND, JJ_NAMESPACE, OPERATION_KIND, WORKSPACE_KIND};

pub const JJ_SCHEMA_VERSION: &str = "1";
pub const WHY_QUERY_NAME: &str = "why";

pub fn jj_schema_key() -> SchemaKey {
    SchemaKey {
        namespace: Namespace::from(JJ_NAMESPACE),
        version: SchemaVersion::from(JJ_SCHEMA_VERSION),
    }
}

fn entity_type(kind: &str) -> EntityType {
    EntityType {
        namespace: Namespace::from(JJ_NAMESPACE),
        kind: EntityKind::from(kind),
    }
}

fn external_pattern(kind: &str) -> EntityTypePattern {
    EntityTypePattern::External(entity_type(kind))
}

fn string_field(required: bool) -> FieldSchema {
    FieldSchema {
        value_type: ValueType::String,
        required,
    }
}

fn entity_field(kind: &str, required: bool) -> FieldSchema {
    FieldSchema {
        value_type: ValueType::Entity(external_pattern(kind)),
        required,
    }
}

fn entity_schema(
    kind: &str,
    fields: impl IntoIterator<Item = (&'static str, FieldSchema)>,
) -> EntitySchema {
    EntitySchema {
        entity_type: entity_type(kind),
        fields: fields
            .into_iter()
            .map(|(name, schema)| (FieldName::from(name), schema))
            .collect(),
        allow_unknown_fields: false,
    }
}

fn relation(
    name: &'static str,
    from: &str,
    to: &str,
    role: ExplanationRole,
    direction: ExplanationDirection,
) -> RelationSchema {
    RelationSchema {
        relation_type: RelationType {
            namespace: Namespace::from(JJ_NAMESPACE),
            name: RelationName::from(name),
        },
        from: external_pattern(from),
        to: external_pattern(to),
        explanation: Some(ExplanationSemantics { role, direction }),
    }
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
                        ("commit_id", string_field(true)),
                        ("change_id", string_field(true)),
                        ("description", string_field(true)),
                        ("author", string_field(true)),
                        ("timestamp", string_field(true)),
                    ],
                ),
            ),
            (
                EntityKind::from(CHANGE_KIND),
                entity_schema(
                    CHANGE_KIND,
                    [
                        ("change_id", string_field(true)),
                        ("current_commit", entity_field(COMMIT_KIND, true)),
                    ],
                ),
            ),
            (
                EntityKind::from(OPERATION_KIND),
                entity_schema(
                    OPERATION_KIND,
                    [
                        ("operation_id", string_field(true)),
                        ("description", string_field(true)),
                    ],
                ),
            ),
            (
                EntityKind::from(WORKSPACE_KIND),
                entity_schema(WORKSPACE_KIND, [("workspace_id", string_field(true))]),
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
        key: QueryKey {
            namespace: Namespace::from(JJ_NAMESPACE),
            version: SchemaVersion::from(JJ_SCHEMA_VERSION),
            name: QueryName::from(WHY_QUERY_NAME),
        },
        input: external_pattern(COMMIT_KIND),
        template: QueryTemplate::Explain {
            max_depth: None,
            roles: BTreeSet::from([ExplanationRole::Primary, ExplanationRole::Supporting]),
        },
    }
}
