//! Declarative query types and the pure reference query engine.

use crate::Result;
use crate::state::{Fact, GraphEdge, Projection, State, find_relation_schema, matches_entity_type};
use provenance_data_model::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

// NAMED DECLARATIVE QUERIES
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QueryKey {
    pub namespace: Namespace,
    pub version: SchemaVersion,
    pub name: QueryName,
}

/// Named queries supplied by an ontology/plugin.
///
/// They still compile to generic kernel queries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedQueryDefinition {
    pub key: QueryKey,
    pub input: EntityTypePattern,
    pub template: QueryTemplate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryTemplate {
    History,
    Explain {
        max_depth: Option<usize>,
        roles: BTreeSet<ExplanationRole>,
    },
    Traverse {
        direction: Direction,
        relations: RelationSelector,
        max_depth: Option<usize>,
    },
}

// QUERY LANGUAGE
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Incoming,
    Outgoing,
    Both,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationSelector {
    Any,
    Types(BTreeSet<RelationType>),
    /// Follow only relations whose schemas declare explanation semantics.
    Explanatory,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Predicate<M: Model> {
    Any,
    EntityType(EntityTypePattern),
    PropertyEquals { field: FieldName, value: Value<M> },
    And(Vec<Predicate<M>>),
    Or(Vec<Predicate<M>>),
    Not(Box<Predicate<M>>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Query<M: Model> {
    Lookup {
        entity: EntityRef<M>,
    },
    History {
        entity: EntityRef<M>,
    },
    Traverse {
        roots: BTreeSet<EntityRef<M>>,
        direction: Direction,
        relations: RelationSelector,
        max_depth: Option<usize>,
        predicate: Predicate<M>,
    },
    /// Generic `why`.
    ///
    /// Its behavior is driven by schema-declared ExplanationSemantics.
    Explain {
        root: EntityRef<M>,
        max_depth: Option<usize>,
        roles: BTreeSet<ExplanationRole>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct QueryResult<M: Model> {
    pub entities: BTreeSet<EntityRef<M>>,
    pub edges: BTreeSet<GraphEdge<M>>,
    pub events: BTreeSet<EventId<M>>,
    pub operations: BTreeSet<OperationId<M>>,
}

impl<M: Model> Default for QueryResult<M> {
    fn default() -> Self {
        Self {
            entities: BTreeSet::new(),
            edges: BTreeSet::new(),
            events: BTreeSet::new(),
            operations: BTreeSet::new(),
        }
    }
}

// =============================================================================
// PURE QUERY ENGINE
// =============================================================================

impl<M: Model> State<M> {
    pub fn query(&self, query: &Query<M>) -> Result<QueryResult<M>> {
        match query {
            Query::Lookup { entity } => {
                let mut result = QueryResult::default();
                result.entities.insert(entity.clone());
                Ok(result)
            }

            Query::History { entity } => self.query_history(entity),
            Query::Traverse {
                roots,
                direction,
                relations,
                max_depth,
                predicate,
            } => self.query_traverse(roots, *direction, relations, *max_depth, predicate),
            Query::Explain {
                root,
                max_depth,
                roles,
            } => self.query_explain(root, *max_depth, roles),
        }
    }

    fn query_history(&self, entity: &EntityRef<M>) -> Result<QueryResult<M>> {
        let mut result = QueryResult::default();
        result.entities.insert(entity.clone());
        if let EntityRef::External(address) = entity
            && let Some(operations) = self.projection.entity_history.get(address)
        {
            result.operations.extend(operations.iter().cloned());
        }

        for event in self.projection.events.values() {
            let touches_subject = event.subjects.contains(entity);
            let touches_relation = event
                .relations
                .iter()
                .any(|relation| &relation.from == entity || &relation.to == entity);
            if touches_subject || touches_relation {
                result.events.insert(event.id.clone());
            }
        }
        for (operation_id, operation) in self.operations.iter() {
            if operation.facts.iter().any(|fact| {
                matches!(
                    fact,
                    Fact::EventRecorded(event) if result.events.contains(&event.id)
                )
            }) {
                result.operations.insert(operation_id.clone());
            }
        }

        Ok(result)
    }

    fn query_traverse(
        &self,
        roots: &BTreeSet<EntityRef<M>>,
        direction: Direction,
        relations: &RelationSelector,
        max_depth: Option<usize>,
        predicate: &Predicate<M>,
    ) -> Result<QueryResult<M>> {
        let mut result = QueryResult::default();
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::new();
        for root in roots {
            queue.push_back((root.clone(), 0usize));
        }

        while let Some((entity, depth)) = queue.pop_front() {
            if !visited.insert(entity.clone()) {
                continue;
            }

            if predicate_matches(&self.projection, &entity, predicate) {
                result.entities.insert(entity.clone());
            }

            if max_depth.is_some_and(|max| depth >= max) {
                continue;
            }

            let edges = self.edges_for(&entity, direction);
            for edge in edges {
                if !relation_selected(&self.projection, &edge.relation, relations)? {
                    continue;
                }

                let next = traversal_other_end(&entity, &edge.relation, direction);
                let Some(next) = next else {
                    continue;
                };
                result.edges.insert(edge.clone());
                result.events.insert(edge.event.clone());
                result.operations.insert(edge.operation.clone());
                queue.push_back((next, depth + 1));
            }
        }

        Ok(result)
    }

    fn query_explain(
        &self,
        root: &EntityRef<M>,
        max_depth: Option<usize>,
        roles: &BTreeSet<ExplanationRole>,
    ) -> Result<QueryResult<M>> {
        let mut result = QueryResult::default();
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::new();
        queue.push_back((root.clone(), 0usize));
        while let Some((entity, depth)) = queue.pop_front() {
            if !visited.insert(entity.clone()) {
                continue;
            }

            result.entities.insert(entity.clone());
            if max_depth.is_some_and(|max| depth >= max) {
                continue;
            }

            let mut candidate_edges = Vec::new();
            if let Some(edges) = self.projection.outgoing.get(&entity) {
                candidate_edges.extend(edges.iter());
            }

            if let Some(edges) = self.projection.incoming.get(&entity) {
                candidate_edges.extend(edges.iter());
            }

            for edge in candidate_edges {
                let relation_schema = find_relation_schema(&self.projection, &edge.relation)?;
                let Some(explanation) = relation_schema.explanation else {
                    continue;
                };
                if !roles.is_empty() && !roles.contains(&explanation.role) {
                    continue;
                }

                let next = explanation_predecessor(&entity, &edge.relation, explanation.direction);
                let Some(next) = next else {
                    continue;
                };
                result.edges.insert(edge.clone());
                result.events.insert(edge.event.clone());
                result.operations.insert(edge.operation.clone());
                queue.push_back((next, depth + 1));
            }
        }

        Ok(result)
    }

    fn edges_for(&self, entity: &EntityRef<M>, direction: Direction) -> Vec<GraphEdge<M>> {
        let mut result = BTreeSet::new();
        if matches!(direction, Direction::Outgoing | Direction::Both)
            && let Some(edges) = self.projection.outgoing.get(entity)
        {
            result.extend(edges.iter().cloned());
        }

        if matches!(direction, Direction::Incoming | Direction::Both)
            && let Some(edges) = self.projection.incoming.get(entity)
        {
            result.extend(edges.iter().cloned());
        }

        result.into_iter().collect()
    }
}

fn relation_selected<M: Model>(
    projection: &Projection<M>,
    relation: &Relation<M>,
    selector: &RelationSelector,
) -> Result<bool> {
    match selector {
        RelationSelector::Any => Ok(true),
        RelationSelector::Types(types) => Ok(types.contains(&relation.relation_type)),
        RelationSelector::Explanatory => Ok(find_relation_schema(projection, relation)?
            .explanation
            .is_some()),
    }
}

fn traversal_other_end<M: Model>(
    current: &EntityRef<M>,
    relation: &Relation<M>,
    direction: Direction,
) -> Option<EntityRef<M>> {
    match direction {
        Direction::Outgoing if &relation.from == current => Some(relation.to.clone()),
        Direction::Incoming if &relation.to == current => Some(relation.from.clone()),
        Direction::Both if &relation.from == current => Some(relation.to.clone()),
        Direction::Both if &relation.to == current => Some(relation.from.clone()),
        _ => None,
    }
}

fn explanation_predecessor<M: Model>(
    current: &EntityRef<M>,
    relation: &Relation<M>,
    direction: ExplanationDirection,
) -> Option<EntityRef<M>> {
    match direction {
        ExplanationDirection::FromExplainedByTo if &relation.from == current => {
            Some(relation.to.clone())
        }
        ExplanationDirection::ToExplainedByFrom if &relation.to == current => {
            Some(relation.from.clone())
        }
        ExplanationDirection::Symmetric if &relation.from == current => Some(relation.to.clone()),
        ExplanationDirection::Symmetric if &relation.to == current => Some(relation.from.clone()),
        _ => None,
    }
}

fn predicate_matches<M: Model>(
    projection: &Projection<M>,
    entity: &EntityRef<M>,
    predicate: &Predicate<M>,
) -> bool {
    match predicate {
        Predicate::Any => true,
        Predicate::EntityType(expected) => matches_entity_type(entity, expected),
        Predicate::PropertyEquals { field, value } => {
            let EntityRef::External(address) = entity else {
                return false;
            };
            projection
                .entities
                .get(address)
                .and_then(|observation| observation.attributes.get(field))
                == Some(value)
        }
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_matches(projection, entity, predicate)),
        Predicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_matches(projection, entity, predicate)),
        Predicate::Not(predicate) => !predicate_matches(projection, entity, predicate),
    }
}
