//! Backend-neutral projection indexing for `provenance-core`.
//!
//! This crate owns the projection contract, query semantics, and optional
//! database implementations. Concrete implementations live under
//! [`implementation`] and are selected with mutually exclusive feature flags;
//! the generic [`ProjectionIndex`] supplies rebuild, source-store traversal,
//! and query execution consistently across those engines.

use std::collections::{BTreeSet, VecDeque};
use std::fmt::{Debug, Display};
use std::marker::PhantomData;

use provenance_core::{
    EntityObservation, EntityRef, EntityTypePattern, EventId, GraphEdge, Model, Operation,
    OperationId, Predicate, Query, QueryEngine, QueryResult, Relation, RelationSchema,
    RelationSelector, SchemaDefinition, SchemaKey, State,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

pub mod implementation;

#[cfg(any(
    feature = "sqlite",
    feature = "duckdb",
    feature = "doltlite",
    feature = "turso",
    feature = "firebird",
    feature = "lbug",
))]
pub use implementation::{BackendIndex, BackendIndexError, BackendStorage, BackendStorageError};

/// Models supported by a serialized projection storage boundary.
///
/// `provenance-core::Model` intentionally does not require serialization. A
/// concrete projection backend does, because it must persist generic IDs,
/// addresses, values, and operation records.
pub trait IndexModel: Model
where
    Self::Id: Serialize + DeserializeOwned,
    Self::Seed: Serialize + DeserializeOwned,
    Self::ExternalId: Serialize + DeserializeOwned,
    Self::Payload: Serialize + DeserializeOwned,
{
}

impl<M> IndexModel for M
where
    M: Model,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
}

/// Events and operations associated with one entity in a projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryRows<M: Model> {
    pub events: BTreeSet<EventId<M>>,
    pub operations: BTreeSet<OperationId<M>>,
}

impl<M: Model> Default for HistoryRows<M> {
    fn default() -> Self {
        Self {
            events: BTreeSet::new(),
            operations: BTreeSet::new(),
        }
    }
}

/// Storage operations required by the generic projection/query adapter.
///
/// Implementors own database schema, transactions, serialization, and query
/// decoding. They do not own provenance semantics: [`ProjectionIndex`] builds
/// the canonical [`State`] and applies the reference query semantics over the
/// storage reads below.
pub trait ProjectionStorage<M>: Sized
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error: Debug + Display;

    /// Atomically replace all disposable projection data with this state.
    fn replace(&mut self, state: &State<M>) -> Result<(), Self::Error>;

    /// Delete all disposable projection data while leaving the authoritative
    /// operation store untouched.
    fn clear(&mut self) -> Result<(), Self::Error>;

    fn operation_count(&self) -> Result<usize, Self::Error>;

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error>;

    fn entity_observation(
        &self,
        entity: &EntityRef<M>,
    ) -> Result<Option<EntityObservation<M>>, Self::Error>;

    fn schema(&self, key: &SchemaKey) -> Result<Option<SchemaDefinition>, Self::Error>;

    fn history(&self, entity: &EntityRef<M>) -> Result<HistoryRows<M>, Self::Error>;

    fn edges(
        &self,
        entity: &EntityRef<M>,
        direction: provenance_core::Direction,
    ) -> Result<Vec<GraphEdge<M>>, Self::Error>;
}

/// Errors shared by every projection adapter.
#[derive(Debug, Error)]
pub enum ProjectionIndexError<E>
where
    E: Debug + Display,
{
    #[error("projection storage error: {0}")]
    Storage(E),
    #[error("authoritative provenance error: {0}")]
    Authority(String),
    #[error("core projection rebuild rejected the operation graph: {0:?}")]
    Core(provenance_core::Error),
    #[error("invalid indexed provenance: {0}")]
    InvalidProjection(String),
}

/// Generic projection adapter over one concrete storage implementation.
///
/// New database backends normally only implement [`ProjectionStorage`]. The
/// rebuild lifecycle and all `QueryEngine` semantics then come from this type.
pub struct ProjectionIndex<S, M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
    S: ProjectionStorage<M>,
{
    storage: S,
    marker: PhantomData<M>,
}

impl<S, M> ProjectionIndex<S, M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
    S: ProjectionStorage<M>,
    S::Error: Debug + Display,
{
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            marker: PhantomData,
        }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Rebuild the disposable projection from immutable operations.
    pub fn rebuild(
        &mut self,
        operations: &[Operation<M>],
    ) -> Result<(), ProjectionIndexError<S::Error>> {
        let state = state_from_operations(operations).map_err(ProjectionIndexError::Core)?;
        self.storage
            .replace(&state)
            .map_err(ProjectionIndexError::Storage)
    }

    /// Rebuild from the reachable history of any authoritative operation
    /// store. Published heads, not an index, define the input boundary.
    pub fn rebuild_from<T>(&mut self, store: &T) -> Result<(), ProjectionIndexError<S::Error>>
    where
        T: provenance_core::ProvenanceStore<M>,
        T::Error: Display,
    {
        let operations = reachable_operations(store).map_err(|error| match error {
            ReachableOperationsError::Authority(message) => {
                ProjectionIndexError::Authority(message)
            }
        })?;
        self.rebuild(&operations)
    }

    pub fn clear(&mut self) -> Result<(), ProjectionIndexError<S::Error>> {
        self.storage.clear().map_err(ProjectionIndexError::Storage)
    }

    pub fn operation_count(&self) -> Result<usize, ProjectionIndexError<S::Error>> {
        self.storage
            .operation_count()
            .map_err(ProjectionIndexError::Storage)
    }

    pub fn contains(
        &self,
        operation_id: &OperationId<M>,
    ) -> Result<bool, ProjectionIndexError<S::Error>> {
        self.storage
            .contains(operation_id)
            .map_err(ProjectionIndexError::Storage)
    }

    fn execute_query(
        &self,
        query: &Query<M>,
    ) -> Result<QueryResult<M>, ProjectionIndexError<S::Error>> {
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

    fn query_history(
        &self,
        entity: &EntityRef<M>,
    ) -> Result<QueryResult<M>, ProjectionIndexError<S::Error>> {
        let mut result = QueryResult::default();
        result.entities.insert(entity.clone());
        let rows = self
            .storage
            .history(entity)
            .map_err(ProjectionIndexError::Storage)?;
        result.events.extend(rows.events);
        result.operations.extend(rows.operations);
        Ok(result)
    }

    fn query_traverse(
        &self,
        roots: &BTreeSet<EntityRef<M>>,
        direction: provenance_core::Direction,
        relations: &RelationSelector,
        max_depth: Option<usize>,
        predicate: &Predicate<M>,
    ) -> Result<QueryResult<M>, ProjectionIndexError<S::Error>> {
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

            if self.predicate_matches(&entity, predicate)? {
                result.entities.insert(entity.clone());
            }
            if max_depth.is_some_and(|max| depth >= max) {
                continue;
            }

            for edge in self
                .storage
                .edges(&entity, direction)
                .map_err(ProjectionIndexError::Storage)?
            {
                if !self.relation_selected(&edge.relation, relations)? {
                    continue;
                }
                let Some(next) = traversal_other_end(&entity, &edge.relation, direction) else {
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
        roles: &BTreeSet<provenance_core::ExplanationRole>,
    ) -> Result<QueryResult<M>, ProjectionIndexError<S::Error>> {
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

            for edge in self
                .storage
                .edges(&entity, provenance_core::Direction::Both)
                .map_err(ProjectionIndexError::Storage)?
            {
                let Some(explanation) = self.relation_schema(&edge.relation)?.explanation else {
                    continue;
                };
                if !roles.is_empty() && !roles.contains(&explanation.role) {
                    continue;
                }
                let Some(next) =
                    explanation_predecessor(&entity, &edge.relation, explanation.direction)
                else {
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

    fn relation_schema(
        &self,
        relation: &Relation<M>,
    ) -> Result<RelationSchema, ProjectionIndexError<S::Error>> {
        let schema = self
            .storage
            .schema(&relation.schema)
            .map_err(ProjectionIndexError::Storage)?
            .ok_or_else(|| {
                ProjectionIndexError::InvalidProjection("missing relation schema".into())
            })?;
        schema
            .relations
            .get(&relation.relation_type.name)
            .filter(|definition| definition.relation_type == relation.relation_type)
            .cloned()
            .ok_or_else(|| {
                ProjectionIndexError::InvalidProjection("missing relation schema".into())
            })
    }

    fn relation_selected(
        &self,
        relation: &Relation<M>,
        selector: &RelationSelector,
    ) -> Result<bool, ProjectionIndexError<S::Error>> {
        match selector {
            RelationSelector::Any => Ok(true),
            RelationSelector::Types(types) => Ok(types.contains(&relation.relation_type)),
            RelationSelector::Explanatory => {
                Ok(self.relation_schema(relation)?.explanation.is_some())
            }
        }
    }

    fn predicate_matches(
        &self,
        entity: &EntityRef<M>,
        predicate: &Predicate<M>,
    ) -> Result<bool, ProjectionIndexError<S::Error>> {
        match predicate {
            Predicate::Any => Ok(true),
            Predicate::EntityType(expected) => Ok(matches_entity_type(entity, expected)),
            Predicate::PropertyEquals { field, value } => {
                let EntityRef::External(_) = entity else {
                    return Ok(false);
                };
                Ok(self
                    .storage
                    .entity_observation(entity)
                    .map_err(ProjectionIndexError::Storage)?
                    .and_then(|observation| observation.attributes.get(field).cloned())
                    .as_ref()
                    == Some(value))
            }
            Predicate::And(predicates) => predicates
                .iter()
                .map(|predicate| self.predicate_matches(entity, predicate))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.into_iter().all(|value| value)),
            Predicate::Or(predicates) => predicates
                .iter()
                .map(|predicate| self.predicate_matches(entity, predicate))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.into_iter().any(|value| value)),
            Predicate::Not(predicate) => Ok(!self.predicate_matches(entity, predicate)?),
        }
    }
}

impl<S, M> QueryEngine<M> for ProjectionIndex<S, M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
    S: ProjectionStorage<M>,
    S::Error: Debug + Display,
{
    type Error = ProjectionIndexError<S::Error>;

    fn execute(&self, query: &Query<M>) -> Result<QueryResult<M>, Self::Error> {
        self.execute_query(query)
    }
}

/// Common lifecycle contract implemented by every concrete projection index.
pub trait ProjectionBackend<M>: QueryEngine<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    fn rebuild(&mut self, operations: &[Operation<M>]) -> Result<(), Self::Error>;

    fn rebuild_from<T>(&mut self, store: &T) -> Result<(), Self::Error>
    where
        T: provenance_core::ProvenanceStore<M>,
        T::Error: Display;

    fn clear(&mut self) -> Result<(), Self::Error>;

    fn operation_count(&self) -> Result<usize, Self::Error>;

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error>;
}

impl<S, M> ProjectionBackend<M> for ProjectionIndex<S, M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
    S: ProjectionStorage<M>,
    S::Error: Debug + Display,
{
    fn rebuild(&mut self, operations: &[Operation<M>]) -> Result<(), Self::Error> {
        ProjectionIndex::rebuild(self, operations)
    }

    fn rebuild_from<T>(&mut self, store: &T) -> Result<(), Self::Error>
    where
        T: provenance_core::ProvenanceStore<M>,
        T::Error: Display,
    {
        ProjectionIndex::rebuild_from(self, store)
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        ProjectionIndex::clear(self)
    }

    fn operation_count(&self) -> Result<usize, Self::Error> {
        ProjectionIndex::operation_count(self)
    }

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error> {
        ProjectionIndex::contains(self, operation_id)
    }
}

/// Build the canonical core state used as the input to every storage backend.
pub fn state_from_operations<M>(
    operations: &[Operation<M>],
) -> Result<State<M>, provenance_core::Error>
where
    M: Model,
{
    let mut state = State::new();
    for operation in operations {
        state
            .operations
            .declare(operation.id.clone(), operation.clone())?;
    }
    state.rebuild()?;
    Ok(state)
}

#[derive(Debug)]
pub enum ReachableOperationsError {
    Authority(String),
}

impl Display for ReachableOperationsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authority(message) => formatter.write_str(message),
        }
    }
}

/// Load the operation closure reachable from authoritative published heads.
pub fn reachable_operations<M, S>(store: &S) -> Result<Vec<Operation<M>>, ReachableOperationsError>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
    S: provenance_core::ProvenanceStore<M>,
    S::Error: Display,
{
    let mut pending = store
        .heads()
        .map_err(|error| ReachableOperationsError::Authority(error.to_string()))?
        .into_iter()
        .collect::<Vec<_>>();
    let mut seen = BTreeSet::new();
    let mut operations = Vec::new();

    while let Some(operation_id) = pending.pop() {
        if !seen.insert(operation_id.clone()) {
            continue;
        }
        let operation = store
            .get_operation(&operation_id)
            .map_err(|error| ReachableOperationsError::Authority(error.to_string()))?
            .ok_or_else(|| {
                ReachableOperationsError::Authority(format!(
                    "published head references missing operation {:?}",
                    operation_id
                ))
            })?;
        pending.extend(operation.parents.iter().cloned());
        operations.push(operation);
    }

    Ok(operations)
}

fn matches_entity_type<M: Model>(entity: &EntityRef<M>, expected: &EntityTypePattern) -> bool {
    match expected {
        EntityTypePattern::Any => true,
        EntityTypePattern::External(expected) => {
            matches!(entity, EntityRef::External(address) if &address.entity_type() == expected)
        }
        EntityTypePattern::Internal(expected) => {
            matches!(entity.type_pattern(), EntityTypePattern::Internal(actual) if &actual == expected)
        }
    }
}

fn traversal_other_end<M: Model>(
    current: &EntityRef<M>,
    relation: &Relation<M>,
    direction: provenance_core::Direction,
) -> Option<EntityRef<M>> {
    match direction {
        provenance_core::Direction::Outgoing if &relation.from == current => {
            Some(relation.to.clone())
        }
        provenance_core::Direction::Incoming if &relation.to == current => {
            Some(relation.from.clone())
        }
        provenance_core::Direction::Both if &relation.from == current => Some(relation.to.clone()),
        provenance_core::Direction::Both if &relation.to == current => Some(relation.from.clone()),
        _ => None,
    }
}

fn explanation_predecessor<M: Model>(
    current: &EntityRef<M>,
    relation: &Relation<M>,
    direction: provenance_core::ExplanationDirection,
) -> Option<EntityRef<M>> {
    match direction {
        provenance_core::ExplanationDirection::FromExplainedByTo if &relation.from == current => {
            Some(relation.to.clone())
        }
        provenance_core::ExplanationDirection::ToExplainedByFrom if &relation.to == current => {
            Some(relation.from.clone())
        }
        provenance_core::ExplanationDirection::Symmetric if &relation.from == current => {
            Some(relation.to.clone())
        }
        provenance_core::ExplanationDirection::Symmetric if &relation.to == current => {
            Some(relation.from.clone())
        }
        _ => None,
    }
}

#[cfg(feature = "test-support")]
pub mod test_support;
