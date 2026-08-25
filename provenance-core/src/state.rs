//! Authoritative provenance state, projections, and deterministic replay.

use crate::query::{NamedQueryDefinition, QueryKey};
use crate::retention::*;
use crate::{Error, Result};
use provenance_data_model::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// GENERIC COLLECTIONS
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeclareOutcome {
    Inserted,
    AlreadyPresent,
}

/// Append-only identity -> immutable value.
///
/// absent + value           => insert
/// same identity + same     => idempotent
/// same identity + different=> corruption/collision
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Ord, V: Deserialize<'de>",
))]
pub struct Ledger<K, V> {
    inner: BTreeMap<K, V>,
}

impl<K, V> Default for Ledger<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K, V> Ledger<K, V>
where
    K: Key,
    V: Clone + PartialEq,
{
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn require(&self, key: &K) -> Result<&V> {
        self.get(key).ok_or(Error::Missing)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    pub fn declare(&mut self, key: K, value: V) -> Result<DeclareOutcome> {
        match self.inner.get(&key) {
            None => {
                self.inner.insert(key, value);
                Ok(DeclareOutcome::Inserted)
            }

            Some(existing) if existing == &value => Ok(DeclareOutcome::AlreadyPresent),
            Some(_) => Err(Error::IdentityCollision),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.inner.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.values()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Replaceable / derived mapping.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Ord, V: Deserialize<'de>",
))]
pub struct Index<K, V> {
    inner: BTreeMap<K, V>,
}

impl<K, V> Default for Index<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K, V> Index<K, V>
where
    K: Key,
{
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner.get_mut(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    pub fn set(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }
}

/// Derived set.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "K: Serialize", deserialize = "K: Deserialize<'de> + Ord",))]
pub struct Set<K> {
    inner: BTreeSet<K>,
}

impl<K> Default for Set<K> {
    fn default() -> Self {
        Self {
            inner: BTreeSet::new(),
        }
    }
}

impl<K> Set<K>
where
    K: Key,
{
    pub fn contains(&self, key: &K) -> bool {
        self.inner.contains(key)
    }

    pub fn insert(&mut self, key: K) -> bool {
        self.inner.insert(key)
    }

    pub fn remove(&mut self, key: &K) -> bool {
        self.inner.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = &K> {
        self.inner.iter()
    }

    pub fn cloned(&self) -> BTreeSet<K> {
        self.inner.clone()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Derived K -> `set<V>`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Ord, V: Deserialize<'de> + Ord",
))]
pub struct MultiIndex<K, V> {
    inner: BTreeMap<K, BTreeSet<V>>,
}

impl<K, V> Default for MultiIndex<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K, V> MultiIndex<K, V>
where
    K: Key,
    V: Key,
{
    pub fn get(&self, key: &K) -> Option<&BTreeSet<V>> {
        self.inner.get(key)
    }
    pub fn insert(&mut self, key: K, value: V) {
        self.inner.entry(key).or_default().insert(value);
    }
    pub fn remove(&mut self, key: &K, value: &V) {
        if let Some(values) = self.inner.get_mut(key) {
            values.remove(value);
            if values.is_empty() {
                self.inner.remove(key);
            }
        }
    }
    pub fn iter(&self) -> impl Iterator<Item = (&K, &BTreeSet<V>)> {
        self.inner.iter()
    }
}

// =============================================================================

// KERNEL EVENT
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Event<M: Model> {
    pub id: EventId<M>,
    pub session: Option<SessionId<M>>,
    pub actor: Option<ActorId<M>>,
    pub parents: BTreeSet<EventId<M>>,
    pub subjects: BTreeSet<EntityRef<M>>,
    pub relations: BTreeSet<Relation<M>>,
    pub requires: BTreeSet<ResourceRequirement<M>>,
    pub attributes: Attributes<M>,
}

// =============================================================================

// AUTHORITATIVE FACTS
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Fact<M: Model> {
    SchemaRegistered(SchemaDefinition),
    NamedQueryRegistered(NamedQueryDefinition),
    SourceAnchored(SourceAnchor<M>),
    SessionOpened(Session<M>),
    SessionEnded {
        session: SessionId<M>,
    },
    ActorDeclared(Actor<M>),
    ObjectDeclared(Object<M>),
    EntityObserved(EntityObservation<M>),
    EventRecorded(Event<M>),
    ReplicaDeclared(Replica<M>),
    RetentionClaimed(RetentionClaim<M>),
    RetentionReleased {
        claim: ClaimId<M>,
    },
    ResourceObserved {
        resource: Resource<M>,
        observation: ResourceObservation<M>,
    },
}

// =============================================================================
// AUTHORITATIVE OPERATION
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Operation<M: Model> {
    pub id: OperationId<M>,
    pub parents: BTreeSet<OperationId<M>>,
    pub source: Option<EntityAddress<M>>,
    pub facts: Vec<Fact<M>>,
    pub attributes: Attributes<M>,
}

// =============================================================================
// GRAPH EDGE PROJECTION
// =============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct GraphEdge<M: Model> {
    pub relation: Relation<M>,
    pub event: EventId<M>,
    pub operation: OperationId<M>,
}

// =============================================================================
// PROJECTION
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Projection<M: Model> {
    // -------------------------------------------------------------------------
    // ontology
    // -------------------------------------------------------------------------
    pub schemas: Ledger<SchemaKey, SchemaDefinition>,
    pub named_queries: Ledger<QueryKey, NamedQueryDefinition>, // -------------------------------------------------------------------------
    // source mapping
    // -------------------------------------------------------------------------
    pub source_to_operation: Index<EntityAddress<M>, OperationId<M>>,
    pub source_to_primary_event: Index<EntityAddress<M>, EventId<M>>, // -------------------------------------------------------------------------
    // internal entities
    // -------------------------------------------------------------------------
    pub sessions: Ledger<SessionId<M>, Session<M>>,
    pub ended_sessions: Set<SessionId<M>>,
    pub actors: Ledger<ActorId<M>, Actor<M>>,
    pub objects: Ledger<ObjectId<M>, Object<M>>,
    pub events: Ledger<EventId<M>, Event<M>>,
    pub event_heads: Set<EventId<M>>,
    pub replicas: Ledger<ReplicaId<M>, Replica<M>>, // -------------------------------------------------------------------------
    // heterogeneous SDLC graph
    // -------------------------------------------------------------------------
    /// Latest external entity state.
    pub entities: Index<EntityAddress<M>, EntityObservation<M>>,
    /// Complete operation history for each external entity.
    pub entity_history: MultiIndex<EntityAddress<M>, OperationId<M>>,
    pub outgoing: MultiIndex<EntityRef<M>, GraphEdge<M>>,
    pub incoming: MultiIndex<EntityRef<M>, GraphEdge<M>>, // -------------------------------------------------------------------------
    // retention
    // -------------------------------------------------------------------------
    pub active_retention: Index<ClaimId<M>, RetentionClaim<M>>,
    /// Purely derived from active claims.
    pub required_retention: Index<Resource<M>, RetentionStrength>,
    /// Observed from runtime/backends.
    pub resource_observations: Index<Resource<M>, ResourceObservation<M>>,
}

impl<M: Model> Default for Projection<M> {
    fn default() -> Self {
        Self {
            schemas: Ledger::default(),
            named_queries: Ledger::default(),
            source_to_operation: Index::default(),
            source_to_primary_event: Index::default(),
            sessions: Ledger::default(),
            ended_sessions: Set::default(),
            actors: Ledger::default(),
            objects: Ledger::default(),
            events: Ledger::default(),
            event_heads: Set::default(),
            replicas: Ledger::default(),
            entities: Index::default(),
            entity_history: MultiIndex::default(),
            outgoing: MultiIndex::default(),
            incoming: MultiIndex::default(),
            active_retention: Index::default(),
            required_retention: Index::default(),
            resource_observations: Index::default(),
        }
    }
}

// =============================================================================
// PROJECTION ACCESSORS
// =============================================================================

impl<M: Model> Projection<M> {
    pub fn schema(&self, key: &SchemaKey) -> Option<&SchemaDefinition> {
        self.schemas.get(key)
    }

    pub fn require_schema(&self, key: &SchemaKey) -> Result<&SchemaDefinition> {
        self.schema(key).ok_or(Error::MissingSchema)
    }

    pub fn session(&self, id: &SessionId<M>) -> Option<&Session<M>> {
        self.sessions.get(id)
    }

    pub fn require_session(&self, id: &SessionId<M>) -> Result<&Session<M>> {
        self.sessions.get(id).ok_or(Error::MissingSession)
    }

    pub fn session_active(&self, id: &SessionId<M>) -> bool {
        self.sessions.contains(id) && !self.ended_sessions.contains(id)
    }

    pub fn actor(&self, id: &ActorId<M>) -> Option<&Actor<M>> {
        self.actors.get(id)
    }

    pub fn require_actor(&self, id: &ActorId<M>) -> Result<&Actor<M>> {
        self.actors.get(id).ok_or(Error::MissingActor)
    }

    pub fn event(&self, id: &EventId<M>) -> Option<&Event<M>> {
        self.events.get(id)
    }

    pub fn require_event(&self, id: &EventId<M>) -> Result<&Event<M>> {
        self.events.get(id).ok_or(Error::MissingEvent)
    }

    pub fn entity(&self, id: &EntityAddress<M>) -> Option<&EntityObservation<M>> {
        self.entities.get(id)
    }

    pub fn effective_retention(&self, resource: &Resource<M>) -> Option<RetentionStrength> {
        self.required_retention.get(resource).copied()
    }

    pub fn observed_retention(&self, resource: &Resource<M>) -> Option<RetentionStrength> {
        self.resource_observations
            .get(resource)
            .and_then(|observation| observation.observed_retention)
    }

    pub fn retention_satisfied(&self, resource: &Resource<M>) -> bool {
        match (
            self.effective_retention(resource),
            self.observed_retention(resource),
        ) {
            (None, _) => true,
            (Some(required), Some(observed)) => observed >= required,
            (Some(_), None) => false,
        }
    }

    pub(crate) fn recompute_retention_for(
        &mut self,
        resource: &Resource<M>,
    ) -> Option<RetentionStrength> {
        let strength = self
            .active_retention
            .iter()
            .filter(|(_, claim)| &claim.resource == resource)
            .map(|(_, claim)| claim.strength)
            .max();
        match strength {
            Some(strength) => {
                self.required_retention.set(resource.clone(), strength);
            }

            None => {
                self.required_retention.remove(resource);
            }
        }

        strength
    }
}

// =============================================================================

// STATE
// =============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct State<M: Model> {
    /// AUTHORITATIVE.
    pub operations: Ledger<OperationId<M>, Operation<M>>,
    /// DERIVED.
    pub operation_heads: Set<OperationId<M>>,
    /// DERIVED.
    pub projection: Projection<M>,
}

impl<M: Model> Default for State<M> {
    fn default() -> Self {
        Self {
            operations: Ledger::default(),
            operation_heads: Set::default(),
            projection: Projection::default(),
        }
    }
}

impl<M: Model> State<M> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn operation(&self, id: &OperationId<M>) -> Option<&Operation<M>> {
        self.operations.get(id)
    }
    pub fn operation_for_source(&self, source: &EntityAddress<M>) -> Option<&OperationId<M>> {
        self.projection.source_to_operation.get(source)
    }
    pub fn has_source(&self, source: &EntityAddress<M>) -> bool {
        self.operation_for_source(source).is_some()
    }
    pub fn rebuild(&mut self) -> Result<()> {
        let mut projection = Projection::default();
        let ordered = topological_operations(&self.operations)?;
        for operation in ordered {
            apply_operation(&mut projection, operation)?;
        }

        let operation_heads = compute_operation_heads(&self.operations);
        self.projection = projection;
        self.operation_heads = operation_heads;
        Ok(())
    }
}

// =============================================================================

// SCHEMA VALIDATION
// =============================================================================

pub(crate) fn matches_entity_type<M: Model>(
    entity: &EntityRef<M>,
    expected: &EntityTypePattern,
) -> bool {
    match expected {
        EntityTypePattern::Any => true,
        EntityTypePattern::External(expected) => {
            matches!(
                entity,
                EntityRef::External(address)
                    if &address.entity_type() == expected
            )
        }
        EntityTypePattern::Internal(expected) => {
            matches!(
                entity.type_pattern(),
                EntityTypePattern::Internal(actual)
                    if &actual == expected
            )
        }
    }
}

fn value_matches<M: Model>(value: &Value<M>, expected: &ValueType) -> bool {
    match (value, expected) {
        (_, ValueType::Any) => true,
        (Value::Null, ValueType::Null) => true,
        (Value::Bool(_), ValueType::Bool) => true,
        (Value::Integer(_), ValueType::Integer) => true,
        (Value::Decimal(_), ValueType::Decimal) => true,
        (Value::String(_), ValueType::String) => true,
        (Value::Bytes(_), ValueType::Bytes) => true,
        (Value::Entity(entity), ValueType::Entity(expected)) => {
            matches_entity_type(entity, expected)
        }
        (Value::List(values), ValueType::List(inner)) => {
            values.iter().all(|value| value_matches(value, inner))
        }
        (
            Value::Map(values),
            ValueType::Map {
                fields,
                allow_unknown,
            },
        ) => validate_attribute_map(values, fields, *allow_unknown).is_ok(),
        _ => false,
    }
}

fn validate_attribute_map<M: Model>(
    values: &Attributes<M>,
    fields: &BTreeMap<FieldName, FieldSchema>,
    allow_unknown: bool,
) -> Result<()> {
    for (field, schema) in fields {
        if schema.required && !values.contains_key(field) {
            return Err(Error::MissingRequiredField);
        }
    }

    for (field, value) in values {
        let Some(schema) = fields.get(field) else {
            if allow_unknown {
                continue;
            }

            return Err(Error::UnknownField);
        };
        if !value_matches(value, &schema.value_type) {
            return Err(Error::InvalidFieldType);
        }
    }

    Ok(())
}

pub(crate) fn find_entity_schema<'a, M: Model>(
    projection: &'a Projection<M>,
    schema_key: &SchemaKey,
    entity_type: &EntityType,
) -> Result<&'a EntitySchema> {
    let schema = projection.require_schema(schema_key)?;
    schema
        .entities
        .get(&entity_type.kind)
        .filter(|definition| &definition.entity_type == entity_type)
        .ok_or(Error::MissingEntitySchema)
}

pub(crate) fn find_relation_schema<'a, M: Model>(
    projection: &'a Projection<M>,
    relation: &Relation<M>,
) -> Result<&'a RelationSchema> {
    let schema = projection.require_schema(&relation.schema)?;
    schema
        .relations
        .get(&relation.relation_type.name)
        .filter(|definition| definition.relation_type == relation.relation_type)
        .ok_or(Error::MissingRelationSchema)
}

pub(crate) fn validate_entity_observation_schema<M: Model>(
    projection: &Projection<M>,
    observation: &EntityObservation<M>,
) -> Result<()> {
    let entity_type = observation.entity.entity_type();
    let schema = find_entity_schema(projection, &observation.schema, &entity_type)?;
    validate_attribute_map(
        &observation.attributes,
        &schema.fields,
        schema.allow_unknown_fields,
    )
}

pub(crate) fn validate_relation<M: Model>(
    projection: &Projection<M>,
    relation: &Relation<M>,
) -> Result<()> {
    if relation.from == relation.to {
        return Err(Error::InvalidSelfRelation);
    }

    let schema = find_relation_schema(projection, relation)?;
    if !matches_entity_type(&relation.from, &schema.from) {
        return Err(Error::InvalidRelationEndpoint);
    }

    if !matches_entity_type(&relation.to, &schema.to) {
        return Err(Error::InvalidRelationEndpoint);
    }

    Ok(())
}

// =============================================================================

// APPLY AUTHORITATIVE OPERATIONS
// =============================================================================

fn apply_operation<M: Model>(
    projection: &mut Projection<M>,
    operation: &Operation<M>,
) -> Result<()> {
    for fact in &operation.facts {
        apply_fact(projection, &operation.id, fact)?;
    }

    if let Some(source) = &operation.source {
        match projection.source_to_operation.get(source) {
            None => {
                projection
                    .source_to_operation
                    .set(source.clone(), operation.id.clone());
            }

            Some(existing) if existing == &operation.id => {}

            Some(_) => {
                return Err(Error::SourceAlreadyMappedDifferently);
            }
        }

        if let Some(event) = operation.facts.iter().find_map(|fact| match fact {
            Fact::EventRecorded(event) => Some(event.id.clone()),
            _ => None,
        }) {
            projection
                .source_to_primary_event
                .set(source.clone(), event);
        }
    }

    Ok(())
}

pub(crate) fn apply_fact<M: Model>(
    projection: &mut Projection<M>,
    operation_id: &OperationId<M>,
    fact: &Fact<M>,
) -> Result<()> {
    match fact {
        Fact::SchemaRegistered(schema) => {
            projection
                .schemas
                .declare(schema.key.clone(), schema.clone())?;
        }
        Fact::NamedQueryRegistered(query) => {
            projection
                .named_queries
                .declare(query.key.clone(), query.clone())?;
        }
        Fact::SourceAnchored(anchor) => match projection.source_to_operation.get(&anchor.source) {
            None => {
                projection
                    .source_to_operation
                    .set(anchor.source.clone(), anchor.operation.clone());
            }

            Some(existing) if existing == &anchor.operation => {}

            Some(_) => {
                return Err(Error::SourceAlreadyMappedDifferently);
            }
        },
        Fact::SessionOpened(session) => {
            projection
                .sessions
                .declare(session.id.clone(), session.clone())?;
        }
        Fact::SessionEnded { session } => {
            projection.require_session(session)?;
            projection.ended_sessions.insert(session.clone());
        }
        Fact::ActorDeclared(actor) => {
            projection.actors.declare(actor.id.clone(), actor.clone())?;
        }
        Fact::ObjectDeclared(object) => {
            projection
                .objects
                .declare(object.id.clone(), object.clone())?;
        }
        Fact::EntityObserved(observation) => {
            validate_entity_observation_schema(projection, observation)?;
            projection
                .entities
                .set(observation.entity.clone(), observation.clone());
            projection
                .entity_history
                .insert(observation.entity.clone(), operation_id.clone());
        }
        Fact::EventRecorded(event) => {
            match projection.events.declare(event.id.clone(), event.clone())? {
                DeclareOutcome::Inserted => {
                    for parent in &event.parents {
                        projection.event_heads.remove(parent);
                    }
                    projection.event_heads.insert(event.id.clone());
                    for relation in &event.relations {
                        validate_relation(projection, relation)?;
                        let edge = GraphEdge {
                            relation: relation.clone(),
                            event: event.id.clone(),
                            operation: operation_id.clone(),
                        };
                        projection
                            .outgoing
                            .insert(relation.from.clone(), edge.clone());
                        projection.incoming.insert(relation.to.clone(), edge);
                    }
                }

                DeclareOutcome::AlreadyPresent => {}
            }
        }

        Fact::ReplicaDeclared(replica) => {
            projection
                .replicas
                .declare(replica.id.clone(), replica.clone())?;
        }

        Fact::RetentionClaimed(claim) => {
            match projection.active_retention.get(&claim.id) {
                None => {
                    projection
                        .active_retention
                        .set(claim.id.clone(), claim.clone());
                }

                Some(existing) if existing == claim => {}

                Some(_) => {
                    return Err(Error::IdentityCollision);
                }
            }

            projection.recompute_retention_for(&claim.resource);
        }

        Fact::RetentionReleased { claim } => {
            if let Some(existing) = projection.active_retention.remove(claim) {
                projection.recompute_retention_for(&existing.resource);
            }
        }

        Fact::ResourceObserved {
            resource,
            observation,
        } => {
            projection
                .resource_observations
                .set(resource.clone(), observation.clone());
        }
    }

    Ok(())
}

// =============================================================================
// OPERATION DAG
// =============================================================================

fn topological_operations<M: Model>(
    operations: &Ledger<OperationId<M>, Operation<M>>,
) -> Result<Vec<&Operation<M>>> {
    let mut indegree = BTreeMap::<OperationId<M>, usize>::new();
    let mut children = BTreeMap::<OperationId<M>, BTreeSet<OperationId<M>>>::new();
    for id in operations.keys() {
        indegree.insert(id.clone(), 0);
    }

    for operation in operations.values() {
        for parent in &operation.parents {
            if !operations.contains(parent) {
                return Err(Error::MissingOperationParent);
            }

            *indegree.get_mut(&operation.id).unwrap() += 1;
            children
                .entry(parent.clone())
                .or_default()
                .insert(operation.id.clone());
        }
    }

    let mut queue = VecDeque::new();
    for (id, degree) in &indegree {
        if *degree == 0 {
            queue.push_back(id.clone());
        }
    }

    let mut ordered = Vec::new();
    while let Some(id) = queue.pop_front() {
        ordered.push(id.clone());
        if let Some(children) = children.get(&id) {
            for child in children {
                let degree = indegree.get_mut(child).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    if ordered.len() != operations.len() {
        return Err(Error::OperationCycle);
    }

    Ok(ordered
        .iter()
        .map(|id| operations.get(id).unwrap())
        .collect())
}

fn compute_operation_heads<M: Model>(
    operations: &Ledger<OperationId<M>, Operation<M>>,
) -> Set<OperationId<M>> {
    let mut heads = Set::default();
    for id in operations.keys() {
        heads.insert(id.clone());
    }

    for operation in operations.values() {
        for parent in &operation.parents {
            heads.remove(parent);
        }
    }

    heads
}

// =============================================================================
