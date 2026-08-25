//! Transaction intents, policy hooks, commit planning, and kernel execution.

use crate::query::{NamedQueryDefinition, Predicate, Query, QueryKey, QueryTemplate};
use crate::retention::*;
use crate::state::*;
use crate::{Error, Result};
use provenance_data_model::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::marker::PhantomData;

// TRANSACTION INTENTS
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct EventIntent<M: Model> {
    pub session: Option<SessionId<M>>,
    pub actor: Option<ActorId<M>>,
    /// Source causal parents are inherited automatically.
    pub additional_parents: BTreeSet<EventId<M>>,
    pub subjects: BTreeSet<EntityRef<M>>,
    pub relations: BTreeSet<Relation<M>>,
    pub requires: BTreeSet<ResourceRequirement<M>>,
    pub attributes: Attributes<M>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum Intent<M: Model> {
    RegisterSchema(SchemaDefinition),
    RegisterNamedQuery(NamedQueryDefinition),
    OpenSession {
        seed: M::Seed,
        parent: Option<SessionId<M>>,
        attributes: Attributes<M>,
    },
    EndSession {
        session: SessionId<M>,
    },
    DeclareActor {
        seed: M::Seed,
        session: Option<SessionId<M>>,
        attributes: Attributes<M>,
    },
    PutObject {
        seed: M::Seed,
        payload: M::Payload,
        attributes: Attributes<M>,
    },
    ObserveEntity(EntityObservation<M>),
    RecordEvent(EventIntent<M>),
    ReleaseRetention {
        claim: ClaimId<M>,
    },
    DeclareReplica {
        seed: M::Seed,
        attributes: Attributes<M>,
    },
    ObserveResource {
        resource: Resource<M>,
        observation: ResourceObservation<M>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct Transaction<M: Model> {
    pub seed: M::Seed,
    pub source: Option<SourceOperation<M>>,
    pub intents: Vec<Intent<M>>,
    pub attributes: Attributes<M>,
}

// =============================================================================
// POLICY EXTENSION
// =============================================================================

pub trait Rules<M: Model>: Send + Sync {
    fn validate_schema(&self, _state: &State<M>, _schema: &SchemaDefinition) -> Result<()> {
        Ok(())
    }
    fn validate_entity_observation(
        &self,
        _state: &State<M>,
        _observation: &EntityObservation<M>,
    ) -> Result<()> {
        Ok(())
    }
    fn validate_event(&self, _state: &State<M>, _event: &Event<M>) -> Result<()> {
        Ok(())
    }
    fn validate_operation(&self, _state: &State<M>, _operation: &Operation<M>) -> Result<()> {
        Ok(())
    }
    fn require_source_parents(&self) -> bool {
        true
    }
    fn retain_event_requirements(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct DefaultRules;
impl<M: Model> Rules<M> for DefaultRules {}

// =============================================================================
// COMMIT PROTOCOL
// =============================================================================

/// Must become true BEFORE operation publication.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum PrepareRequirement<M: Model> {
    MaterializeObject(Object<M>),
    Retention(RetentionTransition<M>),
}

/// Safe to perform AFTER authoritative publication.
///
/// A crash here does not invalidate provenance.
/// Runtime can reconcile later.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub enum FinalizeAction<M: Model> {
    Retention(RetentionTransition<M>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "M::Id: Serialize, M::Seed: Serialize, M::ExternalId: Serialize, M::Payload: Serialize",
    deserialize = "M::Id: Deserialize<'de>, M::Seed: Deserialize<'de>, M::ExternalId: Deserialize<'de>, M::Payload: Deserialize<'de>",
))]
pub struct CommitPlan<M: Model> {
    pub prepare: Vec<PrepareRequirement<M>>,
    pub operation: Operation<M>,
    pub next_state: State<M>,
    pub finalize: Vec<FinalizeAction<M>>,
    pub idempotent: bool,
}

// =============================================================================

// KERNEL
// =============================================================================

pub struct Kernel<M, I, R = DefaultRules>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
{
    ids: I,
    rules: R,
    _model: PhantomData<fn() -> M>,
}

struct SourceResolution<M: Model>(
    Option<EntityAddress<M>>,
    BTreeSet<OperationId<M>>,
    BTreeSet<EventId<M>>,
);

impl<M, I> Kernel<M, I, DefaultRules>
where
    M: Model,
    I: IdentityScheme<M>,
{
    pub fn new(ids: I) -> Self {
        Self {
            ids,
            rules: DefaultRules,
            _model: PhantomData,
        }
    }
}

impl<M, I, R> Kernel<M, I, R>
where
    M: Model,
    I: IdentityScheme<M>,
    R: Rules<M>,
{
    pub fn with_rules(ids: I, rules: R) -> Self {
        Self {
            ids,
            rules,
            _model: PhantomData,
        }
    }

    // =========================================================================
    // TRANSACTION
    // =========================================================================

    pub fn transact(&self, state: &State<M>, tx: Transaction<M>) -> Result<CommitPlan<M>> {
        let operation_id = self.ids.operation(&tx.seed);
        let SourceResolution(source, operation_parents, inherited_event_parents) =
            self.resolve_source(state, &tx, &operation_id)?; // ---------------------------------------------------------------------
        // Source replay fast path.
        // ---------------------------------------------------------------------

        if let Some(source) = &source
            && let Some(existing_id) = state.projection.source_to_operation.get(source)
        {
            let existing = state
                .operations
                .get(existing_id)
                .ok_or(Error::MissingOperationParent)?;
            if existing.id == operation_id {
                return Ok(CommitPlan {
                    prepare: Vec::new(),
                    operation: existing.clone(),
                    next_state: state.clone(),
                    finalize: Vec::new(),
                    idempotent: true,
                });
            }

            return Err(Error::SourceAlreadyMappedDifferently);
        }

        let mut projection = state.projection.clone();
        let mut facts = Vec::new();
        let mut prepare = Vec::new();
        let mut finalize = Vec::new();
        if let Some(source) = source.clone() {
            let fact = Fact::SourceAnchored(SourceAnchor {
                source,
                operation: operation_id.clone(),
            });
            apply_fact(&mut projection, &operation_id, &fact)?;
            facts.push(fact);
        }

        let mut event_index = 0usize;
        let mut object_index = 0usize;
        let mut primary_event = None;
        for intent in tx.intents {
            match intent {
                // =============================================================
                // SCHEMA
                // =============================================================
                Intent::RegisterSchema(schema) => {
                    for dependency in &schema.requires {
                        if !projection.schemas.contains(dependency) {
                            return Err(Error::MissingSchemaDependency);
                        }
                    }

                    self.rules.validate_schema(state, &schema)?;
                    match projection
                        .schemas
                        .declare(schema.key.clone(), schema.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::SchemaRegistered(schema));
                }

                // =============================================================
                // NAMED QUERY
                // =============================================================
                Intent::RegisterNamedQuery(query) => {
                    // Namespace/schema ownership enforcement can be stricter
                    // in plugin host policy.

                    match projection
                        .named_queries
                        .declare(query.key.clone(), query.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::NamedQueryRegistered(query));
                }

                // =============================================================
                // SESSION
                // =============================================================
                Intent::OpenSession {
                    seed,
                    parent,
                    attributes,
                } => {
                    if let Some(parent) = &parent {
                        projection.require_session(parent)?;
                    }

                    let session = Session {
                        id: self.ids.session(&seed),
                        parent,
                        attributes,
                    };
                    match projection
                        .sessions
                        .declare(session.id.clone(), session.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::SessionOpened(session));
                }

                Intent::EndSession { session } => {
                    projection.require_session(&session)?;
                    if projection.ended_sessions.contains(&session) {
                        continue;
                    }

                    projection.ended_sessions.insert(session.clone());
                    facts.push(Fact::SessionEnded { session });
                }

                // =============================================================
                // ACTOR
                // =============================================================
                Intent::DeclareActor {
                    seed,
                    session,
                    attributes,
                } => {
                    if let Some(session) = &session {
                        projection.require_session(session)?;
                        if !projection.session_active(session) {
                            return Err(Error::SessionAlreadyEnded);
                        }
                    }

                    let actor = Actor {
                        id: self.ids.actor(&seed),
                        session,
                        attributes,
                    };
                    match projection.actors.declare(actor.id.clone(), actor.clone())? {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::ActorDeclared(actor));
                }

                // =============================================================
                // OBJECT
                // =============================================================
                Intent::PutObject {
                    seed,
                    payload,
                    attributes,
                } => {
                    let object = Object {
                        id: self.ids.object(&seed, object_index),
                        payload,
                        attributes,
                    };
                    object_index += 1;
                    match projection
                        .objects
                        .declare(object.id.clone(), object.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    prepare.push(PrepareRequirement::MaterializeObject(object.clone()));
                    facts.push(Fact::ObjectDeclared(object));
                }

                // =============================================================
                // EXTERNAL TYPED ENTITY
                // =============================================================
                Intent::ObserveEntity(observation) => {
                    validate_entity_observation_schema(&projection, &observation)?;
                    self.rules
                        .validate_entity_observation(state, &observation)?;
                    projection
                        .entities
                        .set(observation.entity.clone(), observation.clone());
                    projection
                        .entity_history
                        .insert(observation.entity.clone(), operation_id.clone());
                    facts.push(Fact::EntityObserved(observation));
                }

                // =============================================================
                // EVENT
                // =============================================================
                Intent::RecordEvent(intent) => {
                    let event_id = self.ids.event(&tx.seed, event_index);
                    let mut parents = inherited_event_parents.clone();
                    parents.extend(intent.additional_parents.iter().cloned());
                    for parent in &parents {
                        projection.require_event(parent)?;
                    }

                    if let Some(session) = &intent.session {
                        projection.require_session(session)?;
                        if !projection.session_active(session) {
                            return Err(Error::SessionAlreadyEnded);
                        }
                    }

                    if let Some(actor_id) = &intent.actor {
                        let actor = projection.require_actor(actor_id)?;
                        if let (Some(event_session), Some(actor_session)) =
                            (&intent.session, &actor.session)
                            && event_session != actor_session
                        {
                            return Err(Error::ActorSessionMismatch);
                        }
                    }

                    for relation in &intent.relations {
                        validate_relation(&projection, relation)?;
                    }

                    let event = Event {
                        id: event_id.clone(),
                        session: intent.session,
                        actor: intent.actor,
                        parents,
                        subjects: intent.subjects,
                        relations: intent.relations,
                        requires: intent.requires,
                        attributes: intent.attributes,
                    };
                    self.rules.validate_event(state, &event)?;
                    match projection.events.declare(event.id.clone(), event.clone())? {
                        DeclareOutcome::AlreadyPresent => {
                            event_index += 1;
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    for parent in &event.parents {
                        projection.event_heads.remove(parent);
                    }

                    projection.event_heads.insert(event.id.clone());
                    for relation in &event.relations {
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

                    facts.push(Fact::EventRecorded(event.clone()));
                    if primary_event.is_none() {
                        primary_event = Some(event.id.clone());
                    }

                    // ---------------------------------------------------------
                    // AUTOMATIC AGGREGATE RETENTION
                    // ---------------------------------------------------------

                    if self.rules.retain_event_requirements() {
                        for (requirement_index, requirement) in
                            event.requires.iter().cloned().enumerate()
                        {
                            let before = projection.effective_retention(&requirement.resource);
                            let claim = RetentionClaim {
                                id: self.ids.claim(&tx.seed, event_index, requirement_index),
                                owner: RetentionOwner::Event(event.id.clone()),
                                resource: requirement.resource.clone(),
                                strength: requirement.strength,
                            };
                            if let Some(existing) = projection.active_retention.get(&claim.id) {
                                if existing != &claim {
                                    return Err(Error::IdentityCollision);
                                }

                                continue;
                            }

                            projection
                                .active_retention
                                .set(claim.id.clone(), claim.clone());
                            let after = projection.recompute_retention_for(&claim.resource);
                            if let Some(transition) =
                                retention_transition(claim.resource.clone(), before, after)
                            {
                                if retention_is_prepare(&transition) {
                                    prepare.push(PrepareRequirement::Retention(transition));
                                } else {
                                    finalize.push(FinalizeAction::Retention(transition));
                                }
                            }

                            facts.push(Fact::RetentionClaimed(claim));
                        }
                    }

                    event_index += 1;
                }

                // =============================================================
                // RETENTION RELEASE
                // =============================================================
                Intent::ReleaseRetention { claim } => {
                    let existing = projection
                        .active_retention
                        .get(&claim)
                        .cloned()
                        .ok_or(Error::UnknownRetentionClaim)?;
                    let before = projection.effective_retention(&existing.resource);
                    projection.active_retention.remove(&claim);
                    let after = projection.recompute_retention_for(&existing.resource);
                    if let Some(transition) = retention_transition(existing.resource, before, after)
                    {
                        if retention_is_prepare(&transition) {
                            prepare.push(PrepareRequirement::Retention(transition));
                        } else {
                            finalize.push(FinalizeAction::Retention(transition));
                        }
                    }

                    facts.push(Fact::RetentionReleased { claim });
                }

                // =============================================================
                // REPLICA
                // =============================================================
                Intent::DeclareReplica { seed, attributes } => {
                    let replica = Replica {
                        id: self.ids.replica(&seed),
                        attributes,
                    };
                    match projection
                        .replicas
                        .declare(replica.id.clone(), replica.clone())?
                    {
                        DeclareOutcome::AlreadyPresent => {
                            continue;
                        }

                        DeclareOutcome::Inserted => {}
                    }

                    facts.push(Fact::ReplicaDeclared(replica));
                }

                // =============================================================
                // RESOURCE OBSERVATION
                // =============================================================
                Intent::ObserveResource {
                    resource,
                    observation,
                } => {
                    if projection.resource_observations.get(&resource) == Some(&observation) {
                        continue;
                    }

                    projection
                        .resource_observations
                        .set(resource.clone(), observation.clone());
                    facts.push(Fact::ResourceObserved {
                        resource,
                        observation,
                    });
                }
            }
        }

        if let (Some(source), Some(event)) = (source.clone(), primary_event) {
            projection.source_to_primary_event.set(source, event);
        }

        let operation = Operation {
            id: operation_id.clone(),
            parents: operation_parents,
            source,
            facts,
            attributes: tx.attributes,
        };
        self.rules.validate_operation(state, &operation)?;
        if let Some(existing) = state.operations.get(&operation.id) {
            if existing == &operation {
                return Ok(CommitPlan {
                    prepare: Vec::new(),
                    operation: existing.clone(),
                    next_state: state.clone(),
                    finalize: Vec::new(),
                    idempotent: true,
                });
            }

            return Err(Error::IdentityCollision);
        }

        let mut next = state.clone();
        for parent in &operation.parents {
            next.operation_heads.remove(parent);
        }

        next.operation_heads.insert(operation.id.clone());
        next.operations
            .declare(operation.id.clone(), operation.clone())?;
        next.projection = projection;
        Ok(CommitPlan {
            prepare,
            operation,
            next_state: next,
            finalize,
            idempotent: false,
        })
    }

    // =========================================================================
    // SOURCE CAUSALITY
    // =========================================================================

    fn resolve_source(
        &self,
        state: &State<M>,
        tx: &Transaction<M>,
        operation_id: &OperationId<M>,
    ) -> Result<SourceResolution<M>> {
        let Some(source) = &tx.source else {
            return Ok(SourceResolution(
                None,
                state.operation_heads.cloned(),
                state.projection.event_heads.cloned(),
            ));
        };
        if let Some(existing) = state.projection.source_to_operation.get(&source.id)
            && existing != operation_id
        {
            return Err(Error::SourceAlreadyMappedDifferently);
        }

        let mut operation_parents = BTreeSet::new();
        let mut event_parents = BTreeSet::new();
        for source_parent in &source.parents {
            match state.projection.source_to_operation.get(source_parent) {
                Some(parent) => {
                    operation_parents.insert(parent.clone());
                }
                None if self.rules.require_source_parents() => {
                    return Err(Error::MissingSourceParent);
                }
                None => {}
            }

            if let Some(event) = state.projection.source_to_primary_event.get(source_parent) {
                event_parents.insert(event.clone());
            }
        }

        Ok(SourceResolution(
            Some(source.id.clone()),
            operation_parents,
            event_parents,
        ))
    }

    // =========================================================================
    // DISTRIBUTED MERGE
    // =========================================================================

    pub fn merge(&self, left: &State<M>, right: &State<M>) -> Result<State<M>> {
        let mut merged = left.clone();
        for (id, operation) in right.operations.iter() {
            merged.operations.declare(id.clone(), operation.clone())?;
        }

        merged.rebuild()?;
        Ok(merged)
    }

    // =========================================================================
    // QUERY VALIDATION / NAMED QUERY EXPANSION
    // =========================================================================

    pub fn instantiate_named_query(
        &self,
        state: &State<M>,
        key: &QueryKey,
        input: EntityRef<M>,
    ) -> Result<Query<M>> {
        let definition = state
            .projection
            .named_queries
            .get(key)
            .ok_or(Error::InvalidNamedQuery)?;
        if !matches_entity_type(&input, &definition.input) {
            return Err(Error::InvalidNamedQuery);
        }

        Ok(match &definition.template {
            QueryTemplate::History => Query::History { entity: input },
            QueryTemplate::Explain { max_depth, roles } => Query::Explain {
                root: input,
                max_depth: *max_depth,
                roles: roles.clone(),
            },
            QueryTemplate::Traverse {
                direction,
                relations,
                max_depth,
            } => Query::Traverse {
                roots: BTreeSet::from([input]),
                direction: *direction,
                relations: relations.clone(),
                max_depth: *max_depth,
                predicate: Predicate::Any,
            },
        })
    }
}

// =============================================================================
