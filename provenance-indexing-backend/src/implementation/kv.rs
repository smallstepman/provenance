//! Shared projection row/index logic for ordered key-value backends.
//!
//! The concrete backends only provide an atomic key/value store. This module
//! owns the portable projection row layout and keeps query semantics in the
//! generic [`ProjectionIndex`] layer.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};

use postcard::Error as PostcardError;
use provenance_core::{
    EntityObservation, EntityRef, EventId, Fact, GraphEdge, Model, Operation, OperationId,
    SchemaDefinition, SchemaKey, State,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{HistoryRows, IndexModel, ProjectionStorage};

type Entry = (Vec<u8>, Vec<u8>);

const OPERATION_TAG: u8 = b'o';
const SOURCE_TAG: u8 = b's';
const SCHEMA_TAG: u8 = b'h';
const NAMED_QUERY_TAG: u8 = b'n';
const ENTITY_TAG: u8 = b'e';
const ENTITY_HISTORY_TAG: u8 = b'H';
const EVENT_TAG: u8 = b'v';
const EVENT_ENTITY_TAG: u8 = b'x';
const GRAPH_OUTGOING_TAG: u8 = b'g';
const GRAPH_INCOMING_TAG: u8 = b'G';

/// Errors shared by the row/index layer and one concrete key/value backend.
#[derive(Debug, Error)]
pub enum KvStorageError<E>
where
    E: Debug + Display,
{
    #[error("key/value projection storage error: {0}")]
    Backend(E),
    #[error("could not encode indexed value: {0}")]
    Encode(#[source] PostcardError),
    #[error("could not decode indexed value: {0}")]
    Decode(#[source] PostcardError),
    #[error("invalid indexed provenance: {0}")]
    InvalidProjection(String),
}

/// Minimal atomic persistence surface needed by the projection row index.
pub trait KvBackend {
    type Error: Debug + Display;

    fn replace_entries(&mut self, entries: &[Entry]) -> Result<(), Self::Error>;
    fn apply_entries(&mut self, entries: &[Entry]) -> Result<(), Self::Error>;
    fn clear(&mut self) -> Result<(), Self::Error>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;
    fn entries(&self) -> Result<Vec<Entry>, Self::Error>;
}

/// Generic projection storage over a concrete ordered key/value backend.
pub struct KvProjectionStorage<B> {
    backend: B,
}

impl<B> KvProjectionStorage<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    pub fn into_backend(self) -> B {
        self.backend
    }
}

impl<B, M> ProjectionStorage<M> for KvProjectionStorage<B>
where
    B: KvBackend,
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = KvStorageError<B::Error>;

    fn replace(&mut self, state: &State<M>) -> Result<(), Self::Error> {
        let entries = state_entries::<M, B::Error>(state)?;
        self.backend
            .replace_entries(&entries)
            .map_err(KvStorageError::Backend)
    }

    fn apply_delta(
        &mut self,
        operations: &[Operation<M>],
        _state: &State<M>,
    ) -> Result<(), Self::Error> {
        if operations.is_empty() {
            return Ok(());
        }

        let mut entries = Vec::new();
        let mut seen_events = BTreeSet::new();
        for operation in operations {
            let mut skipped_events = BTreeSet::new();
            for fact in &operation.facts {
                let Fact::EventRecorded(event) = fact else {
                    continue;
                };
                let event_id = encoded::<B::Error, _>(&event.id)?;
                let duplicate = !seen_events.insert(event_id.clone());
                let indexed = !duplicate
                    && self
                        .backend
                        .get(&make_key(EVENT_TAG, std::slice::from_ref(&event_id)))
                        .map_err(KvStorageError::Backend)?
                        .is_some();
                if duplicate || indexed {
                    skipped_events.insert(event_id);
                }
            }
            entries.extend(operation_entries::<M, B::Error>(
                operation,
                &skipped_events,
            )?);
        }
        self.backend
            .apply_entries(&entries)
            .map_err(KvStorageError::Backend)
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.backend.clear().map_err(KvStorageError::Backend)
    }

    fn operation_count(&self) -> Result<usize, Self::Error> {
        Ok(self.entries_with_prefix(&[OPERATION_TAG])?.len())
    }

    fn indexed_operations(&self) -> Result<Vec<Operation<M>>, Self::Error> {
        self.entries_with_prefix(&[OPERATION_TAG])?
            .into_iter()
            .map(|(_, value)| decode::<B::Error, Operation<M>>(&value))
            .collect()
    }

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error> {
        let id = encoded::<B::Error, _>(operation_id)?;
        Ok(self
            .backend
            .get(&make_key(OPERATION_TAG, &[id]))
            .map_err(KvStorageError::Backend)?
            .is_some())
    }

    fn entity_observation(
        &self,
        entity: &EntityRef<M>,
    ) -> Result<Option<EntityObservation<M>>, Self::Error> {
        let entity = encoded::<B::Error, _>(entity)?;
        self.backend
            .get(&make_key(ENTITY_TAG, &[entity]))
            .map_err(KvStorageError::Backend)?
            .map(|value| decode::<B::Error, EntityObservation<M>>(&value))
            .transpose()
    }

    fn schema(&self, key: &SchemaKey) -> Result<Option<SchemaDefinition>, Self::Error> {
        let key = encoded::<B::Error, _>(key)?;
        self.backend
            .get(&make_key(SCHEMA_TAG, &[key]))
            .map_err(KvStorageError::Backend)?
            .map(|value| decode::<B::Error, SchemaDefinition>(&value))
            .transpose()
    }

    fn history(&self, entity: &EntityRef<M>) -> Result<HistoryRows<M>, Self::Error> {
        let entity = encoded::<B::Error, _>(entity)?;
        let prefix = make_prefix(ENTITY_HISTORY_TAG, &entity);
        let event_prefix = make_prefix(EVENT_ENTITY_TAG, &entity);
        let mut history = HistoryRows::default();

        for (_, value) in self.entries_with_prefix(&prefix)? {
            history
                .operations
                .insert(decode::<B::Error, OperationId<M>>(&value)?);
        }
        for (_, value) in self.entries_with_prefix(&event_prefix)? {
            let (event_id, operation_id) =
                decode::<B::Error, (EventId<M>, OperationId<M>)>(&value)?;
            history.events.insert(event_id);
            history.operations.insert(operation_id);
        }

        Ok(history)
    }

    fn edges(
        &self,
        entity: &EntityRef<M>,
        direction: provenance_core::Direction,
    ) -> Result<Vec<GraphEdge<M>>, Self::Error> {
        let entity = encoded::<B::Error, _>(entity)?;
        let outgoing_prefix = make_prefix(GRAPH_OUTGOING_TAG, &entity);
        let incoming_prefix = make_prefix(GRAPH_INCOMING_TAG, &entity);
        let mut edges = BTreeSet::new();

        if matches!(
            direction,
            provenance_core::Direction::Outgoing | provenance_core::Direction::Both
        ) {
            for (_, value) in self.entries_with_prefix(&outgoing_prefix)? {
                edges.insert(decode::<B::Error, GraphEdge<M>>(&value)?);
            }
        }
        if matches!(
            direction,
            provenance_core::Direction::Incoming | provenance_core::Direction::Both
        ) {
            for (_, value) in self.entries_with_prefix(&incoming_prefix)? {
                edges.insert(decode::<B::Error, GraphEdge<M>>(&value)?);
            }
        }

        Ok(edges.into_iter().collect())
    }
}

impl<B> KvProjectionStorage<B>
where
    B: KvBackend,
{
    fn entries_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Entry>, KvStorageError<B::Error>> {
        Ok(self
            .backend
            .entries()
            .map_err(KvStorageError::Backend)?
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .collect())
    }
}

fn state_entries<M, E>(state: &State<M>) -> Result<Vec<Entry>, KvStorageError<E>>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
    E: Debug + Display,
{
    let mut entries = Vec::new();

    for (_, operation) in state.operations.iter() {
        let operation_id = encoded::<E, _>(&operation.id)?;
        add_entry(
            &mut entries,
            make_key(OPERATION_TAG, std::slice::from_ref(&operation_id)),
            operation,
        )?;
    }

    for (source, operation_id) in state.projection.source_to_operation.iter() {
        let source = encoded::<E, _>(source)?;
        add_entry(
            &mut entries,
            make_key(SOURCE_TAG, std::slice::from_ref(&source)),
            operation_id,
        )?;
    }
    for (schema_key, schema) in state.projection.schemas.iter() {
        let schema_key = encoded::<E, _>(schema_key)?;
        add_entry(
            &mut entries,
            make_key(SCHEMA_TAG, std::slice::from_ref(&schema_key)),
            schema,
        )?;
    }
    for (query_key, query) in state.projection.named_queries.iter() {
        let query_key = encoded::<E, _>(query_key)?;
        add_entry(
            &mut entries,
            make_key(NAMED_QUERY_TAG, std::slice::from_ref(&query_key)),
            query,
        )?;
    }
    for (entity, observation) in state.projection.entities.iter() {
        let entity = EntityRef::External(entity.clone());
        let entity = encoded::<E, _>(&entity)?;
        add_entry(
            &mut entries,
            make_key(ENTITY_TAG, std::slice::from_ref(&entity)),
            observation,
        )?;
    }
    for (entity, operation_ids) in state.projection.entity_history.iter() {
        let entity = EntityRef::External(entity.clone());
        let entity = encoded::<E, _>(&entity)?;
        for operation_id in operation_ids {
            let operation_id_bytes = encoded::<E, _>(operation_id)?;
            add_entry(
                &mut entries,
                make_key(ENTITY_HISTORY_TAG, &[entity.clone(), operation_id_bytes]),
                operation_id,
            )?;
        }
    }

    let event_operations = event_operations(state);
    for (event_id, event) in state.projection.events.iter() {
        let operation_id = event_operations.get(event_id).ok_or_else(|| {
            KvStorageError::InvalidProjection(format!(
                "event {:?} has no containing operation",
                event_id
            ))
        })?;
        let event_id_bytes = encoded::<E, _>(event_id)?;
        add_entry(
            &mut entries,
            make_key(EVENT_TAG, std::slice::from_ref(&event_id_bytes)),
            &(event.clone(), operation_id.clone()),
        )?;

        let mut entities = event.subjects.clone();
        for relation in &event.relations {
            entities.insert(relation.from.clone());
            entities.insert(relation.to.clone());
        }
        for entity in entities {
            let entity = encoded::<E, _>(&entity)?;
            add_entry(
                &mut entries,
                make_key(EVENT_ENTITY_TAG, &[entity, event_id_bytes.clone()]),
                &(event_id.clone(), operation_id.clone()),
            )?;
        }
    }

    for (from, graph_edges) in state.projection.outgoing.iter() {
        let from = encoded::<E, _>(from)?;
        for edge in graph_edges {
            let edge_key = encoded::<E, _>(edge)?;
            let to = encoded::<E, _>(&edge.relation.to)?;
            add_entry(
                &mut entries,
                make_key(GRAPH_OUTGOING_TAG, &[from.clone(), edge_key.clone()]),
                edge,
            )?;
            add_entry(
                &mut entries,
                make_key(GRAPH_INCOMING_TAG, &[to, edge_key]),
                edge,
            )?;
        }
    }

    Ok(entries)
}

fn event_operations<M: Model>(state: &State<M>) -> BTreeMap<EventId<M>, OperationId<M>> {
    let mut events = BTreeMap::new();
    let mut visited = BTreeSet::new();
    let mut stack = Vec::new();

    for operation_id in state.operations.keys() {
        stack.push((operation_id.clone(), false));
        while let Some((operation_id, expanded)) = stack.pop() {
            if expanded {
                if let Some(operation) = state.operation(&operation_id) {
                    for fact in &operation.facts {
                        if let Fact::EventRecorded(event) = fact {
                            events
                                .entry(event.id.clone())
                                .or_insert_with(|| operation.id.clone());
                        }
                    }
                }
                continue;
            }
            if !visited.insert(operation_id.clone()) {
                continue;
            }
            let Some(operation) = state.operation(&operation_id) else {
                continue;
            };
            stack.push((operation_id, true));
            for parent in operation.parents.iter().rev() {
                stack.push((parent.clone(), false));
            }
        }
    }
    events
}

fn operation_entries<M, E>(
    operation: &Operation<M>,
    skipped_events: &BTreeSet<Vec<u8>>,
) -> Result<Vec<Entry>, KvStorageError<E>>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
    E: Debug + Display,
{
    let mut entries = Vec::new();
    let operation_id = encoded::<E, _>(&operation.id)?;
    add_entry(
        &mut entries,
        make_key(OPERATION_TAG, std::slice::from_ref(&operation_id)),
        operation,
    )?;

    if let Some(source) = &operation.source {
        let source = encoded::<E, _>(source)?;
        add_entry(
            &mut entries,
            make_key(SOURCE_TAG, std::slice::from_ref(&source)),
            &operation.id,
        )?;
    }

    for fact in &operation.facts {
        match fact {
            Fact::SchemaRegistered(schema) => {
                let key = encoded::<E, _>(&schema.key)?;
                add_entry(
                    &mut entries,
                    make_key(SCHEMA_TAG, std::slice::from_ref(&key)),
                    schema,
                )?;
            }
            Fact::NamedQueryRegistered(query) => {
                let key = encoded::<E, _>(&query.key)?;
                add_entry(
                    &mut entries,
                    make_key(NAMED_QUERY_TAG, std::slice::from_ref(&key)),
                    query,
                )?;
            }
            Fact::SourceAnchored(anchor) => {
                let source = encoded::<E, _>(&anchor.source)?;
                add_entry(
                    &mut entries,
                    make_key(SOURCE_TAG, std::slice::from_ref(&source)),
                    &anchor.operation,
                )?;
            }
            Fact::EntityObserved(observation) => {
                let entity = EntityRef::External(observation.entity.clone());
                let entity = encoded::<E, _>(&entity)?;
                add_entry(
                    &mut entries,
                    make_key(ENTITY_TAG, std::slice::from_ref(&entity)),
                    observation,
                )?;

                let operation_id = encoded::<E, _>(&operation.id)?;
                add_entry(
                    &mut entries,
                    make_key(ENTITY_HISTORY_TAG, &[entity, operation_id.clone()]),
                    &operation.id,
                )?;
            }
            Fact::EventRecorded(event) => {
                let event_id = encoded::<E, _>(&event.id)?;
                if skipped_events.contains(&event_id) {
                    continue;
                }
                add_entry(
                    &mut entries,
                    make_key(EVENT_TAG, std::slice::from_ref(&event_id)),
                    &(event.clone(), operation.id.clone()),
                )?;

                let mut entities = event.subjects.clone();
                for relation in &event.relations {
                    entities.insert(relation.from.clone());
                    entities.insert(relation.to.clone());

                    let edge = GraphEdge {
                        relation: relation.clone(),
                        event: event.id.clone(),
                        operation: operation.id.clone(),
                    };
                    let edge_key = encoded::<E, _>(&edge)?;
                    let from = encoded::<E, _>(&relation.from)?;
                    let to = encoded::<E, _>(&relation.to)?;
                    add_entry(
                        &mut entries,
                        make_key(GRAPH_OUTGOING_TAG, &[from, edge_key.clone()]),
                        &edge,
                    )?;
                    add_entry(
                        &mut entries,
                        make_key(GRAPH_INCOMING_TAG, &[to, edge_key]),
                        &edge,
                    )?;
                }

                for entity in entities {
                    let entity = encoded::<E, _>(&entity)?;
                    add_entry(
                        &mut entries,
                        make_key(EVENT_ENTITY_TAG, &[entity, event_id.clone()]),
                        &(event.id.clone(), operation.id.clone()),
                    )?;
                }
            }
            _ => {}
        }
    }

    Ok(entries)
}

fn add_entry<E, T>(
    entries: &mut Vec<Entry>,
    key: Vec<u8>,
    value: &T,
) -> Result<(), KvStorageError<E>>
where
    E: Debug + Display,
    T: Serialize,
{
    let value = postcard::to_allocvec(value).map_err(KvStorageError::Encode)?;
    entries.push((key, value));
    Ok(())
}

fn encoded<E, T>(value: &T) -> Result<Vec<u8>, KvStorageError<E>>
where
    E: Debug + Display,
    T: Serialize,
{
    postcard::to_allocvec(value).map_err(KvStorageError::Encode)
}

fn decode<E, T>(value: &[u8]) -> Result<T, KvStorageError<E>>
where
    E: Debug + Display,
    T: DeserializeOwned,
{
    postcard::from_bytes(value).map_err(KvStorageError::Decode)
}

fn make_key(tag: u8, parts: &[Vec<u8>]) -> Vec<u8> {
    let capacity = 1 + parts.iter().map(|part| 4 + part.len()).sum::<usize>();
    let mut key = Vec::with_capacity(capacity);
    key.push(tag);
    for part in parts {
        key.extend_from_slice(&(part.len() as u32).to_be_bytes());
        key.extend_from_slice(part);
    }
    key
}

fn make_prefix(tag: u8, part: &[u8]) -> Vec<u8> {
    make_key(tag, &[part.to_vec()])
}
