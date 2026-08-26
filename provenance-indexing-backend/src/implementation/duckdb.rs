//! DuckDB implementation of the generic provenance projection contract.
//!
//! This is a proving-ground backend: it shares projection and query semantics
//! with the SQLite adapter while using DuckDB's storage and execution engine.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use duckdb::{Connection, Transaction, params};
use postcard::Error as PostcardError;
use provenance_core::{
    EntityObservation, EntityRef, EventId, Fact, GraphEdge, Model, Operation, OperationId,
    SchemaDefinition, SchemaKey, State,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{
    HistoryRows, IndexModel, ProjectionBackend, ProjectionIndex, ProjectionIndexError,
    ProjectionStorage,
};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS operation_index (
    operation_id BLOB PRIMARY KEY NOT NULL,
    operation BLOB NOT NULL,
    parent_count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_parent_index (
    operation_id BLOB NOT NULL,
    parent_id BLOB NOT NULL,
    PRIMARY KEY (operation_id, parent_id)
);

CREATE TABLE IF NOT EXISTS source_index (
    source BLOB PRIMARY KEY NOT NULL,
    operation_id BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_index (
    schema_key BLOB PRIMARY KEY NOT NULL,
    schema BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS named_query_index (
    query_key BLOB PRIMARY KEY NOT NULL,
    query BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_index (
    entity_key BLOB PRIMARY KEY NOT NULL,
    observation BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_history_index (
    entity_key BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    PRIMARY KEY (entity_key, operation_id)
);

CREATE TABLE IF NOT EXISTS event_index (
    event_id BLOB PRIMARY KEY NOT NULL,
    event BLOB NOT NULL,
    operation_id BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS event_entity_index (
    event_id BLOB NOT NULL,
    entity_key BLOB NOT NULL,
    PRIMARY KEY (event_id, entity_key)
);

CREATE TABLE IF NOT EXISTS graph_edge_index (
    edge_key BLOB PRIMARY KEY NOT NULL,
    from_key BLOB NOT NULL,
    to_key BLOB NOT NULL,
    edge BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS graph_edge_from_idx ON graph_edge_index(from_key);
CREATE INDEX IF NOT EXISTS graph_edge_to_idx ON graph_edge_index(to_key);
CREATE INDEX IF NOT EXISTS event_entity_entity_idx ON event_entity_index(entity_key);
"#;

#[derive(Debug, Error)]
pub enum DuckDbStorageError {
    #[error("DuckDB projection storage error: {0}")]
    Db(#[from] duckdb::Error),
    #[error("could not create DuckDB projection directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not encode indexed value: {0}")]
    Encode(#[source] PostcardError),
    #[error("could not decode indexed value: {0}")]
    Decode(#[source] PostcardError),
    #[error("invalid indexed provenance: {0}")]
    InvalidProjection(String),
}

pub type DuckDbIndexError = ProjectionIndexError<DuckDbStorageError>;

pub struct DuckDbStorage {
    connection: Connection,
}

impl DuckDbStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DuckDbStorageError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        Self::from_connection(Connection::open(path)?)
    }

    pub fn in_memory() -> Result<Self, DuckDbStorageError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, DuckDbStorageError> {
        connection.execute_batch(SCHEMA)?;
        Ok(Self { connection })
    }
}

impl<M> ProjectionStorage<M> for DuckDbStorage
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = DuckDbStorageError;

    fn replace(&mut self, state: &State<M>) -> Result<(), Self::Error> {
        let transaction = self.connection.transaction()?;
        clear_transaction(&transaction)?;
        write_state(&transaction, state)?;
        transaction.commit()?;
        Ok(())
    }

    fn apply_delta(
        &mut self,
        operations: &[Operation<M>],
        _state: &State<M>,
    ) -> Result<(), Self::Error> {
        if operations.is_empty() {
            return Ok(());
        }
        let transaction = self.connection.transaction()?;
        write_operation_delta(&transaction, operations)?;
        transaction.commit()?;
        Ok(())
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        let transaction = self.connection.transaction()?;
        clear_transaction(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    fn operation_count(&self) -> Result<usize, Self::Error> {
        let count: i64 =
            self.connection
                .query_row("SELECT COUNT(*) FROM operation_index", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    fn indexed_operations(&self) -> Result<Vec<Operation<M>>, Self::Error> {
        let mut statement = self
            .connection
            .prepare("SELECT operation FROM operation_index")?;
        let mut rows = statement.query([])?;
        let mut operations = Vec::new();
        while let Some(row) = rows.next()? {
            let blob: Vec<u8> = row.get(0)?;
            operations.push(decode(&blob)?);
        }
        Ok(operations)
    }

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error> {
        let key = encode(operation_id)?;
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM operation_index WHERE operation_id = ?1",
            params![key],
            |row| row.get(0),
        )?;
        Ok(count != 0)
    }

    fn entity_observation(
        &self,
        entity: &EntityRef<M>,
    ) -> Result<Option<EntityObservation<M>>, Self::Error> {
        let entity_key = encode(entity)?;
        optional_blob(
            &self.connection,
            "SELECT observation FROM entity_index WHERE entity_key = ?1",
            params![entity_key],
        )?
        .map(|blob| decode(&blob))
        .transpose()
    }

    fn schema(&self, key: &SchemaKey) -> Result<Option<SchemaDefinition>, Self::Error> {
        let schema_key = encode(key)?;
        optional_blob(
            &self.connection,
            "SELECT schema FROM schema_index WHERE schema_key = ?1",
            params![schema_key],
        )?
        .map(|blob| decode(&blob))
        .transpose()
    }

    fn history(&self, entity: &EntityRef<M>) -> Result<HistoryRows<M>, Self::Error> {
        let entity_key = encode(entity)?;
        let mut history = HistoryRows::default();

        let mut operation_statement = self
            .connection
            .prepare("SELECT operation_id FROM entity_history_index WHERE entity_key = ?1")?;
        let mut operation_rows = operation_statement.query(params![&entity_key])?;
        while let Some(row) = operation_rows.next()? {
            let blob: Vec<u8> = row.get(0)?;
            history.operations.insert(decode(&blob)?);
        }

        let mut event_statement = self.connection.prepare(
            "SELECT event_id, operation_id FROM event_index WHERE event_id IN (
                 SELECT event_id FROM event_entity_index WHERE entity_key = ?1
             )",
        )?;
        let mut event_rows = event_statement.query(params![entity_key])?;
        while let Some(row) = event_rows.next()? {
            let event_id: Vec<u8> = row.get(0)?;
            let operation_id: Vec<u8> = row.get(1)?;
            history.events.insert(decode(&event_id)?);
            history.operations.insert(decode(&operation_id)?);
        }
        Ok(history)
    }

    fn edges(
        &self,
        entity: &EntityRef<M>,
        direction: provenance_core::Direction,
    ) -> Result<Vec<GraphEdge<M>>, Self::Error> {
        let entity_key = encode(entity)?;
        let blobs = match direction {
            provenance_core::Direction::Outgoing => read_blobs(
                &self.connection,
                "SELECT edge FROM graph_edge_index WHERE from_key = ?1",
                params![entity_key],
            )?,
            provenance_core::Direction::Incoming => read_blobs(
                &self.connection,
                "SELECT edge FROM graph_edge_index WHERE to_key = ?1",
                params![entity_key],
            )?,
            provenance_core::Direction::Both => read_blobs(
                &self.connection,
                "SELECT edge FROM graph_edge_index WHERE from_key = ?1 OR to_key = ?2",
                params![entity_key, entity_key],
            )?,
        };
        blobs.iter().map(|blob| decode(blob)).collect()
    }
}

pub struct DuckDbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<DuckDbStorage, M>,
}

impl<M> DuckDbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DuckDbIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                DuckDbStorage::open(path).map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }

    pub fn in_memory() -> Result<Self, DuckDbIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                DuckDbStorage::in_memory().map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }
}

impl<M> Deref for DuckDbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<DuckDbStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for DuckDbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<M> provenance_core::QueryEngine<M> for DuckDbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = DuckDbIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for DuckDbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    fn rebuild(&mut self, operations: &[Operation<M>]) -> Result<(), Self::Error> {
        self.inner.rebuild(operations)
    }

    fn rebuild_from<T>(&mut self, store: &T) -> Result<(), Self::Error>
    where
        T: provenance_core::ProvenanceStore<M>,
        T::Error: std::fmt::Display,
    {
        self.inner.rebuild_from(store)
    }

    fn update_from<T>(&mut self, store: &T) -> Result<(), Self::Error>
    where
        T: provenance_core::ProvenanceStore<M>,
        T::Error: std::fmt::Display,
    {
        self.inner.update_from(store)
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.inner.clear()
    }

    fn operation_count(&self) -> Result<usize, Self::Error> {
        self.inner.operation_count()
    }

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error> {
        self.inner.contains(operation_id)
    }
}

fn clear_transaction(transaction: &Transaction<'_>) -> Result<(), DuckDbStorageError> {
    for table in [
        "operation_parent_index",
        "source_index",
        "schema_index",
        "named_query_index",
        "entity_index",
        "entity_history_index",
        "event_entity_index",
        "event_index",
        "graph_edge_index",
        "operation_index",
    ] {
        transaction.execute(&format!("DELETE FROM {table}"), [])?;
    }
    Ok(())
}

fn write_state<M>(transaction: &Transaction<'_>, state: &State<M>) -> Result<(), DuckDbStorageError>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    for (operation_id, operation) in state.operations.iter() {
        transaction.execute(
            "INSERT INTO operation_index(operation_id, operation, parent_count) VALUES (?1, ?2, ?3)",
            params![
                encode(operation_id)?,
                encode(operation)?,
                operation.parents.len() as i64
            ],
        )?;
        for parent_id in &operation.parents {
            transaction.execute(
                "INSERT INTO operation_parent_index(operation_id, parent_id) VALUES (?1, ?2)",
                params![encode(operation_id)?, encode(parent_id)?],
            )?;
        }
    }

    for (source, operation_id) in state.projection.source_to_operation.iter() {
        transaction.execute(
            "INSERT INTO source_index(source, operation_id) VALUES (?1, ?2)",
            params![encode(source)?, encode(operation_id)?],
        )?;
    }
    for (schema_key, schema) in state.projection.schemas.iter() {
        transaction.execute(
            "INSERT INTO schema_index(schema_key, schema) VALUES (?1, ?2)",
            params![encode(schema_key)?, encode(schema)?],
        )?;
    }
    for (query_key, query) in state.projection.named_queries.iter() {
        transaction.execute(
            "INSERT INTO named_query_index(query_key, query) VALUES (?1, ?2)",
            params![encode(query_key)?, encode(query)?],
        )?;
    }
    for (entity, observation) in state.projection.entities.iter() {
        transaction.execute(
            "INSERT INTO entity_index(entity_key, observation) VALUES (?1, ?2)",
            params![
                encode(&EntityRef::External(entity.clone()))?,
                encode(observation)?
            ],
        )?;
    }
    for (entity, operation_ids) in state.projection.entity_history.iter() {
        for operation_id in operation_ids {
            transaction.execute(
                "INSERT INTO entity_history_index(entity_key, operation_id) VALUES (?1, ?2)",
                params![
                    encode(&EntityRef::External(entity.clone()))?,
                    encode(operation_id)?
                ],
            )?;
        }
    }

    let event_operations = event_operations(state);
    for (event_id, event) in state.projection.events.iter() {
        let operation_id = event_operations.get(event_id).ok_or_else(|| {
            DuckDbStorageError::InvalidProjection(format!(
                "event {:?} has no containing operation",
                event_id
            ))
        })?;
        transaction.execute(
            "INSERT INTO event_index(event_id, event, operation_id) VALUES (?1, ?2, ?3)",
            params![encode(event_id)?, encode(event)?, encode(operation_id)?],
        )?;
        let mut entities = event.subjects.clone();
        for relation in &event.relations {
            entities.insert(relation.from.clone());
            entities.insert(relation.to.clone());
        }
        for entity in entities {
            transaction.execute(
                "INSERT INTO event_entity_index(event_id, entity_key) VALUES (?1, ?2)",
                params![encode(event_id)?, encode(&entity)?],
            )?;
        }
    }

    for (from, edges) in state.projection.outgoing.iter() {
        for edge in edges {
            transaction.execute(
                "INSERT INTO graph_edge_index(edge_key, from_key, to_key, edge) VALUES (?1, ?2, ?3, ?4)",
                params![
                    encode(edge)?,
                    encode(from)?,
                    encode(&edge.relation.to)?,
                    encode(edge)?
                ],
            )?;
        }
    }
    Ok(())
}

fn write_operation_delta<M>(
    transaction: &Transaction<'_>,
    operations: &[Operation<M>],
) -> Result<(), DuckDbStorageError>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    for operation in operations {
        let operation_id = encode(&operation.id)?;
        let operation_blob = encode(operation)?;
        transaction.execute(
            "INSERT OR IGNORE INTO operation_index(operation_id, operation, parent_count) \
             VALUES (?1, ?2, ?3)",
            params![
                &operation_id,
                &operation_blob,
                operation.parents.len() as i64
            ],
        )?;
        for parent_id in &operation.parents {
            transaction.execute(
                "INSERT OR IGNORE INTO operation_parent_index(operation_id, parent_id) \
                 VALUES (?1, ?2)",
                params![&operation_id, encode(parent_id)?],
            )?;
        }

        if let Some(source) = &operation.source {
            transaction.execute(
                "INSERT OR IGNORE INTO source_index(source, operation_id) VALUES (?1, ?2)",
                params![encode(source)?, &operation_id],
            )?;
        }

        for fact in &operation.facts {
            match fact {
                Fact::SchemaRegistered(schema) => {
                    transaction.execute(
                        "INSERT OR IGNORE INTO schema_index(schema_key, schema) VALUES (?1, ?2)",
                        params![encode(&schema.key)?, encode(schema)?],
                    )?;
                }
                Fact::NamedQueryRegistered(query) => {
                    transaction.execute(
                        "INSERT OR IGNORE INTO named_query_index(query_key, query) VALUES (?1, ?2)",
                        params![encode(&query.key)?, encode(query)?],
                    )?;
                }
                Fact::SourceAnchored(anchor) => {
                    transaction.execute(
                        "INSERT OR IGNORE INTO source_index(source, operation_id) VALUES (?1, ?2)",
                        params![encode(&anchor.source)?, encode(&anchor.operation)?],
                    )?;
                }
                Fact::EntityObserved(observation) => {
                    let entity = EntityRef::External(observation.entity.clone());
                    let entity_key = encode(&entity)?;
                    transaction.execute(
                        "INSERT OR REPLACE INTO entity_index(entity_key, observation) \
                         VALUES (?1, ?2)",
                        params![&entity_key, encode(observation)?],
                    )?;
                    transaction.execute(
                        "INSERT OR IGNORE INTO entity_history_index(entity_key, operation_id) \
                         VALUES (?1, ?2)",
                        params![&entity_key, &operation_id],
                    )?;
                }
                Fact::EventRecorded(event) => {
                    let event_id = encode(&event.id)?;
                    let inserted = transaction.execute(
                        "INSERT OR IGNORE INTO event_index(event_id, event, operation_id) \
                         VALUES (?1, ?2, ?3)",
                        params![&event_id, encode(event)?, &operation_id],
                    )?;
                    if inserted == 0 {
                        continue;
                    }
                    let mut entities = event.subjects.clone();
                    for relation in &event.relations {
                        entities.insert(relation.from.clone());
                        entities.insert(relation.to.clone());
                        let edge = GraphEdge {
                            relation: relation.clone(),
                            event: event.id.clone(),
                            operation: operation.id.clone(),
                        };
                        let edge_key = encode(&edge)?;
                        transaction.execute(
                            "INSERT OR IGNORE INTO graph_edge_index \
                             (edge_key, from_key, to_key, edge) VALUES (?1, ?2, ?3, ?4)",
                            params![
                                &edge_key,
                                encode(&relation.from)?,
                                encode(&relation.to)?,
                                encode(&edge)?
                            ],
                        )?;
                    }
                    for entity in entities {
                        transaction.execute(
                            "INSERT OR IGNORE INTO event_entity_index(event_id, entity_key) \
                             VALUES (?1, ?2)",
                            params![&event_id, encode(&entity)?],
                        )?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn event_operations<M: Model>(state: &State<M>) -> BTreeMap<EventId<M>, OperationId<M>> {
    let mut result = BTreeMap::new();
    for (operation_id, operation) in state.operations.iter() {
        for fact in &operation.facts {
            if let Fact::EventRecorded(event) = fact {
                result.insert(event.id.clone(), operation_id.clone());
            }
        }
    }
    result
}

fn optional_blob<P>(
    connection: &Connection,
    sql: &str,
    parameters: P,
) -> Result<Option<Vec<u8>>, DuckDbStorageError>
where
    P: duckdb::Params,
{
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query(parameters)?;
    match rows.next()? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

fn read_blobs<P>(
    connection: &Connection,
    sql: &str,
    parameters: P,
) -> Result<Vec<Vec<u8>>, DuckDbStorageError>
where
    P: duckdb::Params,
{
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query(parameters)?;
    let mut blobs = Vec::new();
    while let Some(row) = rows.next()? {
        blobs.push(row.get(0)?);
    }
    Ok(blobs)
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, DuckDbStorageError> {
    postcard::to_allocvec(value).map_err(DuckDbStorageError::Encode)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DuckDbStorageError> {
    postcard::from_bytes(bytes).map_err(DuckDbStorageError::Decode)
}
