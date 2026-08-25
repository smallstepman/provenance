//! SQLite implementation of the generic provenance projection contract.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use db::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
#[cfg(feature = "doltlite")]
use doltlite as db;
use postcard::Error as PostcardError;
use provenance_core::{
    EntityObservation, EntityRef, EventId, Fact, GraphEdge, Model, Operation, OperationId,
    SchemaDefinition, SchemaKey, State,
};
#[cfg(feature = "sqlite")]
use rusqlite as db;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{
    HistoryRows, IndexModel, ProjectionBackend, ProjectionIndex, ProjectionIndexError,
    ProjectionStorage,
};

const SCHEMA: &str = r#"
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS operation_index (
    operation_id BLOB PRIMARY KEY NOT NULL,
    operation BLOB NOT NULL,
    parent_count INTEGER NOT NULL
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
pub enum SqliteStorageError {
    #[error("SQLite projection storage error: {0}")]
    Sql(#[from] db::Error),
    #[error("could not create SQLite projection directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not encode indexed value: {0}")]
    Encode(#[source] PostcardError),
    #[error("could not decode indexed value: {0}")]
    Decode(#[source] PostcardError),
    #[error("invalid indexed provenance: {0}")]
    InvalidProjection(String),
}

pub type SqliteIndexError = ProjectionIndexError<SqliteStorageError>;

pub struct SqliteStorage {
    connection: Connection,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteStorageError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        Self::from_connection(Connection::open(path)?)
    }

    pub fn in_memory() -> Result<Self, SqliteStorageError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, SqliteStorageError> {
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self { connection })
    }
}

impl<M> ProjectionStorage<M> for SqliteStorage
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = SqliteStorageError;

    fn replace(&mut self, state: &State<M>) -> Result<(), Self::Error> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        clear_transaction(&transaction)?;
        write_state(&transaction, state)?;
        transaction.commit()?;
        Ok(())
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        let blob = self
            .connection
            .query_row(
                "SELECT observation FROM entity_index WHERE entity_key = ?1",
                params![entity_key],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        blob.map(|blob| decode(&blob)).transpose()
    }

    fn schema(&self, key: &SchemaKey) -> Result<Option<SchemaDefinition>, Self::Error> {
        let schema_key = encode(key)?;
        let blob = self
            .connection
            .query_row(
                "SELECT schema FROM schema_index WHERE schema_key = ?1",
                params![schema_key],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        blob.map(|blob| decode(&blob)).transpose()
    }

    fn history(&self, entity: &EntityRef<M>) -> Result<HistoryRows<M>, Self::Error> {
        let entity_key = encode(entity)?;
        let mut history = HistoryRows::default();
        let mut operation_statement = self
            .connection
            .prepare("SELECT operation_id FROM entity_history_index WHERE entity_key = ?1")?;
        let operation_rows =
            operation_statement.query_map(params![&entity_key], |row| row.get::<_, Vec<u8>>(0))?;
        for row in operation_rows {
            history.operations.insert(decode(&row?)?);
        }

        let mut event_statement = self.connection.prepare(
            "SELECT event_id, operation_id FROM event_index WHERE event_id IN (
                 SELECT event_id FROM event_entity_index WHERE entity_key = ?1
             )",
        )?;
        let event_rows = event_statement.query_map(params![entity_key], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        for row in event_rows {
            let (event_id, operation_id) = row?;
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
        let sql = match direction {
            provenance_core::Direction::Outgoing => {
                "SELECT edge FROM graph_edge_index WHERE from_key = ?1"
            }
            provenance_core::Direction::Incoming => {
                "SELECT edge FROM graph_edge_index WHERE to_key = ?1"
            }
            provenance_core::Direction::Both => {
                "SELECT edge FROM graph_edge_index WHERE from_key = ?1 OR to_key = ?2"
            }
        };
        let mut statement = self.connection.prepare(sql)?;
        let blobs = match direction {
            provenance_core::Direction::Both => statement
                .query_map(params![entity_key, entity_key], |row| {
                    row.get::<_, Vec<u8>>(0)
                })?
                .collect::<db::Result<Vec<_>>>()?,
            _ => statement
                .query_map(params![entity_key], |row| row.get::<_, Vec<u8>>(0))?
                .collect::<db::Result<Vec<_>>>()?,
        };
        blobs.iter().map(|blob| decode(blob)).collect()
    }
}

pub struct SqliteIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<SqliteStorage, M>,
}

impl<M> SqliteIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                SqliteStorage::open(path).map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }

    pub fn in_memory() -> Result<Self, SqliteIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                SqliteStorage::in_memory().map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }
}

impl<M> Deref for SqliteIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<SqliteStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for SqliteIndex<M>
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

impl<M> provenance_core::QueryEngine<M> for SqliteIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = SqliteIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for SqliteIndex<M>
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

fn clear_transaction(transaction: &Transaction<'_>) -> Result<(), SqliteStorageError> {
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

fn write_state<M>(transaction: &Transaction<'_>, state: &State<M>) -> Result<(), SqliteStorageError>
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
            SqliteStorageError::InvalidProjection(format!(
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

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SqliteStorageError> {
    postcard::to_allocvec(value).map_err(SqliteStorageError::Encode)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SqliteStorageError> {
    postcard::from_bytes(bytes).map_err(SqliteStorageError::Decode)
}
