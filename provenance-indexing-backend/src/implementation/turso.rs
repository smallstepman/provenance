//! Turso implementation of the generic provenance projection contract.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use postcard::Error as PostcardError;
use provenance_core::{
    EntityObservation, EntityRef, EventId, Fact, GraphEdge, Model, Operation, OperationId,
    SchemaDefinition, SchemaKey, State,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use turso::{Builder, Connection, Value};

use crate::{
    HistoryRows, IndexModel, ProjectionBackend, ProjectionIndex, ProjectionIndexError,
    ProjectionStorage,
};

const SCHEMA: &str = r#"
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
pub enum TursoStorageError {
    #[error("Turso projection storage error: {0}")]
    Db(#[from] turso::Error),
    #[error("could not create Turso projection directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not encode indexed value: {0}")]
    Encode(#[source] PostcardError),
    #[error("could not decode indexed value: {0}")]
    Decode(#[source] PostcardError),
    #[error("invalid indexed provenance: {0}")]
    InvalidProjection(String),
}

pub type TursoIndexError = ProjectionIndexError<TursoStorageError>;

pub struct TursoStorage {
    connection: Connection,
}

impl TursoStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TursoStorageError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let path = path.to_string_lossy().into_owned();
        let database = pollster::block_on(Builder::new_local(&path).build())?;
        Self::from_connection(database.connect()?)
    }

    pub fn in_memory() -> Result<Self, TursoStorageError> {
        let database = pollster::block_on(Builder::new_local(":memory:").build())?;
        Self::from_connection(database.connect()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, TursoStorageError> {
        pollster::block_on(connection.execute_batch(SCHEMA))?;
        Ok(Self { connection })
    }
}

impl<M> ProjectionStorage<M> for TursoStorage
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = TursoStorageError;

    fn replace(&mut self, state: &State<M>) -> Result<(), Self::Error> {
        pollster::block_on(async {
            self.connection.execute("BEGIN IMMEDIATE", ()).await?;
            let result = async {
                clear_transaction(&self.connection).await?;
                write_state(&self.connection, state).await
            }
            .await;
            match result {
                Ok(()) => {
                    self.connection.execute("COMMIT", ()).await?;
                    Ok(())
                }
                Err(error) => {
                    let _ = self.connection.execute("ROLLBACK", ()).await;
                    Err(error)
                }
            }
        })
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        pollster::block_on(async {
            self.connection.execute("BEGIN IMMEDIATE", ()).await?;
            let result = clear_transaction(&self.connection).await;
            match result {
                Ok(()) => {
                    self.connection.execute("COMMIT", ()).await?;
                    Ok(())
                }
                Err(error) => {
                    let _ = self.connection.execute("ROLLBACK", ()).await;
                    Err(error)
                }
            }
        })
    }

    fn operation_count(&self) -> Result<usize, Self::Error> {
        pollster::block_on(async {
            let mut rows = self
                .connection
                .query("SELECT COUNT(*) FROM operation_index", ())
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                TursoStorageError::InvalidProjection("operation count returned no row".into())
            })?;
            match row.get_value(0)? {
                Value::Integer(count) if count >= 0 => Ok(count as usize),
                value => Err(TursoStorageError::InvalidProjection(format!(
                    "operation count returned {value:?}"
                ))),
            }
        })
    }

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error> {
        let key = encode(operation_id)?;
        pollster::block_on(async {
            let mut rows = self
                .connection
                .query(
                    "SELECT operation_id FROM operation_index WHERE operation_id = ?1",
                    [key],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    }

    fn entity_observation(
        &self,
        entity: &EntityRef<M>,
    ) -> Result<Option<EntityObservation<M>>, Self::Error> {
        let entity_key = encode(entity)?;
        let blob = pollster::block_on(read_optional_blob(
            &self.connection,
            "SELECT observation FROM entity_index WHERE entity_key = ?1",
            [entity_key],
        ))?;
        blob.map(|bytes| decode(&bytes)).transpose()
    }

    fn schema(&self, key: &SchemaKey) -> Result<Option<SchemaDefinition>, Self::Error> {
        let schema_key = encode(key)?;
        let blob = pollster::block_on(read_optional_blob(
            &self.connection,
            "SELECT schema FROM schema_index WHERE schema_key = ?1",
            [schema_key],
        ))?;
        blob.map(|bytes| decode(&bytes)).transpose()
    }

    fn history(&self, entity: &EntityRef<M>) -> Result<HistoryRows<M>, Self::Error> {
        let entity_key = encode(entity)?;
        pollster::block_on(async {
            let mut history = HistoryRows::default();
            let mut operation_rows = self
                .connection
                .query(
                    "SELECT operation_id FROM entity_history_index WHERE entity_key = ?1",
                    [entity_key.clone()],
                )
                .await?;
            while let Some(row) = operation_rows.next().await? {
                history
                    .operations
                    .insert(decode(&blob_value(row.get_value(0)?)?)?);
            }

            let mut event_rows = self
                .connection
                .query(
                    "SELECT event_id, operation_id FROM event_index WHERE event_id IN (
                         SELECT event_id FROM event_entity_index WHERE entity_key = ?1
                     )",
                    [entity_key],
                )
                .await?;
            while let Some(row) = event_rows.next().await? {
                history
                    .events
                    .insert(decode(&blob_value(row.get_value(0)?)?)?);
                history
                    .operations
                    .insert(decode(&blob_value(row.get_value(1)?)?)?);
            }
            Ok(history)
        })
    }

    fn edges(
        &self,
        entity: &EntityRef<M>,
        direction: provenance_core::Direction,
    ) -> Result<Vec<GraphEdge<M>>, Self::Error> {
        let entity_key = encode(entity)?;
        pollster::block_on(async {
            let blobs = match direction {
                provenance_core::Direction::Outgoing => {
                    read_blobs(
                        &self.connection,
                        "SELECT edge FROM graph_edge_index WHERE from_key = ?1",
                        [entity_key.clone()],
                    )
                    .await?
                }
                provenance_core::Direction::Incoming => {
                    read_blobs(
                        &self.connection,
                        "SELECT edge FROM graph_edge_index WHERE to_key = ?1",
                        [entity_key.clone()],
                    )
                    .await?
                }
                provenance_core::Direction::Both => {
                    read_blobs(
                        &self.connection,
                        "SELECT edge FROM graph_edge_index WHERE from_key = ?1 OR to_key = ?2",
                        [entity_key.clone(), entity_key],
                    )
                    .await?
                }
            };
            blobs.iter().map(|blob| decode(blob)).collect()
        })
    }
}

pub struct TursoIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<TursoStorage, M>,
}

impl<M> TursoIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TursoIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                TursoStorage::open(path).map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }

    pub fn in_memory() -> Result<Self, TursoIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                TursoStorage::in_memory().map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }
}

impl<M> Deref for TursoIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<TursoStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for TursoIndex<M>
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

impl<M> provenance_core::QueryEngine<M> for TursoIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = TursoIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for TursoIndex<M>
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

async fn clear_transaction(connection: &Connection) -> Result<(), TursoStorageError> {
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
        connection
            .execute(format!("DELETE FROM {table}"), ())
            .await?;
    }
    Ok(())
}

async fn write_state<M>(connection: &Connection, state: &State<M>) -> Result<(), TursoStorageError>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    for (operation_id, operation) in state.operations.iter() {
        connection
            .execute(
                "INSERT INTO operation_index(operation_id, operation, parent_count) VALUES (?1, ?2, ?3)",
                (encode(operation_id)?, encode(operation)?, operation.parents.len() as i64),
            )
            .await?;
        for parent_id in &operation.parents {
            connection
                .execute(
                    "INSERT INTO operation_parent_index(operation_id, parent_id) VALUES (?1, ?2)",
                    (encode(operation_id)?, encode(parent_id)?),
                )
                .await?;
        }
    }

    for (source, operation_id) in state.projection.source_to_operation.iter() {
        connection
            .execute(
                "INSERT INTO source_index(source, operation_id) VALUES (?1, ?2)",
                (encode(source)?, encode(operation_id)?),
            )
            .await?;
    }
    for (schema_key, schema) in state.projection.schemas.iter() {
        connection
            .execute(
                "INSERT INTO schema_index(schema_key, schema) VALUES (?1, ?2)",
                (encode(schema_key)?, encode(schema)?),
            )
            .await?;
    }
    for (query_key, query) in state.projection.named_queries.iter() {
        connection
            .execute(
                "INSERT INTO named_query_index(query_key, query) VALUES (?1, ?2)",
                (encode(query_key)?, encode(query)?),
            )
            .await?;
    }
    for (entity, observation) in state.projection.entities.iter() {
        connection
            .execute(
                "INSERT INTO entity_index(entity_key, observation) VALUES (?1, ?2)",
                (
                    encode(&EntityRef::External(entity.clone()))?,
                    encode(observation)?,
                ),
            )
            .await?;
    }
    for (entity, operation_ids) in state.projection.entity_history.iter() {
        for operation_id in operation_ids {
            connection
                .execute(
                    "INSERT INTO entity_history_index(entity_key, operation_id) VALUES (?1, ?2)",
                    (
                        encode(&EntityRef::External(entity.clone()))?,
                        encode(operation_id)?,
                    ),
                )
                .await?;
        }
    }

    let event_operations = event_operations(state);
    for (event_id, event) in state.projection.events.iter() {
        let operation_id = event_operations.get(event_id).ok_or_else(|| {
            TursoStorageError::InvalidProjection(format!(
                "event {:?} has no containing operation",
                event_id
            ))
        })?;
        connection
            .execute(
                "INSERT INTO event_index(event_id, event, operation_id) VALUES (?1, ?2, ?3)",
                (encode(event_id)?, encode(event)?, encode(operation_id)?),
            )
            .await?;
        let mut entities = event.subjects.clone();
        for relation in &event.relations {
            entities.insert(relation.from.clone());
            entities.insert(relation.to.clone());
        }
        for entity in entities {
            connection
                .execute(
                    "INSERT INTO event_entity_index(event_id, entity_key) VALUES (?1, ?2)",
                    (encode(event_id)?, encode(&entity)?),
                )
                .await?;
        }
    }

    for (from, edges) in state.projection.outgoing.iter() {
        for edge in edges {
            connection
                .execute(
                    "INSERT INTO graph_edge_index(edge_key, from_key, to_key, edge) VALUES (?1, ?2, ?3, ?4)",
                    (
                        encode(edge)?,
                        encode(from)?,
                        encode(&edge.relation.to)?,
                        encode(edge)?,
                    ),
                )
                .await?;
        }
    }
    Ok(())
}

async fn read_optional_blob<P>(
    connection: &Connection,
    sql: &str,
    params: P,
) -> Result<Option<Vec<u8>>, TursoStorageError>
where
    P: turso::IntoParams,
{
    let mut rows = connection.query(sql, params).await?;
    rows.next()
        .await?
        .map(|row| row.get_value(0).map_err(TursoStorageError::from))
        .transpose()
        .map(|value| value.map(blob_value).transpose())?
}

async fn read_blobs<P>(
    connection: &Connection,
    sql: &str,
    params: P,
) -> Result<Vec<Vec<u8>>, TursoStorageError>
where
    P: turso::IntoParams,
{
    let mut rows = connection.query(sql, params).await?;
    let mut blobs = Vec::new();
    while let Some(row) = rows.next().await? {
        blobs.push(blob_value(row.get_value(0)?)?);
    }
    Ok(blobs)
}

fn blob_value(value: Value) -> Result<Vec<u8>, TursoStorageError> {
    match value {
        Value::Blob(bytes) => Ok(bytes),
        value => Err(TursoStorageError::InvalidProjection(format!(
            "expected BLOB value, got {value:?}"
        ))),
    }
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

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, TursoStorageError> {
    postcard::to_allocvec(value).map_err(TursoStorageError::Encode)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, TursoStorageError> {
    postcard::from_bytes(bytes).map_err(TursoStorageError::Decode)
}
