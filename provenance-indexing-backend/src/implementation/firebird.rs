//! Firebird Embedded implementation of the generic provenance projection contract.
//!
//! Firebird does not provide an in-memory database. `in_memory` therefore owns
//! an isolated temporary Embedded database and keeps it alive for the storage's
//! lifetime.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use postcard::Error as PostcardError;
use provenance_core::{
    EntityObservation, EntityRef, EventId, Fact, GraphEdge, Model, Operation, OperationId,
    SchemaDefinition, SchemaKey, State,
};
use rsfbclient::SimpleConnection;
use rsfbclient::prelude::{Execute, Queryable};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{
    HistoryRows, IndexModel, ProjectionBackend, ProjectionIndex, ProjectionIndexError,
    ProjectionStorage,
};

const KEY_LIMIT: usize = 2048;

#[derive(Debug, Error)]
pub enum FirebirdStorageError {
    #[error("Firebird projection storage error: {0}")]
    Db(#[from] rsfbclient::FbError),
    #[error("could not create Firebird projection directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not encode indexed value: {0}")]
    Encode(#[source] PostcardError),
    #[error("could not decode indexed value: {0}")]
    Decode(#[source] PostcardError),
    #[error("invalid indexed provenance: {0}")]
    InvalidProjection(String),
}

pub type FirebirdIndexError = ProjectionIndexError<FirebirdStorageError>;

pub struct FirebirdStorage {
    connection: RefCell<SimpleConnection>,
    _temporary: Option<tempfile::TempDir>,
}

impl FirebirdStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FirebirdStorageError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        Self::connect(path.to_path_buf(), None)
    }

    pub fn in_memory() -> Result<Self, FirebirdStorageError> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("projection.fdb");
        Self::connect(path, Some(temporary))
    }

    fn connect(
        path: PathBuf,
        temporary: Option<tempfile::TempDir>,
    ) -> Result<Self, FirebirdStorageError> {
        let library = std::env::var("PROVENANCE_FIREBIRD_CLIENT").unwrap_or_else(|_| {
            if cfg!(target_os = "windows") {
                "fbclient.dll".into()
            } else if cfg!(target_os = "macos") {
                "libfbclient.dylib".into()
            } else {
                "libfbclient.so".into()
            }
        });
        let mut builder = rsfbclient::builder_native()
            .with_dyn_load(library)
            .with_embedded();
        builder.db_name(path.to_string_lossy());
        let connection = if path.exists() {
            builder.connect()?
        } else {
            builder.create_database()?
        };
        let storage = Self {
            connection: RefCell::new(connection.into()),
            _temporary: temporary,
        };
        ensure_schema(&mut storage.connection.borrow_mut())?;
        Ok(storage)
    }
}

impl<M> ProjectionStorage<M> for FirebirdStorage
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = FirebirdStorageError;

    fn replace(&mut self, state: &State<M>) -> Result<(), Self::Error> {
        let mut connection = self.connection.borrow_mut();
        connection.begin_transaction()?;
        let result =
            clear_transaction(&mut connection).and_then(|()| write_state(&mut connection, state));
        match result {
            Ok(()) => connection.commit().map_err(Into::into),
            Err(error) => {
                let _ = connection.rollback();
                Err(error)
            }
        }
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        let mut connection = self.connection.borrow_mut();
        connection.begin_transaction()?;
        match clear_transaction(&mut connection) {
            Ok(()) => connection.commit().map_err(Into::into),
            Err(error) => {
                let _ = connection.rollback();
                Err(error)
            }
        }
    }

    fn operation_count(&self) -> Result<usize, Self::Error> {
        let row = self
            .connection
            .borrow_mut()
            .query_first::<_, (i64,)>("SELECT COUNT(*) FROM operation_index", ())?;
        let count = row.ok_or_else(|| {
            FirebirdStorageError::InvalidProjection("operation count returned no row".into())
        })?;
        if count.0 < 0 {
            return Err(FirebirdStorageError::InvalidProjection(
                "operation count was negative".into(),
            ));
        }
        Ok(count.0 as usize)
    }

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error> {
        let key = encode_key(operation_id)?;
        Ok(self
            .connection
            .borrow_mut()
            .query_first::<_, (String,)>(
                "SELECT operation_id FROM operation_index WHERE operation_id = ?",
                (key,),
            )?
            .is_some())
    }

    fn entity_observation(
        &self,
        entity: &EntityRef<M>,
    ) -> Result<Option<EntityObservation<M>>, Self::Error> {
        let key = encode_key(entity)?;
        let row = self.connection.borrow_mut().query_first::<_, (Vec<u8>,)>(
            "SELECT observation FROM entity_index WHERE entity_key = ?",
            (key,),
        )?;
        row.map(|(value,)| decode(&value)).transpose()
    }

    fn schema(&self, key: &SchemaKey) -> Result<Option<SchemaDefinition>, Self::Error> {
        let key = encode_key(key)?;
        let row = self.connection.borrow_mut().query_first::<_, (Vec<u8>,)>(
            "SELECT schema FROM schema_index WHERE schema_key = ?",
            (key,),
        )?;
        row.map(|(value,)| decode(&value)).transpose()
    }

    fn history(&self, entity: &EntityRef<M>) -> Result<HistoryRows<M>, Self::Error> {
        let key = encode_key(entity)?;
        let mut connection = self.connection.borrow_mut();
        let mut history = HistoryRows::default();
        for (operation_id,) in connection.query::<_, (String,)>(
            "SELECT operation_id FROM entity_history_index WHERE entity_key = ?",
            (key.clone(),),
        )? {
            history.operations.insert(decode_key(&operation_id)?);
        }
        for (event_id, operation_id) in connection.query::<_, (String, String)>(
            "SELECT event_id, operation_id FROM event_index WHERE event_id IN (
                 SELECT event_id FROM event_entity_index WHERE entity_key = ?
             )",
            (key,),
        )? {
            history.events.insert(decode_key(&event_id)?);
            history.operations.insert(decode_key(&operation_id)?);
        }
        Ok(history)
    }

    fn edges(
        &self,
        entity: &EntityRef<M>,
        direction: provenance_core::Direction,
    ) -> Result<Vec<GraphEdge<M>>, Self::Error> {
        let key = encode_key(entity)?;
        let mut connection = self.connection.borrow_mut();
        let rows = match direction {
            provenance_core::Direction::Outgoing => connection.query::<_, (Vec<u8>,)>(
                "SELECT edge FROM graph_edge_index WHERE from_key = ?",
                (key,),
            )?,
            provenance_core::Direction::Incoming => connection.query::<_, (Vec<u8>,)>(
                "SELECT edge FROM graph_edge_index WHERE to_key = ?",
                (key,),
            )?,
            provenance_core::Direction::Both => connection.query::<_, (Vec<u8>,)>(
                "SELECT edge FROM graph_edge_index WHERE from_key = ? OR to_key = ?",
                (key.clone(), key),
            )?,
        };
        rows.into_iter().map(|(value,)| decode(&value)).collect()
    }
}

pub struct FirebirdIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<FirebirdStorage, M>,
}

impl<M> FirebirdIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FirebirdIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                FirebirdStorage::open(path).map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }

    pub fn in_memory() -> Result<Self, FirebirdIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                FirebirdStorage::in_memory().map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }
}

impl<M> Deref for FirebirdIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<FirebirdStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for FirebirdIndex<M>
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

impl<M> provenance_core::QueryEngine<M> for FirebirdIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = FirebirdIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for FirebirdIndex<M>
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

fn ensure_schema(connection: &mut SimpleConnection) -> Result<(), FirebirdStorageError> {
    for (name, ddl) in [
        (
            "OPERATION_INDEX",
            "CREATE TABLE operation_index (operation_id VARCHAR(2048) NOT NULL PRIMARY KEY, operation BLOB SUB_TYPE BINARY NOT NULL, parent_count BIGINT NOT NULL)",
        ),
        (
            "OPERATION_PARENT_INDEX",
            "CREATE TABLE operation_parent_index (operation_id VARCHAR(2048) NOT NULL, parent_id VARCHAR(2048) NOT NULL, PRIMARY KEY (operation_id, parent_id))",
        ),
        (
            "SOURCE_INDEX",
            "CREATE TABLE source_index (source VARCHAR(2048) NOT NULL PRIMARY KEY, operation_id VARCHAR(2048) NOT NULL)",
        ),
        (
            "SCHEMA_INDEX",
            "CREATE TABLE schema_index (schema_key VARCHAR(2048) NOT NULL PRIMARY KEY, schema BLOB SUB_TYPE BINARY NOT NULL)",
        ),
        (
            "NAMED_QUERY_INDEX",
            "CREATE TABLE named_query_index (query_key VARCHAR(2048) NOT NULL PRIMARY KEY, query BLOB SUB_TYPE BINARY NOT NULL)",
        ),
        (
            "ENTITY_INDEX",
            "CREATE TABLE entity_index (entity_key VARCHAR(2048) NOT NULL PRIMARY KEY, observation BLOB SUB_TYPE BINARY NOT NULL)",
        ),
        (
            "ENTITY_HISTORY_INDEX",
            "CREATE TABLE entity_history_index (entity_key VARCHAR(2048) NOT NULL, operation_id VARCHAR(2048) NOT NULL, PRIMARY KEY (entity_key, operation_id))",
        ),
        (
            "EVENT_INDEX",
            "CREATE TABLE event_index (event_id VARCHAR(2048) NOT NULL PRIMARY KEY, event BLOB SUB_TYPE BINARY NOT NULL, operation_id VARCHAR(2048) NOT NULL)",
        ),
        (
            "EVENT_ENTITY_INDEX",
            "CREATE TABLE event_entity_index (event_id VARCHAR(2048) NOT NULL, entity_key VARCHAR(2048) NOT NULL, PRIMARY KEY (event_id, entity_key))",
        ),
        (
            "GRAPH_EDGE_INDEX",
            "CREATE TABLE graph_edge_index (edge_key VARCHAR(2048) NOT NULL PRIMARY KEY, from_key VARCHAR(2048) NOT NULL, to_key VARCHAR(2048) NOT NULL, edge BLOB SUB_TYPE BINARY NOT NULL)",
        ),
    ] {
        let exists = connection
            .query_first::<_, (i64,)>(
                "SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = ?",
                (name.to_string(),),
            )?
            .is_some_and(|(count,)| count != 0);
        if !exists {
            connection.execute(ddl, ())?;
        }
    }
    for (name, ddl) in [
        (
            "GRAPH_EDGE_FROM_IDX",
            "CREATE INDEX graph_edge_from_idx ON graph_edge_index(from_key)",
        ),
        (
            "GRAPH_EDGE_TO_IDX",
            "CREATE INDEX graph_edge_to_idx ON graph_edge_index(to_key)",
        ),
        (
            "EVENT_ENTITY_ENTITY_IDX",
            "CREATE INDEX event_entity_entity_idx ON event_entity_index(entity_key)",
        ),
    ] {
        let exists = connection
            .query_first::<_, (i64,)>(
                "SELECT COUNT(*) FROM RDB$INDICES WHERE RDB$INDEX_NAME = ?",
                (name.to_string(),),
            )?
            .is_some_and(|(count,)| count != 0);
        if !exists {
            connection.execute(ddl, ())?;
        }
    }
    Ok(())
}

fn clear_transaction(connection: &mut SimpleConnection) -> Result<(), FirebirdStorageError> {
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
        connection.execute(&format!("DELETE FROM {table}"), ())?;
    }
    Ok(())
}

fn write_state<M>(
    connection: &mut SimpleConnection,
    state: &State<M>,
) -> Result<(), FirebirdStorageError>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    for (operation_id, operation) in state.operations.iter() {
        connection.execute(
            "INSERT INTO operation_index(operation_id, operation, parent_count) VALUES (?, ?, ?)",
            (
                encode_key(operation_id)?,
                encode(operation)?,
                operation.parents.len() as i64,
            ),
        )?;
        for parent_id in &operation.parents {
            connection.execute(
                "INSERT INTO operation_parent_index(operation_id, parent_id) VALUES (?, ?)",
                (encode_key(operation_id)?, encode_key(parent_id)?),
            )?;
        }
    }

    for (source, operation_id) in state.projection.source_to_operation.iter() {
        connection.execute(
            "INSERT INTO source_index(source, operation_id) VALUES (?, ?)",
            (encode_key(source)?, encode_key(operation_id)?),
        )?;
    }
    for (schema_key, schema) in state.projection.schemas.iter() {
        connection.execute(
            "INSERT INTO schema_index(schema_key, schema) VALUES (?, ?)",
            (encode_key(schema_key)?, encode(schema)?),
        )?;
    }
    for (query_key, query) in state.projection.named_queries.iter() {
        connection.execute(
            "INSERT INTO named_query_index(query_key, query) VALUES (?, ?)",
            (encode_key(query_key)?, encode(query)?),
        )?;
    }
    for (entity, observation) in state.projection.entities.iter() {
        connection.execute(
            "INSERT INTO entity_index(entity_key, observation) VALUES (?, ?)",
            (
                encode_key(&EntityRef::External(entity.clone()))?,
                encode(observation)?,
            ),
        )?;
    }
    for (entity, operation_ids) in state.projection.entity_history.iter() {
        for operation_id in operation_ids {
            connection.execute(
                "INSERT INTO entity_history_index(entity_key, operation_id) VALUES (?, ?)",
                (
                    encode_key(&EntityRef::External(entity.clone()))?,
                    encode_key(operation_id)?,
                ),
            )?;
        }
    }

    let event_operations = event_operations(state);
    for (event_id, event) in state.projection.events.iter() {
        let operation_id = event_operations.get(event_id).ok_or_else(|| {
            FirebirdStorageError::InvalidProjection(format!(
                "event {:?} has no containing operation",
                event_id
            ))
        })?;
        connection.execute(
            "INSERT INTO event_index(event_id, event, operation_id) VALUES (?, ?, ?)",
            (
                encode_key(event_id)?,
                encode(event)?,
                encode_key(operation_id)?,
            ),
        )?;
        let mut entities = event.subjects.clone();
        for relation in &event.relations {
            entities.insert(relation.from.clone());
            entities.insert(relation.to.clone());
        }
        for entity in entities {
            connection.execute(
                "INSERT INTO event_entity_index(event_id, entity_key) VALUES (?, ?)",
                (encode_key(event_id)?, encode_key(&entity)?),
            )?;
        }
    }

    for (from, edges) in state.projection.outgoing.iter() {
        for edge in edges {
            connection.execute(
                "INSERT INTO graph_edge_index(edge_key, from_key, to_key, edge) VALUES (?, ?, ?, ?)",
                (
                    encode_key(edge)?,
                    encode_key(from)?,
                    encode_key(&edge.relation.to)?,
                    encode(edge)?,
                ),
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

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, FirebirdStorageError> {
    postcard::to_allocvec(value).map_err(FirebirdStorageError::Encode)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, FirebirdStorageError> {
    postcard::from_bytes(bytes).map_err(FirebirdStorageError::Decode)
}

fn encode_key<T: Serialize>(value: &T) -> Result<String, FirebirdStorageError> {
    let bytes = encode(value)?;
    let mut key = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        key.push_str(&format!("{byte:02x}"));
    }
    if key.len() > KEY_LIMIT {
        return Err(FirebirdStorageError::InvalidProjection(format!(
            "encoded key exceeds Firebird VARCHAR({KEY_LIMIT})"
        )));
    }
    Ok(key)
}

fn decode_key<T: DeserializeOwned>(key: &str) -> Result<T, FirebirdStorageError> {
    if key.len() % 2 != 0 {
        return Err(FirebirdStorageError::InvalidProjection(
            "encoded Firebird key has odd length".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(key.len() / 2);
    let chars = key.as_bytes();
    for index in (0..chars.len()).step_by(2) {
        let high = hex_digit(chars[index]).ok_or_else(|| {
            FirebirdStorageError::InvalidProjection("invalid hexadecimal Firebird key".into())
        })?;
        let low = hex_digit(chars[index + 1]).ok_or_else(|| {
            FirebirdStorageError::InvalidProjection("invalid hexadecimal Firebird key".into())
        })?;
        bytes.push((high << 4) | low);
    }
    decode(&bytes)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
