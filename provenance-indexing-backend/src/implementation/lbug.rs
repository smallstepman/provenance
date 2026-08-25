//! LadybugDB implementation of the generic provenance projection contract.
//!
//! The disposable projection is represented as typed LadybugDB node and
//! relationship tables. Payloads and keys are postcard-encoded hexadecimal
//! strings so the database remains independent of the concrete provenance
//! model.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::path::Path;

use lbug::{Connection, Database, SystemConfig, Value};
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

const NODE_TABLES: &[&str] = &[
    "operation_index",
    "operation_parent_index",
    "source_index",
    "schema_index",
    "named_query_index",
    "entity_index",
    "entity_history_index",
    "event_index",
    "event_entity_index",
    "graph_node",
];

#[derive(Debug, Error)]
pub enum LbugStorageError {
    #[error("LadybugDB projection storage error: {0}")]
    Db(#[from] lbug::Error),
    #[error("could not create LadybugDB projection directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not encode indexed value: {0}")]
    Encode(#[source] PostcardError),
    #[error("could not decode indexed value: {0}")]
    Decode(#[source] PostcardError),
    #[error("invalid indexed provenance: {0}")]
    InvalidProjection(String),
}

pub type LbugIndexError = ProjectionIndexError<LbugStorageError>;

pub struct LbugStorage {
    database: Database,
    _temporary: Option<tempfile::TempDir>,
}

impl LbugStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LbugStorageError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        Self::from_database(Database::new(path, SystemConfig::default())?, None)
    }

    pub fn in_memory() -> Result<Self, LbugStorageError> {
        Self::from_database(Database::in_memory(SystemConfig::default())?, None)
    }

    fn from_database(
        database: Database,
        temporary: Option<tempfile::TempDir>,
    ) -> Result<Self, LbugStorageError> {
        let storage = Self {
            database,
            _temporary: temporary,
        };
        ensure_schema(&storage.connection()?)?;
        Ok(storage)
    }

    fn connection(&self) -> Result<Connection<'_>, LbugStorageError> {
        Ok(Connection::new(&self.database)?)
    }
}

impl<M> ProjectionStorage<M> for LbugStorage
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = LbugStorageError;

    fn replace(&mut self, state: &State<M>) -> Result<(), Self::Error> {
        let connection = self.connection()?;
        connection.query("BEGIN TRANSACTION;")?;
        let result = clear_transaction(&connection).and_then(|()| write_state(&connection, state));
        match result {
            Ok(()) => {
                connection.query("COMMIT;")?;
                Ok(())
            }
            Err(error) => {
                let _ = connection.query("ROLLBACK;");
                Err(error)
            }
        }
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        let connection = self.connection()?;
        connection.query("BEGIN TRANSACTION;")?;
        match clear_transaction(&connection) {
            Ok(()) => {
                connection.query("COMMIT;")?;
                Ok(())
            }
            Err(error) => {
                let _ = connection.query("ROLLBACK;");
                Err(error)
            }
        }
    }

    fn operation_count(&self) -> Result<usize, Self::Error> {
        let connection = self.connection()?;
        let mut rows = connection.query("MATCH (n:operation_index) RETURN count(n);")?;
        let row = rows.next().ok_or_else(|| {
            LbugStorageError::InvalidProjection("operation count returned no row".into())
        })?;
        match row.into_iter().next() {
            Some(Value::Int64(count)) if count >= 0 => Ok(count as usize),
            Some(Value::UInt64(count)) => Ok(count as usize),
            value => Err(LbugStorageError::InvalidProjection(format!(
                "operation count returned {value:?}"
            ))),
        }
    }

    fn contains(&self, operation_id: &OperationId<M>) -> Result<bool, Self::Error> {
        let key = encode_key(operation_id)?;
        Ok(!query_strings(
            &self.connection()?,
            &format!(
                "MATCH (n:operation_index) WHERE n.operation_id = {} RETURN n.operation_id;",
                literal(&key)
            ),
        )?
        .is_empty())
    }

    fn entity_observation(
        &self,
        entity: &EntityRef<M>,
    ) -> Result<Option<EntityObservation<M>>, Self::Error> {
        let key = encode_key(entity)?;
        let values = query_strings(
            &self.connection()?,
            &format!(
                "MATCH (n:entity_index) WHERE n.entity_key = {} RETURN n.observation;",
                literal(&key)
            ),
        )?;
        values
            .first()
            .map(|value| decode_hex(value))
            .transpose()?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn schema(&self, key: &SchemaKey) -> Result<Option<SchemaDefinition>, Self::Error> {
        let key = encode_key(key)?;
        let values = query_strings(
            &self.connection()?,
            &format!(
                "MATCH (n:schema_index) WHERE n.schema_key = {} RETURN n.schema;",
                literal(&key)
            ),
        )?;
        values
            .first()
            .map(|value| decode_hex(value))
            .transpose()?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn history(&self, entity: &EntityRef<M>) -> Result<HistoryRows<M>, Self::Error> {
        let key = encode_key(entity)?;
        let connection = self.connection()?;
        let mut history = HistoryRows::default();
        for value in query_strings(
            &connection,
            &format!(
                "MATCH (n:entity_history_index) WHERE n.entity_key = {} RETURN n.operation_id;",
                literal(&key)
            ),
        )? {
            history.operations.insert(decode_key(&value)?);
        }
        let event_rows = query_pairs(
            &connection,
            &format!(
                "MATCH (x:event_entity_index), (e:event_index) WHERE x.entity_key = {} AND x.event_id = e.event_id RETURN e.event_id, e.operation_id;",
                literal(&key)
            ),
        )?;
        for (event_id, operation_id) in event_rows {
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
        let condition = match direction {
            provenance_core::Direction::Outgoing => {
                format!("a.entity_key = {}", literal(&key))
            }
            provenance_core::Direction::Incoming => {
                format!("b.entity_key = {}", literal(&key))
            }
            provenance_core::Direction::Both => format!(
                "a.entity_key = {} OR b.entity_key = {}",
                literal(&key),
                literal(&key)
            ),
        };
        let values = query_strings(
            &self.connection()?,
            &format!(
                "MATCH (a:graph_node)-[r:graph_edge]->(b:graph_node) WHERE {condition} RETURN r.edge;"
            ),
        )?;
        values
            .iter()
            .map(|value| decode_hex(value).and_then(|bytes| decode(&bytes)))
            .collect()
    }
}

pub struct LbugIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<LbugStorage, M>,
}

impl<M> LbugIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LbugIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                LbugStorage::open(path).map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }

    pub fn in_memory() -> Result<Self, LbugIndexError> {
        Ok(Self {
            inner: ProjectionIndex::new(
                LbugStorage::in_memory().map_err(ProjectionIndexError::Storage)?,
            ),
        })
    }
}

impl<M> Deref for LbugIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<LbugStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for LbugIndex<M>
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

impl<M> provenance_core::QueryEngine<M> for LbugIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = LbugIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for LbugIndex<M>
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

fn ensure_schema(connection: &Connection<'_>) -> Result<(), LbugStorageError> {
    let mut tables = BTreeSet::new();
    let mut result = connection.query("CALL SHOW_TABLES() RETURN name;")?;
    for row in &mut result {
        if let Some(Value::String(name)) = row.into_iter().next() {
            tables.insert(name.to_ascii_lowercase());
        }
    }

    for (name, ddl) in [
        (
            "operation_index",
            "CREATE NODE TABLE operation_index(operation_id STRING, operation STRING, parent_count INT64, PRIMARY KEY(operation_id));",
        ),
        (
            "operation_parent_index",
            "CREATE NODE TABLE operation_parent_index(row_key STRING, operation_id STRING, parent_id STRING, PRIMARY KEY(row_key));",
        ),
        (
            "source_index",
            "CREATE NODE TABLE source_index(source STRING, operation_id STRING, PRIMARY KEY(source));",
        ),
        (
            "schema_index",
            "CREATE NODE TABLE schema_index(schema_key STRING, schema STRING, PRIMARY KEY(schema_key));",
        ),
        (
            "named_query_index",
            "CREATE NODE TABLE named_query_index(query_key STRING, query STRING, PRIMARY KEY(query_key));",
        ),
        (
            "entity_index",
            "CREATE NODE TABLE entity_index(entity_key STRING, observation STRING, PRIMARY KEY(entity_key));",
        ),
        (
            "entity_history_index",
            "CREATE NODE TABLE entity_history_index(row_key STRING, entity_key STRING, operation_id STRING, PRIMARY KEY(row_key));",
        ),
        (
            "event_index",
            "CREATE NODE TABLE event_index(event_id STRING, event STRING, operation_id STRING, PRIMARY KEY(event_id));",
        ),
        (
            "event_entity_index",
            "CREATE NODE TABLE event_entity_index(row_key STRING, event_id STRING, entity_key STRING, PRIMARY KEY(row_key));",
        ),
        (
            "graph_node",
            "CREATE NODE TABLE graph_node(entity_key STRING, PRIMARY KEY(entity_key));",
        ),
    ] {
        if !tables.contains(name) {
            connection.query(ddl)?;
        }
    }
    if !tables.contains("graph_edge") {
        connection.query(
            "CREATE REL TABLE graph_edge(FROM graph_node TO graph_node, edge_key STRING, edge STRING);",
        )?;
    }
    Ok(())
}

fn clear_transaction(connection: &Connection<'_>) -> Result<(), LbugStorageError> {
    connection.query("MATCH (a:graph_node)-[r:graph_edge]->(b:graph_node) DELETE r;")?;
    for table in NODE_TABLES {
        connection.query(&format!("MATCH (n:{table}) DELETE n;"))?;
    }
    Ok(())
}

fn write_state<M>(connection: &Connection<'_>, state: &State<M>) -> Result<(), LbugStorageError>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    for (operation_id, operation) in state.operations.iter() {
        let operation_key = encode_key(operation_id)?;
        create_node(
            connection,
            "operation_index",
            &[
                ("operation_id", operation_key.clone()),
                ("operation", encode_hex(&encode(operation)?)),
                ("parent_count", operation.parents.len().to_string()),
            ],
        )?;
        for parent_id in &operation.parents {
            let parent_key = encode_key(parent_id)?;
            create_node(
                connection,
                "operation_parent_index",
                &[
                    ("row_key", format!("{operation_key}:{parent_key}")),
                    ("operation_id", operation_key.clone()),
                    ("parent_id", parent_key),
                ],
            )?;
        }
    }

    for (source, operation_id) in state.projection.source_to_operation.iter() {
        create_node(
            connection,
            "source_index",
            &[
                ("source", encode_key(source)?),
                ("operation_id", encode_key(operation_id)?),
            ],
        )?;
    }
    for (schema_key, schema) in state.projection.schemas.iter() {
        create_node(
            connection,
            "schema_index",
            &[
                ("schema_key", encode_key(schema_key)?),
                ("schema", encode_hex(&encode(schema)?)),
            ],
        )?;
    }
    for (query_key, query) in state.projection.named_queries.iter() {
        create_node(
            connection,
            "named_query_index",
            &[
                ("query_key", encode_key(query_key)?),
                ("query", encode_hex(&encode(query)?)),
            ],
        )?;
    }
    for (entity, observation) in state.projection.entities.iter() {
        create_node(
            connection,
            "entity_index",
            &[
                (
                    "entity_key",
                    encode_key(&EntityRef::External(entity.clone()))?,
                ),
                ("observation", encode_hex(&encode(observation)?)),
            ],
        )?;
    }
    for (entity, operation_ids) in state.projection.entity_history.iter() {
        let entity_key = encode_key(&EntityRef::External(entity.clone()))?;
        for operation_id in operation_ids {
            let operation_key = encode_key(operation_id)?;
            create_node(
                connection,
                "entity_history_index",
                &[
                    ("row_key", format!("{entity_key}:{operation_key}")),
                    ("entity_key", entity_key.clone()),
                    ("operation_id", operation_key),
                ],
            )?;
        }
    }

    let event_operations = event_operations(state);
    for (event_id, event) in state.projection.events.iter() {
        let event_key = encode_key(event_id)?;
        let operation_id = event_operations.get(event_id).ok_or_else(|| {
            LbugStorageError::InvalidProjection(format!(
                "event {:?} has no containing operation",
                event_id
            ))
        })?;
        create_node(
            connection,
            "event_index",
            &[
                ("event_id", event_key.clone()),
                ("event", encode_hex(&encode(event)?)),
                ("operation_id", encode_key(operation_id)?),
            ],
        )?;
        let mut entities = event.subjects.clone();
        for relation in &event.relations {
            entities.insert(relation.from.clone());
            entities.insert(relation.to.clone());
        }
        for entity in entities {
            let entity_key = encode_key(&entity)?;
            create_node(
                connection,
                "event_entity_index",
                &[
                    ("row_key", format!("{event_key}:{entity_key}")),
                    ("event_id", event_key.clone()),
                    ("entity_key", entity_key),
                ],
            )?;
        }
    }

    let mut graph_nodes = BTreeSet::new();
    for (from, edges) in state.projection.outgoing.iter() {
        graph_nodes.insert(from.clone());
        for edge in edges {
            graph_nodes.insert(edge.relation.to.clone());
        }
    }
    for entity in graph_nodes {
        create_node(
            connection,
            "graph_node",
            &[("entity_key", encode_key(&entity)?)],
        )?;
    }
    for (from, edges) in state.projection.outgoing.iter() {
        let from_key = encode_key(from)?;
        for edge in edges {
            let to_key = encode_key(&edge.relation.to)?;
            let edge_key = encode_key(edge)?;
            let query = format!(
                "MATCH (a:graph_node), (b:graph_node) WHERE a.entity_key = {} AND b.entity_key = {} CREATE (a)-[:graph_edge {{edge_key: {}, edge: {}}}]->(b);",
                literal(&from_key),
                literal(&to_key),
                literal(&edge_key),
                literal(&encode_hex(&encode(edge)?)),
            );
            connection.query(&query)?;
        }
    }
    Ok(())
}

fn create_node(
    connection: &Connection<'_>,
    table: &str,
    properties: &[(&str, String)],
) -> Result<(), LbugStorageError> {
    let properties = properties
        .iter()
        .map(|(name, value)| format!("{name}: {}", literal(value)))
        .collect::<Vec<_>>()
        .join(", ");
    connection.query(&format!("CREATE (:{table} {{{properties}}});"))?;
    Ok(())
}

fn query_strings(
    connection: &Connection<'_>,
    query: &str,
) -> Result<Vec<String>, LbugStorageError> {
    let mut result = connection.query(query)?;
    let mut values = Vec::new();
    for row in &mut result {
        if let Some(value) = row.into_iter().next() {
            values.push(string_value(value)?);
        }
    }
    Ok(values)
}

fn query_pairs(
    connection: &Connection<'_>,
    query: &str,
) -> Result<Vec<(String, String)>, LbugStorageError> {
    let mut result = connection.query(query)?;
    let mut values = Vec::new();
    for row in &mut result {
        if row.len() != 2 {
            return Err(LbugStorageError::InvalidProjection(
                "LadybugDB pair query returned the wrong number of columns".into(),
            ));
        }
        values.push((string_value(row[0].clone())?, string_value(row[1].clone())?));
    }
    Ok(values)
}

fn string_value(value: Value) -> Result<String, LbugStorageError> {
    match value {
        Value::String(value) => Ok(value),
        value => Err(LbugStorageError::InvalidProjection(format!(
            "expected STRING value, got {value:?}"
        ))),
    }
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, LbugStorageError> {
    postcard::to_allocvec(value).map_err(LbugStorageError::Encode)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, LbugStorageError> {
    postcard::from_bytes(bytes).map_err(LbugStorageError::Decode)
}

fn encode_key<T: Serialize>(value: &T) -> Result<String, LbugStorageError> {
    Ok(encode_hex(&encode(value)?))
}

fn decode_key<T: DeserializeOwned>(value: &str) -> Result<T, LbugStorageError> {
    let bytes = decode_hex(value)?;
    decode(&bytes)
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

fn decode_hex(value: &str) -> Result<Vec<u8>, LbugStorageError> {
    if value.len() % 2 != 0 {
        return Err(LbugStorageError::InvalidProjection(
            "encoded LadybugDB value has odd length".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let value = value.as_bytes();
    for index in (0..value.len()).step_by(2) {
        let high = hex_digit(value[index]).ok_or_else(|| {
            LbugStorageError::InvalidProjection("invalid hexadecimal LadybugDB value".into())
        })?;
        let low = hex_digit(value[index + 1]).ok_or_else(|| {
            LbugStorageError::InvalidProjection("invalid hexadecimal LadybugDB value".into())
        })?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
