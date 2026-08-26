//! Mnestic implementation of the ordered key/value projection contract.
//!
//! Mnestic exposes a relational Datalog API. The projection row index only
//! needs an atomic key/value surface, so this adapter stores the portable row
//! layout in one keyed Mnestic relation and reuses the shared KV projection
//! implementation for provenance semantics.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};
use provenance_core::{Operation, OperationId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use super::kv::{KvBackend, KvProjectionStorage, KvStorageError};
use crate::{IndexModel, ProjectionBackend, ProjectionIndex, ProjectionIndexError};

const RELATION: &str = "provenance_projection";
const CREATE_RELATION: &str = ":create provenance_projection { key: Bytes => value: Bytes }";
const SCAN_RELATION: &str = "?[key, value] := *provenance_projection{key, value}";
const LOOKUP_RELATION: &str = "?[value] := *provenance_projection{key, value}, key == $key";

/// Low-level Mnestic errors normalized for the projection storage contract.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct MnesticBackendError(String);

impl MnesticBackendError {
    fn from_display(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

/// Persistent Mnestic database used by [`MnesticStorage`].
pub struct MnesticBackend {
    database: DbInstance,
}

impl MnesticBackend {
    /// Open a disk-backed Mnestic projection database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MnesticBackendError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(MnesticBackendError::from_display)?;
        }
        let database =
            DbInstance::new("rocksdb", path, "{}").map_err(MnesticBackendError::from_display)?;
        Self::from_database(database)
    }

    /// Open an in-memory Mnestic projection database.
    pub fn in_memory() -> Result<Self, MnesticBackendError> {
        let database =
            DbInstance::new("mem", "", "{}").map_err(MnesticBackendError::from_display)?;
        Self::from_database(database)
    }

    fn from_database(database: DbInstance) -> Result<Self, MnesticBackendError> {
        let backend = Self { database };
        backend.ensure_relation()?;
        Ok(backend)
    }

    fn execute(
        &self,
        script: &str,
        params: BTreeMap<String, DataValue>,
        mutability: ScriptMutability,
    ) -> Result<NamedRows, MnesticBackendError> {
        self.database
            .run_script(script, params, mutability)
            .map_err(MnesticBackendError::from_display)
    }

    fn ensure_relation(&self) -> Result<(), MnesticBackendError> {
        let relations =
            self.execute("::relations", BTreeMap::new(), ScriptMutability::Immutable)?;
        let exists = relations.rows.iter().any(|row| {
            row.first()
                .is_some_and(|value| value == &DataValue::from(RELATION))
        });
        if !exists {
            self.execute(CREATE_RELATION, BTreeMap::new(), ScriptMutability::Mutable)?;
        }
        Ok(())
    }

    fn mutate(
        &self,
        entries: &[(Vec<u8>, Vec<u8>)],
        operation: &str,
    ) -> Result<(), MnesticBackendError> {
        let data = DataValue::List(
            entries
                .iter()
                .map(|(key, value)| {
                    DataValue::List(vec![
                        DataValue::Bytes(key.clone()),
                        DataValue::Bytes(value.clone()),
                    ])
                })
                .collect(),
        );
        let params = BTreeMap::from([(String::from("data"), data)]);
        let script = format!("?[key, value] <- $data {operation} {RELATION} {{ key, value }}");
        self.execute(&script, params, ScriptMutability::Mutable)?;
        Ok(())
    }

    fn bytes_from_row(row: &[DataValue], index: usize) -> Result<Vec<u8>, MnesticBackendError> {
        match row.get(index) {
            Some(DataValue::Bytes(value)) => Ok(value.clone()),
            value => Err(MnesticBackendError::from_display(format!(
                "Mnestic projection row column {index} is not bytes: {value:?}"
            ))),
        }
    }
}

impl KvBackend for MnesticBackend {
    type Error = MnesticBackendError;

    fn replace_entries(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Self::Error> {
        self.mutate(entries, ":replace")
    }

    fn apply_entries(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Self::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        self.mutate(entries, ":put")
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.mutate(&[], ":replace")
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let params = BTreeMap::from([(String::from("key"), DataValue::Bytes(key.to_vec()))]);
        let rows = self.execute(LOOKUP_RELATION, params, ScriptMutability::Immutable)?;
        match rows.rows.as_slice() {
            [] => Ok(None),
            [row] => Self::bytes_from_row(row, 0).map(Some),
            _ => Err(MnesticBackendError::from_display(
                "Mnestic projection lookup returned duplicate keys",
            )),
        }
    }

    fn entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let rows = self.execute(SCAN_RELATION, BTreeMap::new(), ScriptMutability::Immutable)?;
        rows.rows
            .iter()
            .map(|row| Ok((Self::bytes_from_row(row, 0)?, Self::bytes_from_row(row, 1)?)))
            .collect()
    }
}

pub type MnesticStorageError = KvStorageError<MnesticBackendError>;
pub type MnesticStorage = KvProjectionStorage<MnesticBackend>;
pub type MnesticIndexError = ProjectionIndexError<MnesticStorageError>;

impl MnesticStorage {
    /// Open a persistent Mnestic projection storage.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MnesticStorageError> {
        MnesticBackend::open(path)
            .map(Self::new)
            .map_err(KvStorageError::Backend)
    }

    /// Open an in-memory Mnestic projection storage.
    pub fn in_memory() -> Result<Self, MnesticStorageError> {
        MnesticBackend::in_memory()
            .map(Self::new)
            .map_err(KvStorageError::Backend)
    }
}

/// Generic projection index backed by Mnestic.
pub struct MnesticIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<MnesticStorage, M>,
}

impl<M> MnesticIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    /// Open a persistent Mnestic projection index.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MnesticIndexError> {
        let storage = MnesticStorage::open(path).map_err(ProjectionIndexError::Storage)?;
        Ok(Self {
            inner: ProjectionIndex::new(storage),
        })
    }

    /// Open an in-memory Mnestic projection index.
    pub fn in_memory() -> Result<Self, MnesticIndexError> {
        let storage = MnesticStorage::in_memory().map_err(ProjectionIndexError::Storage)?;
        Ok(Self {
            inner: ProjectionIndex::new(storage),
        })
    }
}

impl<M> Deref for MnesticIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<MnesticStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for MnesticIndex<M>
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

impl<M> provenance_core::QueryEngine<M> for MnesticIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = MnesticIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for MnesticIndex<M>
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
        T::Error: Display,
    {
        self.inner.rebuild_from(store)
    }

    fn update_from<T>(&mut self, store: &T) -> Result<(), Self::Error>
    where
        T: provenance_core::ProvenanceStore<M>,
        T::Error: Display,
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

#[cfg(test)]
mod tests {
    use super::super::kv::KvBackend;
    use super::*;

    #[test]
    fn arbitrary_rows_round_trip_through_mnestic() {
        let mut backend = MnesticBackend::in_memory().expect("open Mnestic backend");
        let entries = vec![
            (vec![0, 1, 255], vec![9, 8, 7]),
            (vec![b'e', 0, b'n', 1], vec![0, 255]),
        ];
        backend
            .replace_entries(&entries)
            .expect("replace Mnestic rows");

        for (key, value) in &entries {
            assert_eq!(
                backend.get(key).expect("read Mnestic row"),
                Some(value.clone())
            );
        }
        let mut actual = backend.entries().expect("enumerate Mnestic rows");
        actual.sort();
        let mut expected = entries;
        expected.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn replacement_and_incremental_updates_are_atomic_at_the_row_surface() {
        let mut backend = MnesticBackend::in_memory().expect("open Mnestic backend");
        backend
            .replace_entries(&[(b"old".to_vec(), b"value".to_vec())])
            .expect("seed Mnestic rows");
        backend
            .apply_entries(&[(b"new".to_vec(), b"next".to_vec())])
            .expect("apply Mnestic rows");
        assert_eq!(
            backend.get(b"old").expect("read old row"),
            Some(b"value".to_vec())
        );
        assert_eq!(
            backend.get(b"new").expect("read new row"),
            Some(b"next".to_vec())
        );

        backend
            .replace_entries(&[(b"replacement".to_vec(), b"only".to_vec())])
            .expect("replace Mnestic rows");
        assert_eq!(backend.get(b"old").expect("read removed row"), None);
        assert_eq!(
            backend.get(b"replacement").expect("read replacement row"),
            Some(b"only".to_vec())
        );
    }
}
