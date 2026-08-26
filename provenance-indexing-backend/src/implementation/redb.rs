//! `redb` projection storage.

use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use provenance_core::{Operation, OperationId};
use redb::{
    Database, MultimapTableDefinition, ReadableDatabase, ReadableMultimapTable, ReadableTable,
    TableDefinition,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tempfile::TempDir;
use thiserror::Error;

use super::kv::{KvBackend, KvProjectionStorage, KvStorageError};
use crate::{IndexModel, ProjectionBackend, ProjectionIndex, ProjectionIndexError};

const PROJECTION_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("projection");
/// Fan-out rows use redb's native multimap table; other rows stay key/value.
const PROJECTION_MULTIMAP_TABLE: MultimapTableDefinition<&[u8], &[u8]> =
    MultimapTableDefinition::new("projection_multimap");

/// Low-level redb errors normalized for the projection storage contract.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct RedbBackendError(String);

impl RedbBackendError {
    fn from_display(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

/// Persistent redb key/value store used by [`RedbStorage`].
pub struct RedbBackend {
    database: Database,
    _temporary_directory: Option<TempDir>,
}

impl RedbBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RedbBackendError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(RedbBackendError::from_display)?;
        }
        let database = Database::create(path).map_err(RedbBackendError::from_display)?;
        let backend = Self {
            database,
            _temporary_directory: None,
        };
        backend.ensure_table()?;
        Ok(backend)
    }

    pub fn in_memory() -> Result<Self, RedbBackendError> {
        let temporary_directory = tempfile::tempdir().map_err(RedbBackendError::from_display)?;
        let path = temporary_directory.path().join("projection.redb");
        let database = Database::create(&path).map_err(RedbBackendError::from_display)?;
        let backend = Self {
            database,
            _temporary_directory: Some(temporary_directory),
        };
        backend.ensure_table()?;
        Ok(backend)
    }

    fn ensure_table(&self) -> Result<(), RedbBackendError> {
        let transaction = self
            .database
            .begin_write()
            .map_err(RedbBackendError::from_display)?;
        {
            transaction
                .open_table(PROJECTION_TABLE)
                .map_err(RedbBackendError::from_display)?;
            transaction
                .open_multimap_table(PROJECTION_MULTIMAP_TABLE)
                .map_err(RedbBackendError::from_display)?;
        }
        transaction.commit().map_err(RedbBackendError::from_display)
    }
}

impl KvBackend for RedbBackend {
    type Error = RedbBackendError;

    fn replace_entries(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Self::Error> {
        let transaction = self
            .database
            .begin_write()
            .map_err(RedbBackendError::from_display)?;
        {
            let mut table = transaction
                .open_table(PROJECTION_TABLE)
                .map_err(RedbBackendError::from_display)?;
            let mut multimap = transaction
                .open_multimap_table(PROJECTION_MULTIMAP_TABLE)
                .map_err(RedbBackendError::from_display)?;
            table
                .retain(|_, _| false)
                .map_err(RedbBackendError::from_display)?;
            clear_multimap(&mut multimap)?;
            for (key, value) in entries {
                if let Some((logical_key, suffix)) = multimap_parts(key) {
                    let encoded_value = encode_multimap_value(suffix, value);
                    multimap
                        .insert(logical_key, encoded_value.as_slice())
                        .map_err(RedbBackendError::from_display)?;
                } else {
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .map_err(RedbBackendError::from_display)?;
                }
            }
        }
        transaction.commit().map_err(RedbBackendError::from_display)
    }

    fn apply_entries(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Self::Error> {
        let transaction = self
            .database
            .begin_write()
            .map_err(RedbBackendError::from_display)?;
        {
            let mut table = transaction
                .open_table(PROJECTION_TABLE)
                .map_err(RedbBackendError::from_display)?;
            let mut multimap = transaction
                .open_multimap_table(PROJECTION_MULTIMAP_TABLE)
                .map_err(RedbBackendError::from_display)?;
            for (key, value) in entries {
                if let Some((logical_key, suffix)) = multimap_parts(key) {
                    let encoded_value = encode_multimap_value(suffix, value);
                    multimap
                        .insert(logical_key, encoded_value.as_slice())
                        .map_err(RedbBackendError::from_display)?;
                } else {
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .map_err(RedbBackendError::from_display)?;
                }
            }
        }
        transaction.commit().map_err(RedbBackendError::from_display)
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.replace_entries(&[])
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let transaction = self
            .database
            .begin_read()
            .map_err(RedbBackendError::from_display)?;
        let table = transaction
            .open_table(PROJECTION_TABLE)
            .map_err(RedbBackendError::from_display)?;
        if let Some(value) = table.get(key).map_err(RedbBackendError::from_display)? {
            return Ok(Some(value.value().to_vec()));
        }

        let Some((logical_key, suffix)) = multimap_parts(key) else {
            return Ok(None);
        };
        let multimap = transaction
            .open_multimap_table(PROJECTION_MULTIMAP_TABLE)
            .map_err(RedbBackendError::from_display)?;
        let mut values = multimap
            .get(logical_key)
            .map_err(RedbBackendError::from_display)?;
        while let Some(value) = values.next() {
            let value = value.map_err(RedbBackendError::from_display)?;
            let (stored_suffix, payload) = decode_multimap_value(value.value())
                .ok_or_else(|| RedbBackendError("invalid multimap value".to_owned()))?;
            if stored_suffix == suffix {
                return Ok(Some(payload.to_vec()));
            }
        }
        Ok(None)
    }

    fn entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let transaction = self
            .database
            .begin_read()
            .map_err(RedbBackendError::from_display)?;
        let table = transaction
            .open_table(PROJECTION_TABLE)
            .map_err(RedbBackendError::from_display)?;
        let mut entries = Vec::new();
        let mut iterator = table.iter().map_err(RedbBackendError::from_display)?;
        while let Some(item) = iterator.next() {
            let (key, value) = item.map_err(RedbBackendError::from_display)?;
            entries.push((key.value().to_vec(), value.value().to_vec()));
        }

        let multimap = transaction
            .open_multimap_table(PROJECTION_MULTIMAP_TABLE)
            .map_err(RedbBackendError::from_display)?;
        let mut iterator = multimap.iter().map_err(RedbBackendError::from_display)?;
        while let Some(item) = iterator.next() {
            let (logical_key, mut values) = item.map_err(RedbBackendError::from_display)?;
            while let Some(value) = values.next() {
                let value = value.map_err(RedbBackendError::from_display)?;
                let (suffix, payload) = decode_multimap_value(value.value())
                    .ok_or_else(|| RedbBackendError("invalid multimap value".to_owned()))?;
                let mut key = logical_key.value().to_vec();
                append_component(&mut key, suffix);
                entries.push((key, payload.to_vec()));
            }
        }
        Ok(entries)
    }
}

fn clear_multimap(
    multimap: &mut redb::MultimapTable<'_, &[u8], &[u8]>,
) -> Result<(), RedbBackendError> {
    let mut keys = Vec::new();
    let mut iterator = multimap.iter().map_err(RedbBackendError::from_display)?;
    while let Some(item) = iterator.next() {
        let (key, _) = item.map_err(RedbBackendError::from_display)?;
        keys.push(key.value().to_vec());
    }
    drop(iterator);
    for key in keys {
        let mut values = multimap
            .remove_all(key.as_slice())
            .map_err(RedbBackendError::from_display)?;
        while let Some(value) = values.next() {
            value.map_err(RedbBackendError::from_display)?;
        }
    }
    Ok(())
}

fn multimap_parts(key: &[u8]) -> Option<(&[u8], &[u8])> {
    if !matches!(key.first(), Some(b'H' | b'x' | b'g' | b'G')) {
        return None;
    }
    let first_end = component_end(key, 1)?;
    let second_start = first_end;
    let second_end = component_end(key, second_start)?;
    if second_end != key.len() {
        return None;
    }
    Some((&key[..first_end], &key[second_start + 4..second_end]))
}

fn component_end(bytes: &[u8], start: usize) -> Option<usize> {
    let length = u32::from_be_bytes(bytes.get(start..start + 4)?.try_into().ok()?) as usize;
    let end = start.checked_add(4)?.checked_add(length)?;
    (end <= bytes.len()).then_some(end)
}

fn encode_multimap_value(suffix: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(4 + suffix.len() + payload.len());
    value.extend_from_slice(&(suffix.len() as u32).to_be_bytes());
    value.extend_from_slice(suffix);
    value.extend_from_slice(payload);
    value
}

fn decode_multimap_value(value: &[u8]) -> Option<(&[u8], &[u8])> {
    let suffix_length = u32::from_be_bytes(value.get(..4)?.try_into().ok()?) as usize;
    let suffix_start: usize = 4;
    let suffix_end = suffix_start.checked_add(suffix_length)?;
    (suffix_end <= value.len()).then_some((&value[suffix_start..suffix_end], &value[suffix_end..]))
}

fn append_component(key: &mut Vec<u8>, component: &[u8]) {
    key.extend_from_slice(&(component.len() as u32).to_be_bytes());
    key.extend_from_slice(component);
}

pub type RedbStorageError = KvStorageError<RedbBackendError>;
pub type RedbStorage = KvProjectionStorage<RedbBackend>;
pub type RedbIndexError = ProjectionIndexError<RedbStorageError>;
impl RedbStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RedbStorageError> {
        RedbBackend::open(path)
            .map(Self::new)
            .map_err(KvStorageError::Backend)
    }

    pub fn in_memory() -> Result<Self, RedbStorageError> {
        RedbBackend::in_memory()
            .map(Self::new)
            .map_err(KvStorageError::Backend)
    }
}

/// Generic projection index backed by redb.
pub struct RedbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<RedbStorage, M>,
}

impl<M> RedbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RedbIndexError> {
        let storage = RedbBackend::open(path)
            .map(RedbStorage::new)
            .map_err(|error| ProjectionIndexError::Storage(KvStorageError::Backend(error)))?;
        Ok(Self {
            inner: ProjectionIndex::new(storage),
        })
    }

    pub fn in_memory() -> Result<Self, RedbIndexError> {
        let storage = RedbBackend::in_memory()
            .map(RedbStorage::new)
            .map_err(|error| ProjectionIndexError::Storage(KvStorageError::Backend(error)))?;
        Ok(Self {
            inner: ProjectionIndex::new(storage),
        })
    }
}

impl<M> Deref for RedbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<RedbStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for RedbIndex<M>
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

impl<M> provenance_core::QueryEngine<M> for RedbIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = RedbIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for RedbIndex<M>
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

    fn row(tag: u8, logical: &[u8], suffix: &[u8]) -> Vec<u8> {
        let mut key = vec![tag];
        append_component(&mut key, logical);
        append_component(&mut key, suffix);
        key
    }

    #[test]
    fn native_multimap_rows_round_trip_through_kv_surface() {
        let mut backend = RedbBackend::in_memory().expect("open redb backend");
        let entries = vec![
            (row(b'H', b"entity", b"operation-1"), b"history".to_vec()),
            (row(b'x', b"entity", b"event-1"), b"event".to_vec()),
            (row(b'g', b"entity", b"edge-1"), b"outgoing".to_vec()),
            (row(b'G', b"entity", b"edge-2"), b"incoming".to_vec()),
            (row(b'o', b"operation", b"1"), b"normal".to_vec()),
        ];
        backend.replace_entries(&entries).expect("write rows");

        for (key, expected) in &entries {
            assert_eq!(backend.get(key).expect("read row"), Some(expected.clone()));
        }
        let mut actual = backend.entries().expect("enumerate rows");
        actual.sort();
        let mut expected = entries;
        expected.sort();
        assert_eq!(actual, expected);
    }
}
