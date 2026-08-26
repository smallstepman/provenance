//! LMDB projection storage through the safe `heed` wrapper.

use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use heed::types::Bytes;
use heed::{Database, DatabaseFlags, Env, EnvOpenOptions, RoTxn, RwTxn, WithoutTls};
use provenance_core::{Operation, OperationId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use thiserror::Error;

use super::kv::{KvBackend, KvProjectionStorage, KvStorageError};
use crate::{IndexModel, ProjectionBackend, ProjectionIndex, ProjectionIndexError};

/// Low-level heed/LMDB errors normalized for the projection storage contract.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct HeedBackendError(String);

impl HeedBackendError {
    fn from_display(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

/// Persistent LMDB environment used by [`HeedStorage`].
pub struct HeedBackend {
    environment: Env<WithoutTls>,
    database: Database<Bytes, Bytes>,
    /// Native `DUP_SORT` fan-out for rows that fit LMDB's key/value limit.
    duplicate_database: Database<Bytes, Bytes>,
    /// Hashed, chunked rows for keys or values too large for LMDB's native layout.
    overflow_database: Database<Bytes, Bytes>,
    _temporary_directory: Option<TempDir>,
}

impl HeedBackend {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HeedBackendError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(HeedBackendError::from_display)?;
        Self::open_directory(path, None)
    }

    pub fn in_memory() -> Result<Self, HeedBackendError> {
        let temporary_directory = tempfile::tempdir().map_err(HeedBackendError::from_display)?;
        let path = temporary_directory.path().to_path_buf();
        Self::open_directory(&path, Some(temporary_directory))
    }

    fn open_directory(
        path: &Path,
        temporary_directory: Option<TempDir>,
    ) -> Result<Self, HeedBackendError> {
        let mut options = EnvOpenOptions::new().read_txn_without_tls();
        options.map_size(256 * 1024 * 1024).max_dbs(3);
        let environment = unsafe { options.open(path) }.map_err(HeedBackendError::from_display)?;
        let mut transaction = environment
            .write_txn()
            .map_err(HeedBackendError::from_display)?;
        let database = environment
            .database_options()
            .types::<Bytes, Bytes>()
            .name("projection")
            .create(&mut transaction)
            .map_err(HeedBackendError::from_display)?;
        let duplicate_database = environment
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT)
            .name("projection_duplicates")
            .create(&mut transaction)
            .map_err(HeedBackendError::from_display)?;
        let overflow_database = environment
            .database_options()
            .types::<Bytes, Bytes>()
            .name("projection_overflow")
            .create(&mut transaction)
            .map_err(HeedBackendError::from_display)?;
        transaction
            .commit()
            .map_err(HeedBackendError::from_display)?;
        Ok(Self {
            environment,
            database,
            duplicate_database,
            overflow_database,
            _temporary_directory: temporary_directory,
        })
    }
}

impl KvBackend for HeedBackend {
    type Error = HeedBackendError;

    fn replace_entries(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Self::Error> {
        let mut transaction = self
            .environment
            .write_txn()
            .map_err(HeedBackendError::from_display)?;
        self.database
            .clear(&mut transaction)
            .map_err(HeedBackendError::from_display)?;
        self.duplicate_database
            .clear(&mut transaction)
            .map_err(HeedBackendError::from_display)?;
        self.overflow_database
            .clear(&mut transaction)
            .map_err(HeedBackendError::from_display)?;
        for (key, value) in entries {
            self.put_entry(&mut transaction, key, value)?;
        }
        transaction.commit().map_err(HeedBackendError::from_display)
    }

    fn apply_entries(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Self::Error> {
        let mut transaction = self
            .environment
            .write_txn()
            .map_err(HeedBackendError::from_display)?;
        for (key, value) in entries {
            self.delete_entry(&mut transaction, key)?;
            self.put_entry(&mut transaction, key, value)?;
        }
        transaction.commit().map_err(HeedBackendError::from_display)
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.replace_entries(&[])
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let transaction = self
            .environment
            .read_txn()
            .map_err(HeedBackendError::from_display)?;
        if key.len() <= LMDB_MAX_KEY_BYTES {
            if let Some(value) = self
                .database
                .get(&transaction, key)
                .map_err(HeedBackendError::from_display)?
            {
                return Ok(Some(value.to_vec()));
            }
        }

        if let Some((logical_key, suffix)) = duplicate_parts(key)
            && logical_key.len() <= LMDB_MAX_KEY_BYTES
        {
            let Some(mut values) = self
                .duplicate_database
                .get_duplicates(&transaction, logical_key)
                .map_err(HeedBackendError::from_display)?
            else {
                return Ok(self
                    .read_overflow(&transaction, &overflow_digest(key))?
                    .and_then(|(stored_key, payload)| (stored_key == key).then_some(payload)));
            };
            while let Some(item) = values.next() {
                let (_, value) = item.map_err(HeedBackendError::from_display)?;
                let (stored_suffix, payload) = decode_duplicate_value(value)
                    .ok_or_else(|| HeedBackendError("invalid duplicate value".to_owned()))?;
                if stored_suffix == suffix {
                    return Ok(Some(payload.to_vec()));
                }
            }
        }

        Ok(self
            .read_overflow(&transaction, &overflow_digest(key))?
            .and_then(|(stored_key, payload)| (stored_key == key).then_some(payload)))
    }

    fn entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let transaction = self
            .environment
            .read_txn()
            .map_err(HeedBackendError::from_display)?;
        let mut entries = Vec::new();
        let mut iterator = self
            .database
            .iter(&transaction)
            .map_err(HeedBackendError::from_display)?;
        while let Some(item) = iterator.next() {
            let (key, value) = item.map_err(HeedBackendError::from_display)?;
            entries.push((key.to_vec(), value.to_vec()));
        }

        let mut iterator = self
            .duplicate_database
            .iter(&transaction)
            .map_err(HeedBackendError::from_display)?;
        while let Some(item) = iterator.next() {
            let (logical_key, value) = item.map_err(HeedBackendError::from_display)?;
            let (suffix, payload) = decode_duplicate_value(value)
                .ok_or_else(|| HeedBackendError("invalid duplicate value".to_owned()))?;
            let mut key = logical_key.to_vec();
            append_component(&mut key, suffix);
            entries.push((key, payload.to_vec()));
        }

        let mut digests = Vec::new();
        let mut iterator = self
            .overflow_database
            .iter(&transaction)
            .map_err(HeedBackendError::from_display)?;
        while let Some(item) = iterator.next() {
            let (key, _) = item.map_err(HeedBackendError::from_display)?;
            if let Some((digest, chunk)) = overflow_key_parts(key)
                && chunk == 0
            {
                digests.push(digest);
            }
        }
        for digest in digests {
            let Some(entry) = self.read_overflow(&transaction, &digest)? else {
                return Err(HeedBackendError("missing overflow row".to_owned()));
            };
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl HeedBackend {
    fn put_entry(
        &self,
        transaction: &mut RwTxn<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), HeedBackendError> {
        if let Some((logical_key, encoded_value)) = duplicate_entry(key, value) {
            self.duplicate_database
                .put(
                    transaction,
                    logical_key.as_slice(),
                    encoded_value.as_slice(),
                )
                .map_err(HeedBackendError::from_display)?;
        } else if key.len() <= LMDB_MAX_KEY_BYTES && value.len() <= LMDB_MAX_VALUE_BYTES {
            self.database
                .put(transaction, key, value)
                .map_err(HeedBackendError::from_display)?;
        } else {
            for (chunk_key, chunk_value) in overflow_chunks(key, value)? {
                self.overflow_database
                    .put(transaction, chunk_key.as_slice(), chunk_value.as_slice())
                    .map_err(HeedBackendError::from_display)?;
            }
        }
        Ok(())
    }

    fn delete_entry(
        &self,
        transaction: &mut RwTxn<'_>,
        key: &[u8],
    ) -> Result<(), HeedBackendError> {
        if key.len() <= LMDB_MAX_KEY_BYTES {
            self.database
                .delete(transaction, key)
                .map_err(HeedBackendError::from_display)?;
        }

        if let Some((logical_key, suffix)) = duplicate_parts(key)
            && logical_key.len() <= LMDB_MAX_KEY_BYTES
        {
            let mut values = Vec::new();
            if let Some(mut duplicates) = self
                .duplicate_database
                .get_duplicates(&*transaction, logical_key)
                .map_err(HeedBackendError::from_display)?
            {
                while let Some(item) = duplicates.next() {
                    let (_, value) = item.map_err(HeedBackendError::from_display)?;
                    if decode_duplicate_value(value)
                        .is_some_and(|(stored_suffix, _)| stored_suffix == suffix)
                    {
                        values.push(value.to_vec());
                    }
                }
            }
            for value in values {
                self.duplicate_database
                    .delete_one_duplicate(transaction, logical_key, value.as_slice())
                    .map_err(HeedBackendError::from_display)?;
            }
        }

        let prefix = overflow_prefix(&overflow_digest(key));
        let mut iterator = self
            .overflow_database
            .prefix_iter_mut(transaction, prefix.as_slice())
            .map_err(HeedBackendError::from_display)?;
        while iterator
            .next()
            .transpose()
            .map_err(HeedBackendError::from_display)?
            .is_some()
        {
            // The iterator owns the write cursor, so deleting the current row
            // is the only safe way to remove every stale chunk for this key.
            unsafe {
                iterator
                    .del_current()
                    .map_err(HeedBackendError::from_display)?;
            }
        }
        Ok(())
    }

    fn read_overflow(
        &self,
        transaction: &RoTxn<'_, WithoutTls>,
        digest: &[u8; OVERFLOW_DIGEST_BYTES],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, HeedBackendError> {
        let first_key = overflow_chunk_key(digest, 0);
        let Some(first) = self
            .overflow_database
            .get(transaction, first_key.as_slice())
            .map_err(HeedBackendError::from_display)?
        else {
            return Ok(None);
        };
        let (key_len, chunk_count) = overflow_header(&first)?;
        let mut stream = Vec::new();
        if first.len() < OVERFLOW_HEADER_BYTES {
            return Err(HeedBackendError("invalid overflow header".to_owned()));
        }
        stream.extend_from_slice(&first[OVERFLOW_HEADER_BYTES..]);
        for chunk in 1..chunk_count {
            let key = overflow_chunk_key(digest, chunk as u32);
            let Some(value) = self
                .overflow_database
                .get(transaction, key.as_slice())
                .map_err(HeedBackendError::from_display)?
            else {
                return Err(HeedBackendError("missing overflow chunk".to_owned()));
            };
            if value.len() > OVERFLOW_CHUNK_BYTES {
                return Err(HeedBackendError("oversized overflow chunk".to_owned()));
            }
            stream.extend_from_slice(&value);
        }
        if key_len > stream.len() {
            return Err(HeedBackendError(
                "overflow key length exceeds row".to_owned(),
            ));
        }
        let stored_key = stream[..key_len].to_vec();
        if overflow_digest(&stored_key) != *digest {
            return Err(HeedBackendError("overflow digest mismatch".to_owned()));
        }
        Ok(Some((stored_key, stream[key_len..].to_vec())))
    }
}
const LMDB_MAX_KEY_BYTES: usize = 511;
const LMDB_MAX_VALUE_BYTES: usize = 511;
const OVERFLOW_TAG: u8 = b'z';
const OVERFLOW_DIGEST_BYTES: usize = 32;
const OVERFLOW_KEY_BYTES: usize = 1 + OVERFLOW_DIGEST_BYTES + 4;
const OVERFLOW_CHUNK_BYTES: usize = 400;
const OVERFLOW_HEADER_BYTES: usize = 8;

/// Return the stable digest used to address one overflow row.
fn overflow_digest(key: &[u8]) -> [u8; OVERFLOW_DIGEST_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"provenance-indexing-backend/heed/overflow-v1\0");
    hasher.update(key);
    hasher.finalize().into()
}

fn overflow_prefix(digest: &[u8; OVERFLOW_DIGEST_BYTES]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(1 + OVERFLOW_DIGEST_BYTES);
    prefix.push(OVERFLOW_TAG);
    prefix.extend_from_slice(digest);
    prefix
}

fn overflow_chunk_key(digest: &[u8; OVERFLOW_DIGEST_BYTES], chunk: u32) -> Vec<u8> {
    let mut key = overflow_prefix(digest);
    key.extend_from_slice(&chunk.to_be_bytes());
    key
}

fn overflow_key_parts(key: &[u8]) -> Option<([u8; OVERFLOW_DIGEST_BYTES], u32)> {
    if key.len() != OVERFLOW_KEY_BYTES || key.first().copied() != Some(OVERFLOW_TAG) {
        return None;
    }
    let digest = key[1..1 + OVERFLOW_DIGEST_BYTES].try_into().ok()?;
    let chunk = u32::from_be_bytes(key[1 + OVERFLOW_DIGEST_BYTES..].try_into().ok()?);
    Some((digest, chunk))
}

fn overflow_chunks(
    key: &[u8],
    payload: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeedBackendError> {
    let stream_len = key
        .len()
        .checked_add(payload.len())
        .ok_or_else(|| HeedBackendError("overflow row is too large".to_owned()))?;
    let chunk_count = stream_len.saturating_sub(1) / OVERFLOW_CHUNK_BYTES + 1;
    let chunk_count = u32::try_from(chunk_count)
        .map_err(|_| HeedBackendError("overflow row has too many chunks".to_owned()))?;
    let key_len = u32::try_from(key.len())
        .map_err(|_| HeedBackendError("overflow key is too large".to_owned()))?;

    let mut stream = Vec::with_capacity(stream_len);
    stream.extend_from_slice(key);
    stream.extend_from_slice(payload);

    let digest = overflow_digest(key);
    let mut chunks = Vec::with_capacity(chunk_count as usize);
    for (index, bytes) in stream.chunks(OVERFLOW_CHUNK_BYTES).enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| HeedBackendError("overflow row has too many chunks".to_owned()))?;
        let mut value =
            Vec::with_capacity(bytes.len() + if index == 0 { OVERFLOW_HEADER_BYTES } else { 0 });
        if index == 0 {
            value.extend_from_slice(&key_len.to_be_bytes());
            value.extend_from_slice(&chunk_count.to_be_bytes());
        }
        value.extend_from_slice(bytes);
        chunks.push((overflow_chunk_key(&digest, index), value));
    }
    Ok(chunks)
}

fn overflow_header(value: &[u8]) -> Result<(usize, usize), HeedBackendError> {
    if value.len() < OVERFLOW_HEADER_BYTES {
        return Err(HeedBackendError("invalid overflow header".to_owned()));
    }
    let key_len = u32::from_be_bytes(
        value[..4]
            .try_into()
            .map_err(|_| HeedBackendError("invalid overflow key length".to_owned()))?,
    ) as usize;
    let chunk_count = u32::from_be_bytes(
        value[4..8]
            .try_into()
            .map_err(|_| HeedBackendError("invalid overflow chunk count".to_owned()))?,
    ) as usize;
    if chunk_count == 0 {
        return Err(HeedBackendError("invalid overflow chunk count".to_owned()));
    }
    Ok((key_len, chunk_count))
}

fn duplicate_entry(key: &[u8], payload: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let (logical_key, suffix) = duplicate_parts(key)?;
    let encoded = encode_duplicate_value(suffix, payload);
    (logical_key.len() <= LMDB_MAX_KEY_BYTES && encoded.len() <= LMDB_MAX_VALUE_BYTES)
        .then(|| (logical_key.to_vec(), encoded))
}

fn duplicate_parts(key: &[u8]) -> Option<(&[u8], &[u8])> {
    if !matches!(key.first(), Some(b'H' | b'x' | b'g' | b'G')) {
        return None;
    }
    let first_end = component_end(key, 1)?;
    let second_end = component_end(key, first_end)?;
    if second_end != key.len() {
        return None;
    }
    Some((&key[..first_end], &key[first_end + 4..second_end]))
}

fn component_end(bytes: &[u8], start: usize) -> Option<usize> {
    let length = u32::from_be_bytes(bytes.get(start..start + 4)?.try_into().ok()?) as usize;
    let end = start.checked_add(4)?.checked_add(length)?;
    (end <= bytes.len()).then_some(end)
}

fn encode_duplicate_value(suffix: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(4 + suffix.len() + payload.len());
    value.extend_from_slice(&(suffix.len() as u32).to_be_bytes());
    value.extend_from_slice(suffix);
    value.extend_from_slice(payload);
    value
}

fn decode_duplicate_value(value: &[u8]) -> Option<(&[u8], &[u8])> {
    let suffix_length = u32::from_be_bytes(value.get(..4)?.try_into().ok()?) as usize;
    let suffix_start: usize = 4;
    let suffix_end = suffix_start.checked_add(suffix_length)?;
    (suffix_end <= value.len()).then_some((&value[suffix_start..suffix_end], &value[suffix_end..]))
}

fn append_component(key: &mut Vec<u8>, component: &[u8]) {
    key.extend_from_slice(&(component.len() as u32).to_be_bytes());
    key.extend_from_slice(component);
}

pub type HeedStorageError = KvStorageError<HeedBackendError>;
pub type HeedStorage = KvProjectionStorage<HeedBackend>;
pub type HeedIndexError = ProjectionIndexError<HeedStorageError>;
impl HeedStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HeedStorageError> {
        HeedBackend::open(path)
            .map(Self::new)
            .map_err(KvStorageError::Backend)
    }

    pub fn in_memory() -> Result<Self, HeedStorageError> {
        HeedBackend::in_memory()
            .map(Self::new)
            .map_err(KvStorageError::Backend)
    }
}

/// Generic projection index backed by LMDB through heed.
pub struct HeedIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    inner: ProjectionIndex<HeedStorage, M>,
}

impl<M> HeedIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HeedIndexError> {
        let storage = HeedBackend::open(path)
            .map(HeedStorage::new)
            .map_err(|error| ProjectionIndexError::Storage(KvStorageError::Backend(error)))?;
        Ok(Self {
            inner: ProjectionIndex::new(storage),
        })
    }

    pub fn in_memory() -> Result<Self, HeedIndexError> {
        let storage = HeedBackend::in_memory()
            .map(HeedStorage::new)
            .map_err(|error| ProjectionIndexError::Storage(KvStorageError::Backend(error)))?;
        Ok(Self {
            inner: ProjectionIndex::new(storage),
        })
    }
}

impl<M> Deref for HeedIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Target = ProjectionIndex<HeedStorage, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for HeedIndex<M>
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

impl<M> provenance_core::QueryEngine<M> for HeedIndex<M>
where
    M: IndexModel,
    M::Id: Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = HeedIndexError;

    fn execute(
        &self,
        query: &provenance_core::Query<M>,
    ) -> Result<provenance_core::QueryResult<M>, Self::Error> {
        provenance_core::QueryEngine::execute(&self.inner, query)
    }
}

impl<M> ProjectionBackend<M> for HeedIndex<M>
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
    fn native_duplicate_rows_round_trip_through_kv_surface() {
        let mut backend = HeedBackend::in_memory().expect("open heed backend");
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

    #[test]
    fn oversized_rows_round_trip_persist_and_update() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let path = directory.path().join("projection");
        let long_key = vec![b'k'; LMDB_MAX_KEY_BYTES + 37];
        let long_value = vec![b'v'; LMDB_MAX_VALUE_BYTES + 701];
        let changing_key = b"o-changing".to_vec();
        let large_value = vec![b'x'; LMDB_MAX_VALUE_BYTES + 113];
        let small_value = b"small".to_vec();

        {
            let mut backend = HeedBackend::open(&path).expect("open persistent heed backend");
            backend
                .replace_entries(&[
                    (long_key.clone(), long_value.clone()),
                    (changing_key.clone(), large_value.clone()),
                ])
                .expect("write oversized rows");
        }

        {
            let mut backend = HeedBackend::open(&path).expect("reopen persistent heed backend");
            assert_eq!(
                backend.get(&long_key).expect("read long key"),
                Some(long_value.clone())
            );
            assert_eq!(
                backend.get(&changing_key).expect("read large value"),
                Some(large_value.clone())
            );
            assert_eq!(
                backend.entries().expect("enumerate oversized rows").len(),
                2
            );

            backend
                .apply_entries(&[(changing_key.clone(), small_value.clone())])
                .expect("replace overflow row with native row");
            assert_eq!(
                backend.get(&changing_key).expect("read native replacement"),
                Some(small_value.clone())
            );
        }

        let backend = HeedBackend::open(&path).expect("reopen updated heed backend");
        assert_eq!(
            backend.get(&long_key).expect("read persisted long key"),
            Some(long_value)
        );
        assert_eq!(
            backend
                .get(&changing_key)
                .expect("read persisted replacement"),
            Some(small_value)
        );
        assert_eq!(backend.entries().expect("enumerate updated rows").len(), 2);
    }
}
