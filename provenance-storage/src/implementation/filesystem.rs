use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fs2::FileExt;
use provenance_core::{
    Model, Object, ObjectId, Operation, OperationId, ProvenanceStore, PublishOutcome,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{CodecError, StorageError, collect_reachable};

const OPERATIONS_DIR: &str = "operations";
const OBJECTS_DIR: &str = "objects";
const HEADS_DIR: &str = "heads";
const HEADS_MANIFEST: &str = "manifest";
const HEADS_LOCK: &str = "heads.lock";
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Filesystem-backed authoritative provenance storage.
#[derive(Debug)]
pub struct DirectoryProvenanceStore<M: Model> {
    root: PathBuf,
    _model: std::marker::PhantomData<M>,
}

impl<M: Model> DirectoryProvenanceStore<M> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let root = path.as_ref().to_owned();
        fs::create_dir_all(root.join(OPERATIONS_DIR))?;
        fs::create_dir_all(root.join(OBJECTS_DIR))?;
        fs::create_dir_all(root.join(HEADS_DIR))?;
        Ok(Self {
            root,
            _model: std::marker::PhantomData,
        })
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }
}

impl<M> DirectoryProvenanceStore<M>
where
    M: Model,
    M::Id: AsRef<str> + Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    pub fn reachable_operations(&self) -> Result<Vec<Operation<M>>, StorageError> {
        let mut seen = BTreeSet::new();
        let mut ordered = Vec::new();
        for head in self.heads()? {
            collect_reachable(self, &head, &mut seen, &mut ordered)?;
        }
        Ok(ordered)
    }

    pub fn reachable_operation_count(&self) -> Result<usize, StorageError> {
        Ok(self.reachable_operations()?.len())
    }

    fn operation_path(&self, id: &OperationId<M>) -> Result<PathBuf, StorageError> {
        self.path_for(OPERATIONS_DIR, id.raw().as_ref())
    }

    fn object_path(&self, id: &ObjectId<M>) -> Result<PathBuf, StorageError> {
        self.path_for(OBJECTS_DIR, id.raw().as_ref())
    }

    fn path_for(&self, directory: &str, id: &str) -> Result<PathBuf, StorageError> {
        validate_component(id)?;
        Ok(self.root.join(directory).join(id))
    }

    fn read_heads(&self) -> Result<BTreeSet<OperationId<M>>, StorageError> {
        let manifest = self.root.join(HEADS_DIR).join(HEADS_MANIFEST);
        if !manifest.exists() {
            return Ok(BTreeSet::new());
        }
        let bytes = fs::read(manifest)?;
        Ok(crate::codec::decode(&bytes)?)
    }

    fn write_heads(&self, heads: &BTreeSet<OperationId<M>>) -> Result<(), StorageError> {
        let directory = self.root.join(HEADS_DIR);
        let manifest = directory.join(HEADS_MANIFEST);
        let bytes = crate::codec::encode(heads)?;
        atomic_replace(&directory, &manifest, &bytes)
    }

    fn lock_heads(&self) -> Result<File, StorageError> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(HEADS_LOCK))?;
        lock.lock_exclusive()?;
        Ok(lock)
    }
}

impl<M> ProvenanceStore<M> for DirectoryProvenanceStore<M>
where
    M: Model,
    M::Id: AsRef<str> + Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = StorageError;

    fn get_operation(&self, id: &OperationId<M>) -> Result<Option<Operation<M>>, Self::Error> {
        let path = self.operation_path(id)?;
        if !path.exists() {
            return Ok(None);
        }
        let operation: Operation<M> = decode_file(&path)?;
        if operation.id != *id {
            return Err(StorageError::OperationCollision(
                id.raw().as_ref().to_owned(),
            ));
        }
        Ok(Some(operation))
    }

    fn has_operation(&self, id: &OperationId<M>) -> Result<bool, Self::Error> {
        Ok(self.get_operation(id)?.is_some())
    }

    fn put_operation(&mut self, operation: &Operation<M>) -> Result<(), Self::Error> {
        let path = self.operation_path(&operation.id)?;
        put_immutable(&path, operation, &|existing| existing == operation).map_err(|error| {
            match error {
                ImmutablePutError::Io(error) => StorageError::Io(error),
                ImmutablePutError::Codec(error) => StorageError::Codec(error),
                ImmutablePutError::Collision => {
                    StorageError::OperationCollision(operation.id.raw().as_ref().to_owned())
                }
            }
        })
    }

    fn get_object(&self, id: &ObjectId<M>) -> Result<Option<Object<M>>, Self::Error> {
        let path = self.object_path(id)?;
        if !path.exists() {
            return Ok(None);
        }
        let object: Object<M> = decode_file(&path)?;
        if object.id != *id {
            return Err(StorageError::ObjectCollision(id.raw().as_ref().to_owned()));
        }
        Ok(Some(object))
    }

    fn put_object(&mut self, object: &Object<M>) -> Result<(), Self::Error> {
        let path = self.object_path(&object.id)?;
        put_immutable(&path, object, &|existing| existing == object).map_err(|error| match error {
            ImmutablePutError::Io(error) => StorageError::Io(error),
            ImmutablePutError::Codec(error) => StorageError::Codec(error),
            ImmutablePutError::Collision => {
                StorageError::ObjectCollision(object.id.raw().as_ref().to_owned())
            }
        })
    }

    fn heads(&self) -> Result<BTreeSet<OperationId<M>>, Self::Error> {
        self.read_heads()
    }

    fn publish_heads(
        &mut self,
        expected: &BTreeSet<OperationId<M>>,
        next: &BTreeSet<OperationId<M>>,
    ) -> Result<PublishOutcome, Self::Error> {
        for id in next {
            if !self.has_operation(id)? {
                return Err(StorageError::MissingOperation(id.raw().as_ref().to_owned()));
            }
        }

        let lock = self.lock_heads()?;
        let actual = self.read_heads()?;
        let (outcome, resulting_heads) = if actual == *expected {
            if actual == *next {
                (PublishOutcome::AlreadyPresent, actual.clone())
            } else {
                (PublishOutcome::Published, next.clone())
            }
        } else {
            let removed = expected.difference(next).cloned().collect::<BTreeSet<_>>();
            (
                PublishOutcome::Merged,
                actual
                    .union(next)
                    .filter(|id| !removed.contains(*id))
                    .cloned()
                    .collect(),
            )
        };
        if resulting_heads != actual {
            self.write_heads(&resulting_heads)?;
        }
        lock.unlock()?;
        Ok(outcome)
    }
}

fn validate_component(component: &str) -> Result<(), StorageError> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains('/')
        || component.contains('\\')
    {
        return Err(StorageError::InvalidComponent(component.to_owned()));
    }
    Ok(())
}

fn decode_file<T: DeserializeOwned>(path: &Path) -> Result<T, StorageError> {
    let bytes = fs::read(path)?;
    Ok(crate::codec::decode(&bytes)?)
}

enum ImmutablePutError {
    Io(std::io::Error),
    Codec(CodecError),
    Collision,
}

fn put_immutable<T, F>(path: &Path, value: &T, same: &F) -> Result<(), ImmutablePutError>
where
    T: Serialize + DeserializeOwned,
    F: Fn(&T) -> bool,
{
    if path.exists() {
        let existing = decode_file(path).map_err(|error| match error {
            StorageError::Io(error) => ImmutablePutError::Io(error),
            StorageError::Codec(error) => ImmutablePutError::Codec(error),
            StorageError::OperationCollision(_)
            | StorageError::ObjectCollision(_)
            | StorageError::MissingOperation(_)
            | StorageError::InvalidComponent(_) => ImmutablePutError::Collision,
        })?;
        return if same(&existing) {
            Ok(())
        } else {
            Err(ImmutablePutError::Collision)
        };
    }

    let directory = path
        .parent()
        .ok_or_else(|| ImmutablePutError::Io(std::io::Error::other("missing store directory")))?;
    let bytes = crate::codec::encode(value).map_err(ImmutablePutError::Codec)?;
    let temporary = temporary_path(directory, path.file_name().unwrap_or_default());
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(ImmutablePutError::Io)?;
    file.write_all(&bytes).map_err(ImmutablePutError::Io)?;
    file.sync_all().map_err(ImmutablePutError::Io)?;
    drop(file);

    let result = fs::hard_link(&temporary, path);
    let _ = fs::remove_file(&temporary);
    match result {
        Ok(()) => {
            sync_directory(directory).map_err(ImmutablePutError::Io)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = decode_file(path).map_err(|error| match error {
                StorageError::Io(error) => ImmutablePutError::Io(error),
                StorageError::Codec(error) => ImmutablePutError::Codec(error),
                StorageError::OperationCollision(_)
                | StorageError::ObjectCollision(_)
                | StorageError::MissingOperation(_)
                | StorageError::InvalidComponent(_) => ImmutablePutError::Collision,
            })?;
            if same(&existing) {
                Ok(())
            } else {
                Err(ImmutablePutError::Collision)
            }
        }
        Err(error) => Err(ImmutablePutError::Io(error)),
    }
}

fn atomic_replace(directory: &Path, target: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let temporary = temporary_path(directory, target.file_name().unwrap_or_default());
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, target)?;
    sync_directory(directory)?;
    Ok(())
}

fn temporary_path(directory: &Path, name: &std::ffi::OsStr) -> PathBuf {
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(
        ".{}.{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id(),
        counter
    ))
}

fn sync_directory(directory: &Path) -> std::io::Result<()> {
    File::open(directory)?.sync_all()
}
