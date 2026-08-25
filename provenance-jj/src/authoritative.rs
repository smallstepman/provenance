use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fs2::FileExt;
use provenance_core::{Object, ObjectId, Operation, OperationId, ProvenanceStore, PublishOutcome};
use thiserror::Error;

use crate::JjModel;
use crate::codec::{self, CodecError};

const OPERATIONS_DIR: &str = "operations";
const OBJECTS_DIR: &str = "objects";
const HEADS_DIR: &str = "heads";
const HEADS_MANIFEST: &str = "manifest";
const HEADS_LOCK: &str = "heads.lock";
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum DirectoryStoreError {
    #[error("authoritative provenance filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("operation {0} has different immutable contents")]
    OperationCollision(String),
    #[error("object {0} has different immutable contents")]
    ObjectCollision(String),
    #[error("authoritative provenance operation {0} is missing")]
    MissingOperation(String),
    #[error("authoritative provenance path component is invalid: {0}")]
    InvalidComponent(String),
}

#[derive(Debug)]
enum Backend {
    Memory {
        operations: BTreeMap<String, Operation<JjModel>>,
        objects: BTreeMap<String, Object<JjModel>>,
        heads: BTreeSet<OperationId<JjModel>>,
    },
    Filesystem {
        root: PathBuf,
    },
}

/// Immutable operation/object storage with atomically published head pointers.
///
/// The in-memory backend is used by unit/integration tests. The filesystem
/// backend is the durable authority used by the CLI. SQLite indexes may be
/// rebuilt from this interface but never participate in publication.
#[derive(Debug)]
pub struct DirectoryProvenanceStore {
    backend: Backend,
}

impl DirectoryProvenanceStore {
    pub fn in_memory() -> Self {
        Self {
            backend: Backend::Memory {
                operations: BTreeMap::new(),
                objects: BTreeMap::new(),
                heads: BTreeSet::new(),
            },
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, DirectoryStoreError> {
        let root = path.as_ref().to_owned();
        fs::create_dir_all(root.join(OPERATIONS_DIR))?;
        fs::create_dir_all(root.join(OBJECTS_DIR))?;
        fs::create_dir_all(root.join(HEADS_DIR))?;
        Ok(Self {
            backend: Backend::Filesystem { root },
        })
    }

    pub fn root(&self) -> Option<&Path> {
        match &self.backend {
            Backend::Memory { .. } => None,
            Backend::Filesystem { root } => Some(root),
        }
    }

    pub fn reachable_operations(&self) -> Result<Vec<Operation<JjModel>>, DirectoryStoreError> {
        let mut seen = BTreeSet::new();
        let mut ordered = Vec::new();
        for head in self.heads()? {
            self.collect_reachable(&head, &mut seen, &mut ordered)?;
        }
        Ok(ordered)
    }

    pub fn reachable_operation_count(&self) -> Result<usize, DirectoryStoreError> {
        Ok(self.reachable_operations()?.len())
    }

    fn collect_reachable(
        &self,
        id: &OperationId<JjModel>,
        seen: &mut BTreeSet<OperationId<JjModel>>,
        ordered: &mut Vec<Operation<JjModel>>,
    ) -> Result<(), DirectoryStoreError> {
        if !seen.insert(id.clone()) {
            return Ok(());
        }
        let operation = self
            .get_operation(id)?
            .ok_or_else(|| DirectoryStoreError::MissingOperation(id.raw().clone()))?;
        for parent in &operation.parents {
            self.collect_reachable(parent, seen, ordered)?;
        }
        ordered.push(operation);
        Ok(())
    }

    fn operation_path(&self, id: &OperationId<JjModel>) -> Result<PathBuf, DirectoryStoreError> {
        self.path_for(OPERATIONS_DIR, id.raw())
    }

    fn object_path(&self, id: &ObjectId<JjModel>) -> Result<PathBuf, DirectoryStoreError> {
        self.path_for(OBJECTS_DIR, id.raw())
    }

    fn path_for(&self, directory: &str, id: &str) -> Result<PathBuf, DirectoryStoreError> {
        validate_component(id)?;
        let Backend::Filesystem { root } = &self.backend else {
            return Err(DirectoryStoreError::InvalidComponent(
                "memory backend has no filesystem path".to_owned(),
            ));
        };
        Ok(root.join(directory).join(id))
    }

    fn read_heads(&self) -> Result<BTreeSet<OperationId<JjModel>>, DirectoryStoreError> {
        let Backend::Filesystem { root } = &self.backend else {
            return Err(DirectoryStoreError::InvalidComponent(
                "memory backend has no head manifest".to_owned(),
            ));
        };
        let manifest = root.join(HEADS_DIR).join(HEADS_MANIFEST);
        if !manifest.exists() {
            return Ok(BTreeSet::new());
        }
        let bytes = fs::read(manifest)?;
        Ok(codec::decode(&bytes)?)
    }

    fn write_heads(
        &self,
        heads: &BTreeSet<OperationId<JjModel>>,
    ) -> Result<(), DirectoryStoreError> {
        let Backend::Filesystem { root } = &self.backend else {
            return Err(DirectoryStoreError::InvalidComponent(
                "memory backend has no head manifest".to_owned(),
            ));
        };
        let directory = root.join(HEADS_DIR);
        let manifest = directory.join(HEADS_MANIFEST);
        let bytes = codec::encode(heads)?;
        atomic_replace(&directory, &manifest, &bytes)
    }

    fn lock_heads(&self) -> Result<File, DirectoryStoreError> {
        let Backend::Filesystem { root } = &self.backend else {
            return Err(DirectoryStoreError::InvalidComponent(
                "memory backend has no head lock".to_owned(),
            ));
        };
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(HEADS_LOCK))?;
        lock.lock_exclusive()?;
        Ok(lock)
    }
}

impl ProvenanceStore<JjModel> for DirectoryProvenanceStore {
    type Error = DirectoryStoreError;

    fn get_operation(
        &self,
        id: &OperationId<JjModel>,
    ) -> Result<Option<Operation<JjModel>>, Self::Error> {
        match &self.backend {
            Backend::Memory { operations, .. } => Ok(operations.get(id.raw()).cloned()),
            Backend::Filesystem { .. } => {
                let path = self.operation_path(id)?;
                if !path.exists() {
                    return Ok(None);
                }
                let operation: Operation<JjModel> = decode_file(&path)?;
                if operation.id != *id {
                    return Err(DirectoryStoreError::OperationCollision(id.raw().clone()));
                }
                Ok(Some(operation))
            }
        }
    }

    fn has_operation(&self, id: &OperationId<JjModel>) -> Result<bool, Self::Error> {
        Ok(self.get_operation(id)?.is_some())
    }

    fn put_operation(&mut self, operation: &Operation<JjModel>) -> Result<(), Self::Error> {
        match &mut self.backend {
            Backend::Memory { operations, .. } => {
                if let Some(existing) = operations.get(operation.id.raw()) {
                    if existing != operation {
                        return Err(DirectoryStoreError::OperationCollision(
                            operation.id.raw().clone(),
                        ));
                    }
                    return Ok(());
                }
                operations.insert(operation.id.raw().clone(), operation.clone());
                Ok(())
            }
            Backend::Filesystem { .. } => {
                let path = self.operation_path(&operation.id)?;
                put_immutable(&path, operation, &|existing| existing == operation).map_err(
                    |error| match error {
                        ImmutablePutError::Io(error) => DirectoryStoreError::Io(error),
                        ImmutablePutError::Codec(error) => DirectoryStoreError::Codec(error),
                        ImmutablePutError::Collision => {
                            DirectoryStoreError::OperationCollision(operation.id.raw().clone())
                        }
                    },
                )
            }
        }
    }

    fn get_object(&self, id: &ObjectId<JjModel>) -> Result<Option<Object<JjModel>>, Self::Error> {
        match &self.backend {
            Backend::Memory { objects, .. } => Ok(objects.get(id.raw()).cloned()),
            Backend::Filesystem { .. } => {
                let path = self.object_path(id)?;
                if !path.exists() {
                    return Ok(None);
                }
                let object: Object<JjModel> = decode_file(&path)?;
                if object.id != *id {
                    return Err(DirectoryStoreError::ObjectCollision(id.raw().clone()));
                }
                Ok(Some(object))
            }
        }
    }

    fn put_object(&mut self, object: &Object<JjModel>) -> Result<(), Self::Error> {
        match &mut self.backend {
            Backend::Memory { objects, .. } => {
                if let Some(existing) = objects.get(object.id.raw()) {
                    if existing != object {
                        return Err(DirectoryStoreError::ObjectCollision(
                            object.id.raw().clone(),
                        ));
                    }
                    return Ok(());
                }
                objects.insert(object.id.raw().clone(), object.clone());
                Ok(())
            }
            Backend::Filesystem { .. } => {
                let path = self.object_path(&object.id)?;
                put_immutable(&path, object, &|existing| existing == object).map_err(|error| {
                    match error {
                        ImmutablePutError::Io(error) => DirectoryStoreError::Io(error),
                        ImmutablePutError::Codec(error) => DirectoryStoreError::Codec(error),
                        ImmutablePutError::Collision => {
                            DirectoryStoreError::ObjectCollision(object.id.raw().clone())
                        }
                    }
                })
            }
        }
    }

    fn heads(&self) -> Result<BTreeSet<OperationId<JjModel>>, Self::Error> {
        match &self.backend {
            Backend::Memory { heads, .. } => Ok(heads.clone()),
            Backend::Filesystem { .. } => self.read_heads(),
        }
    }

    fn publish_heads(
        &mut self,
        expected: &BTreeSet<OperationId<JjModel>>,
        next: &BTreeSet<OperationId<JjModel>>,
    ) -> Result<PublishOutcome, Self::Error> {
        for id in next {
            if !self.has_operation(id)? {
                return Err(DirectoryStoreError::MissingOperation(id.raw().clone()));
            }
        }

        match &mut self.backend {
            Backend::Memory { heads, .. } => {
                if *heads == *expected {
                    if *heads == *next {
                        return Ok(PublishOutcome::AlreadyPresent);
                    }
                    *heads = next.clone();
                    return Ok(PublishOutcome::Published);
                }
                let removed = expected.difference(next).cloned().collect::<BTreeSet<_>>();
                let merged = heads
                    .union(next)
                    .filter(|id| !removed.contains(*id))
                    .cloned()
                    .collect();
                *heads = merged;
                Ok(PublishOutcome::Merged)
            }
            Backend::Filesystem { .. } => {
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
    }
}

fn validate_component(component: &str) -> Result<(), DirectoryStoreError> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains('/')
        || component.contains('\\')
    {
        return Err(DirectoryStoreError::InvalidComponent(component.to_owned()));
    }
    Ok(())
}

fn decode_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, DirectoryStoreError> {
    let bytes = fs::read(path)?;
    Ok(codec::decode(&bytes)?)
}

enum ImmutablePutError {
    Io(std::io::Error),
    Codec(CodecError),
    Collision,
}

fn put_immutable<T, F>(path: &Path, value: &T, same: &F) -> Result<(), ImmutablePutError>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    F: Fn(&T) -> bool,
{
    if path.exists() {
        let existing = decode_file(path).map_err(|error| match error {
            DirectoryStoreError::Io(error) => ImmutablePutError::Io(error),
            DirectoryStoreError::Codec(error) => ImmutablePutError::Codec(error),
            DirectoryStoreError::OperationCollision(_)
            | DirectoryStoreError::ObjectCollision(_)
            | DirectoryStoreError::MissingOperation(_)
            | DirectoryStoreError::InvalidComponent(_) => ImmutablePutError::Collision,
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
    let bytes = codec::encode(value).map_err(ImmutablePutError::Codec)?;
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
                DirectoryStoreError::Io(error) => ImmutablePutError::Io(error),
                DirectoryStoreError::Codec(error) => ImmutablePutError::Codec(error),
                DirectoryStoreError::OperationCollision(_)
                | DirectoryStoreError::ObjectCollision(_)
                | DirectoryStoreError::MissingOperation(_)
                | DirectoryStoreError::InvalidComponent(_) => ImmutablePutError::Collision,
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

fn atomic_replace(
    directory: &Path,
    target: &Path,
    bytes: &[u8],
) -> Result<(), DirectoryStoreError> {
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
