use std::collections::{BTreeMap, BTreeSet};

use provenance_core::{
    Model, Object, ObjectId, Operation, OperationId, ProvenanceStore, PublishOutcome,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{StorageError, collect_reachable};

/// In-memory authoritative provenance storage.
#[derive(Debug)]
pub struct MemoryProvenanceStore<M: Model> {
    operations: BTreeMap<M::Id, Operation<M>>,
    objects: BTreeMap<M::Id, Object<M>>,
    heads: BTreeSet<OperationId<M>>,
}

impl<M: Model> Default for MemoryProvenanceStore<M> {
    fn default() -> Self {
        Self {
            operations: BTreeMap::new(),
            objects: BTreeMap::new(),
            heads: BTreeSet::new(),
        }
    }
}

impl<M: Model> MemoryProvenanceStore<M> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<M> MemoryProvenanceStore<M>
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
}

impl<M> ProvenanceStore<M> for MemoryProvenanceStore<M>
where
    M: Model,
    M::Id: AsRef<str> + Serialize + DeserializeOwned,
    M::Seed: Serialize + DeserializeOwned,
    M::ExternalId: Serialize + DeserializeOwned,
    M::Payload: Serialize + DeserializeOwned,
{
    type Error = StorageError;

    fn get_operation(&self, id: &OperationId<M>) -> Result<Option<Operation<M>>, Self::Error> {
        Ok(self.operations.get(id.raw()).cloned())
    }

    fn has_operation(&self, id: &OperationId<M>) -> Result<bool, Self::Error> {
        Ok(self.operations.contains_key(id.raw()))
    }

    fn put_operation(&mut self, operation: &Operation<M>) -> Result<(), Self::Error> {
        if let Some(existing) = self.operations.get(operation.id.raw()) {
            if existing != operation {
                return Err(StorageError::OperationCollision(
                    operation.id.raw().as_ref().to_owned(),
                ));
            }
            return Ok(());
        }
        self.operations
            .insert(operation.id.raw().clone(), operation.clone());
        Ok(())
    }

    fn get_object(&self, id: &ObjectId<M>) -> Result<Option<Object<M>>, Self::Error> {
        Ok(self.objects.get(id.raw()).cloned())
    }

    fn put_object(&mut self, object: &Object<M>) -> Result<(), Self::Error> {
        if let Some(existing) = self.objects.get(object.id.raw()) {
            if existing != object {
                return Err(StorageError::ObjectCollision(
                    object.id.raw().as_ref().to_owned(),
                ));
            }
            return Ok(());
        }
        self.objects.insert(object.id.raw().clone(), object.clone());
        Ok(())
    }

    fn heads(&self) -> Result<BTreeSet<OperationId<M>>, Self::Error> {
        Ok(self.heads.clone())
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

        if self.heads == *expected {
            if self.heads == *next {
                return Ok(PublishOutcome::AlreadyPresent);
            }
            self.heads = next.clone();
            return Ok(PublishOutcome::Published);
        }
        let removed = expected.difference(next).cloned().collect::<BTreeSet<_>>();
        self.heads = self
            .heads
            .union(next)
            .filter(|id| !removed.contains(*id))
            .cloned()
            .collect();
        Ok(PublishOutcome::Merged)
    }
}
