use std::collections::BTreeSet;
use std::sync::Arc;

use crate::model::{COMMIT_KIND, JJ_NAMESPACE, JjModel, jj_commit_address};
use gix::refs::transaction::PreviousValue;
use jj_lib::backend::CommitId;
use jj_lib::git_backend::GitBackend;
use jj_lib::object_id::ObjectId;
use jj_lib::repo::{ReadonlyRepo, Repo};
use provenance_core::{EntityRef, Resource, RetentionStrength, RetentionTransition};
use thiserror::Error;

pub const KEEP_REF_PREFIX: &str = "refs/jj-prov/keep/";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JjRetentionError {
    #[error("JJ retention requires a Git-backed repository")]
    UnsupportedBackend,
    #[error("Escrowed retention is not supported by provenance-jj")]
    UnsupportedEscrowed,
    #[error("retention resource is not a JJ commit: {0:?}")]
    InvalidResource(EntityRef<JjModel>),
    #[error("invalid JJ commit id: {0}")]
    InvalidCommitId(String),
    #[error("could not access Git retention ref: {0}")]
    Git(String),
    #[error("retention ref {name} resolved to {actual}, expected {expected}")]
    RefMismatch {
        name: String,
        actual: String,
        expected: String,
    },
    #[error("retention ref {0} does not exist")]
    MissingRef(String),
}

#[derive(Clone)]
pub struct JjRetention {
    repo: Arc<ReadonlyRepo>,
}

impl std::fmt::Debug for JjRetention {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JjRetention")
            .field("repo", &self.repo.loader().store().backend())
            .finish()
    }
}

impl JjRetention {
    pub fn new(repo: Arc<ReadonlyRepo>) -> Self {
        Self { repo }
    }

    pub fn keep_ref_name(commit_id: &str) -> String {
        format!("{KEEP_REF_PREFIX}{commit_id}")
    }

    pub fn prepare(
        &self,
        transition: &RetentionTransition<JjModel>,
    ) -> Result<(), JjRetentionError> {
        match transition {
            RetentionTransition::BecameRequired { resource, strength }
            | RetentionTransition::Strengthened {
                resource,
                to: strength,
                ..
            } => match strength {
                RetentionStrength::Referenced => Ok(()),
                RetentionStrength::Pinned => self.ensure_pinned(resource),
                RetentionStrength::Escrowed => Err(JjRetentionError::UnsupportedEscrowed),
            },
            RetentionTransition::Weakened { resource, to, .. } => match to {
                RetentionStrength::Referenced => Ok(()),
                RetentionStrength::Pinned => self.ensure_pinned(resource),
                RetentionStrength::Escrowed => Err(JjRetentionError::UnsupportedEscrowed),
            },
            RetentionTransition::BecameUnrequired { .. } => Ok(()),
        }
    }

    pub fn finalize(
        &self,
        transition: &RetentionTransition<JjModel>,
    ) -> Result<(), JjRetentionError> {
        match transition {
            RetentionTransition::BecameRequired { resource, strength }
            | RetentionTransition::Strengthened {
                resource,
                to: strength,
                ..
            } => match strength {
                RetentionStrength::Referenced => Ok(()),
                RetentionStrength::Pinned => self.ensure_pinned(resource),
                RetentionStrength::Escrowed => Err(JjRetentionError::UnsupportedEscrowed),
            },
            RetentionTransition::Weakened { resource, to, .. } => match to {
                RetentionStrength::Referenced => self.remove_pin(resource),
                RetentionStrength::Pinned => self.ensure_pinned(resource),
                RetentionStrength::Escrowed => Err(JjRetentionError::UnsupportedEscrowed),
            },
            RetentionTransition::BecameUnrequired { resource, .. } => self.remove_pin(resource),
        }
    }
    /// Reconcile Git retention refs from the authoritative kernel projection.
    ///
    /// This is safe to rerun after a process crash: required pins are verified
    /// and stale provenance-owned pins are removed only when their target still
    /// matches the commit encoded in the ref name.
    pub fn reconcile(
        &self,
        requirements: impl IntoIterator<Item = (Resource<JjModel>, RetentionStrength)>,
    ) -> Result<(), JjRetentionError> {
        let requirements = requirements.into_iter().collect::<Vec<_>>();
        let mut desired = BTreeSet::new();
        for (resource, strength) in &requirements {
            match strength {
                RetentionStrength::Referenced => {}
                RetentionStrength::Pinned => {
                    let (_, name) = self.resource_target(resource)?;
                    desired.insert(name);
                    self.ensure_pinned(resource)?;
                }
                RetentionStrength::Escrowed => {
                    return Err(JjRetentionError::UnsupportedEscrowed);
                }
            }
        }

        let git_repo = match self.git_repo() {
            Ok(repo) => repo,
            Err(JjRetentionError::UnsupportedBackend) if requirements.is_empty() => return Ok(()),
            Err(error) => return Err(error),
        };
        let names = git_repo
            .references()
            .map_err(|error| JjRetentionError::Git(error.to_string()))?
            .all()
            .map_err(|error| JjRetentionError::Git(error.to_string()))?
            .map(|reference| {
                reference
                    .map(|reference| reference.name().as_bstr().to_string())
                    .map_err(|error| JjRetentionError::Git(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for name in names {
            if !name.starts_with(KEEP_REF_PREFIX) || desired.contains(&name) {
                continue;
            }
            let commit_id = &name[KEEP_REF_PREFIX.len()..];
            let resource = Resource::from(EntityRef::External(jj_commit_address(commit_id)));
            // Ignore malformed/non-commit refs under our namespace rather than
            // risking deletion of an unrelated object.
            if self.resource_target(&resource).is_ok() {
                self.remove_pin(&resource)?;
            }
        }
        Ok(())
    }

    pub fn verify_pinned(&self, resource: &Resource<JjModel>) -> Result<(), JjRetentionError> {
        let (commit_id, name) = self.resource_target(resource)?;
        let git_repo = self.git_repo()?;
        let mut reference = git_repo
            .try_find_reference(&name)
            .map_err(|error| JjRetentionError::Git(error.to_string()))?
            .ok_or_else(|| JjRetentionError::MissingRef(name.clone()))?;
        let actual = reference
            .peel_to_id()
            .map_err(|error| JjRetentionError::Git(error.to_string()))?;
        let expected = gix::ObjectId::from_bytes_or_panic(commit_id.as_bytes());
        if actual.as_ref() != expected.as_ref() {
            return Err(JjRetentionError::RefMismatch {
                name,
                actual: actual.to_string(),
                expected: expected.to_string(),
            });
        }
        Ok(())
    }

    fn ensure_pinned(&self, resource: &Resource<JjModel>) -> Result<(), JjRetentionError> {
        let (commit_id, name) = self.resource_target(resource)?;
        let git_repo = self.git_repo()?;
        let expected = gix::ObjectId::from_bytes_or_panic(commit_id.as_bytes());
        if let Some(mut reference) = git_repo
            .try_find_reference(&name)
            .map_err(|error| JjRetentionError::Git(error.to_string()))?
        {
            let actual = reference
                .peel_to_id()
                .map_err(|error| JjRetentionError::Git(error.to_string()))?;
            if actual.as_ref() != expected.as_ref() {
                return Err(JjRetentionError::RefMismatch {
                    name,
                    actual: actual.to_string(),
                    expected: expected.to_string(),
                });
            }
            return Ok(());
        }

        match git_repo.reference(
            name.as_str(),
            expected,
            PreviousValue::MustNotExist,
            "provenance-jj pin",
        ) {
            Ok(_) => self.verify_pinned(resource),
            Err(error) => {
                // A concurrent writer may have created the exact same pin.
                self.verify_pinned(resource).map_err(|verify_error| {
                    JjRetentionError::Git(format!("{error}; {verify_error}"))
                })
            }
        }
    }

    fn remove_pin(&self, resource: &Resource<JjModel>) -> Result<(), JjRetentionError> {
        let (commit_id, name) = self.resource_target(resource)?;
        let git_repo = self.git_repo()?;
        let Some(reference) = git_repo
            .try_find_reference(&name)
            .map_err(|error| JjRetentionError::Git(error.to_string()))?
        else {
            return Ok(());
        };

        let expected = gix::ObjectId::from_bytes_or_panic(commit_id.as_bytes());
        let actual = reference
            .try_id()
            .ok_or_else(|| JjRetentionError::Git(format!("retention ref {name} is symbolic")))?;
        if actual.as_ref() != expected.as_ref() {
            return Err(JjRetentionError::RefMismatch {
                name,
                actual: actual.to_string(),
                expected: expected.to_string(),
            });
        }

        // `Reference::delete()` uses MustExistAndMatch with the target
        // observed above, so a concurrent update fails instead of deleting
        // another writer's retention ref.
        reference
            .delete()
            .map_err(|error| JjRetentionError::Git(error.to_string()))
    }

    fn resource_target(
        &self,
        resource: &Resource<JjModel>,
    ) -> Result<(CommitId, String), JjRetentionError> {
        let EntityRef::External(address) = &resource.entity else {
            return Err(JjRetentionError::InvalidResource(resource.entity.clone()));
        };
        if address.namespace.as_str() != JJ_NAMESPACE || address.kind.as_str() != COMMIT_KIND {
            return Err(JjRetentionError::InvalidResource(resource.entity.clone()));
        }
        let commit_id = CommitId::try_from_hex(address.id.as_bytes())
            .ok_or_else(|| JjRetentionError::InvalidCommitId(address.id.clone()))?;
        Ok((commit_id, Self::keep_ref_name(address.id.as_str())))
    }

    fn git_repo(&self) -> Result<gix::Repository, JjRetentionError> {
        let backend = self
            .repo
            .store()
            .backend_impl::<GitBackend>()
            .ok_or(JjRetentionError::UnsupportedBackend)?;
        Ok(backend.git_repo())
    }
}

pub fn keep_ref_name(commit_id: &str) -> String {
    JjRetention::keep_ref_name(commit_id)
}
