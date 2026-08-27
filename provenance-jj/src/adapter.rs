use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::identity::observation_seed;
use jj_lib::backend::{ChangeId, CommitId};
use jj_lib::commit::Commit;
use jj_lib::default_backend_factories::{
    default_backend_factories, default_working_copy_factories,
};
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::OperationId;
use jj_lib::operation::Operation;
use jj_lib::repo::{ReadonlyRepo, Repo};
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use pollster::block_on;
use provenance_core::{
    Adapter, Attributes, EntityObservation, EventIntent, FieldName, Intent, Relation, RelationType,
    SourceOperation, Transaction, Value,
};
use thiserror::Error;

use crate::model::{
    JJ_NAMESPACE, JjModel, external, jj_change, jj_commit_address, jj_operation, jj_workspace,
    string_value,
};
use crate::schema::{jj_schema, jj_schema_key, jj_why_query};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JjRepositoryError {
    #[error("could not load JJ workspace: {0}")]
    Load(String),
    #[error("could not load JJ operation: {0}")]
    Operation(String),
    #[error("could not load JJ commit: {0}")]
    Commit(String),
}

/// A read-only JJ repository snapshot used as adapter input.
#[derive(Clone)]
pub struct JjRepository {
    repo: Arc<ReadonlyRepo>,
    workspace_root: Option<PathBuf>,
}

impl std::fmt::Debug for JjRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JjRepository")
            .field("repo_path", &self.repo.loader().store().backend())
            .field("workspace_root", &self.workspace_root)
            .field("operation", &self.repo.operation().id())
            .finish()
    }
}

impl JjRepository {
    /// Wraps an already-loaded JJ repository snapshot.
    fn from_repo(repo: Arc<ReadonlyRepo>) -> Self {
        Self {
            repo,
            workspace_root: None,
        }
    }

    /// Loads a workspace and its current repository operation through jj-lib.
    pub fn load(
        settings: &UserSettings,
        workspace_path: impl AsRef<Path>,
    ) -> Result<Self, JjRepositoryError> {
        let workspace_path = workspace_path.as_ref();
        let factories = default_backend_factories();
        let working_copy_factories = default_working_copy_factories();
        let workspace = Workspace::load(
            settings,
            workspace_path,
            &factories,
            &working_copy_factories,
        )
        .map_err(|error| JjRepositoryError::Load(error.to_string()))?;
        let repo = block_on(workspace.repo_loader().load_at_head())
            .map_err(|error| JjRepositoryError::Load(error.to_string()))?;
        Ok(Self {
            repo,
            workspace_root: Some(workspace.workspace_root().to_owned()),
        })
    }

    pub fn repo(&self) -> &Arc<ReadonlyRepo> {
        &self.repo
    }

    pub fn workspace_root(&self) -> Option<&Path> {
        self.workspace_root.as_deref()
    }

    pub fn operation_id(&self) -> String {
        self.repo.operation().id().hex()
    }

    /// Returns this repository reloaded at a JJ operation in its ancestry.
    pub fn at_operation(
        &self,
        id: &jj_lib::op_store::OperationId,
    ) -> Result<Self, JjRepositoryError> {
        let operation = block_on(self.repo.loader().load_operation(id))
            .map_err(|error| JjRepositoryError::Operation(error.to_string()))?;
        let repo = block_on(self.repo.loader().load_at(&operation))
            .map_err(|error| JjRepositoryError::Operation(error.to_string()))?;
        Ok(Self {
            repo,
            workspace_root: self.workspace_root.clone(),
        })
    }

    /// Returns the reachable operation ancestry in root-to-current order.
    pub fn operation_ancestry(&self) -> Result<Vec<Operation>, JjRepositoryError> {
        let mut ordered = Vec::new();
        let mut seen = BTreeSet::new();
        collect_operations(self.repo.operation(), &mut seen, &mut ordered)
            .map_err(JjRepositoryError::Operation)?;
        Ok(ordered)
    }
}
/// Converts a repository handle into an immutable JJ observation snapshot.
pub trait JjRepositorySource {
    fn snapshot(&self) -> JjRepository;
}

impl JjRepositorySource for JjRepository {
    fn snapshot(&self) -> JjRepository {
        self.clone()
    }
}

impl JjRepositorySource for Arc<ReadonlyRepo> {
    fn snapshot(&self) -> JjRepository {
        JjRepository::from_repo(self.clone())
    }
}

fn collect_operations(
    operation: &Operation,
    seen: &mut BTreeSet<OperationId>,
    ordered: &mut Vec<Operation>,
) -> Result<(), String> {
    let operation_id = operation.id().clone();
    if !seen.insert(operation_id) {
        return Ok(());
    }
    let parents = block_on(operation.parents()).map_err(|error| error.to_string())?;
    for parent in parents {
        collect_operations(&parent, seen, ordered)?;
    }
    ordered.push(operation.clone());
    Ok(())
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JjAdapterError {
    #[error(transparent)]
    Repository(#[from] JjRepositoryError),
    #[error("could not load JJ commit: {0}")]
    Commit(String),
    #[error("provenance core rejected JJ transaction: {0}")]
    Core(#[source] provenance_core::Error),
}

/// Adapter that translates one JJ repository snapshot into one generic transaction.
#[derive(Clone, Copy, Debug, Default)]
pub struct JjAdapter;

impl Adapter<JjModel> for JjAdapter {
    type Input = JjRepository;
    type Error = JjAdapterError;

    fn transaction(&self, input: Self::Input) -> Result<Transaction<JjModel>, Self::Error> {
        observe_snapshot(&input)
    }

    fn core_error(&self, error: provenance_core::Error) -> Self::Error {
        JjAdapterError::Core(error)
    }
}

/// Observes the current JJ world as a provenance-core transaction.
#[tracing::instrument(level = "debug", skip_all, err)]
pub fn observe_repository<R: JjRepositorySource>(
    repo: &R,
) -> Result<Transaction<JjModel>, JjAdapterError> {
    JjAdapter.transaction(repo.snapshot())
}

fn observe_snapshot(repo: &JjRepository) -> Result<Transaction<JjModel>, JjAdapterError> {
    let current_operation = repo.repo.operation().clone();
    let current_operation_id = current_operation.id().clone();
    let current_operation_name = current_operation_id.hex();
    let operations = repo.operation_ancestry()?;

    let mut commits = BTreeMap::<CommitId, Commit>::new();
    let mut visible_commits = BTreeSet::<CommitId>::new();
    for commit_id in repo.repo.view().heads() {
        collect_commit(
            repo.repo.store(),
            commit_id,
            true,
            &mut visible_commits,
            &mut commits,
        )?;
    }

    for operation in &operations {
        for commit_id in operation.all_referenced_commit_ids() {
            collect_commit(
                repo.repo.store(),
                commit_id,
                false,
                &mut visible_commits,
                &mut commits,
            )?;
        }
    }

    for commit_id in repo.repo.view().wc_commit_ids().values() {
        collect_commit(
            repo.repo.store(),
            commit_id,
            true,
            &mut visible_commits,
            &mut commits,
        )?;
    }

    let mut current_commits_by_change = BTreeMap::<ChangeId, CommitId>::new();
    // JJ records rewritten/new commit tips in each operation. Prefer the
    // newest operation's commit for a change; lexical ordering is only a
    // deterministic fallback for commits without operation metadata.
    for operation in &operations {
        if let Some(predecessors) = operation.store_operation().commit_predecessors.as_ref() {
            for commit_id in predecessors.keys() {
                if let Some(commit) = commits.get(commit_id) {
                    current_commits_by_change.insert(commit.change_id().clone(), commit_id.clone());
                }
            }
        }
    }
    for (commit_id, commit) in &commits {
        if visible_commits.contains(commit_id) {
            let change_id = commit.change_id().clone();
            current_commits_by_change
                .entry(change_id)
                .and_modify(|existing| {
                    if commit_id > existing {
                        *existing = commit_id.clone();
                    }
                })
                .or_insert_with(|| commit_id.clone());
        }
    }
    for (commit_id, commit) in &commits {
        let change_id = commit.change_id().clone();
        current_commits_by_change
            .entry(change_id)
            .or_insert_with(|| commit_id.clone());
    }

    let schema = jj_schema_key();
    let mut entity_observations = Vec::new();
    let mut subjects = BTreeSet::new();
    let mut relations = BTreeSet::new();

    for operation in &operations {
        let operation_id = operation.id().hex();
        let address = jj_operation(operation_id.clone());
        subjects.insert(external(address.clone()));
        entity_observations.push(EntityObservation {
            entity: address,
            schema: schema.clone(),
            attributes: attributes([
                ("operation_id", string_value(operation_id)),
                (
                    "description",
                    string_value(operation.metadata().description.clone()),
                ),
            ]),
        });

        for parent_id in operation.parent_ids() {
            let parent_address = jj_operation(parent_id.hex());
            relations.insert(jj_relation(
                "follows",
                jj_operation(operation.id().hex()),
                parent_address,
            ));
        }

        if let Some(predecessors) = operation.store_operation().commit_predecessors.as_ref() {
            for (commit_id, predecessor_ids) in predecessors {
                let commit_address = jj_commit_address(commit_id.hex());
                relations.insert(jj_relation(
                    "produced",
                    jj_operation(operation.id().hex()),
                    commit_address.clone(),
                ));
                for predecessor_id in predecessor_ids {
                    relations.insert(jj_relation(
                        "predecessor-of",
                        commit_address.clone(),
                        jj_commit_address(predecessor_id.hex()),
                    ));
                }
            }
        }
    }

    for (commit_id, commit) in &commits {
        let change_id = commit.change_id().reverse_hex();
        let commit_id_name = commit_id.hex();
        let commit_address = jj_commit_address(commit_id_name.clone());
        let change_address = jj_change(change_id.clone());
        subjects.insert(external(commit_address.clone()));
        subjects.insert(external(change_address.clone()));
        entity_observations.push(EntityObservation {
            entity: commit_address.clone(),
            schema: schema.clone(),
            attributes: attributes([
                ("commit_id", string_value(commit_id_name.clone())),
                ("change_id", string_value(change_id)),
                ("description", string_value(commit.description().to_owned())),
                (
                    "author",
                    string_value(format!(
                        "{} <{}>",
                        commit.author().name,
                        commit.author().email
                    )),
                ),
                (
                    "timestamp",
                    string_value(format_timestamp(commit.author().timestamp)),
                ),
            ]),
        });
        relations.insert(jj_relation(
            "belongs-to-change",
            commit_address,
            change_address,
        ));
    }

    for (change_id, commit_id) in &current_commits_by_change {
        let current_commit = jj_commit_address(commit_id.hex());
        let change_id_name = change_id.reverse_hex();
        let change_address = jj_change(change_id_name.clone());
        entity_observations.push(EntityObservation {
            entity: change_address.clone(),
            schema: schema.clone(),
            attributes: attributes([
                ("change_id", string_value(change_id_name)),
                ("current_commit", Value::Entity(external(current_commit))),
            ]),
        });
    }

    for (workspace_id, commit_id) in repo.repo.view().wc_commit_ids() {
        let workspace_id = workspace_id.as_str().to_owned();
        let workspace_address = jj_workspace(workspace_id.clone());
        let commit_address = jj_commit_address(commit_id.hex());
        subjects.insert(external(workspace_address.clone()));
        subjects.insert(external(commit_address.clone()));
        entity_observations.push(EntityObservation {
            entity: workspace_address.clone(),
            schema: schema.clone(),
            attributes: attributes([("workspace_id", string_value(workspace_id))]),
        });
        relations.insert(jj_relation(
            "contains",
            workspace_address.clone(),
            commit_address.clone(),
        ));
        relations.insert(jj_relation(
            "currently-at",
            workspace_address,
            commit_address,
        ));
    }

    let source_parents = current_operation
        .parent_ids()
        .iter()
        .map(|parent| jj_operation(parent.hex()))
        .collect();

    let source = SourceOperation {
        id: jj_operation(current_operation_name.clone()),
        parents: source_parents,
    };

    let intents = vec![
        Intent::RegisterSchema(jj_schema()),
        Intent::RegisterNamedQuery(jj_why_query()),
    ]
    .into_iter()
    .chain(entity_observations.into_iter().map(Intent::ObserveEntity))
    .chain(std::iter::once(Intent::RecordEvent(EventIntent {
        session: None,
        actor: None,
        additional_parents: BTreeSet::new(),
        subjects,
        relations,
        requires: BTreeSet::new(),
        attributes: Attributes::new(),
    })))
    .collect();

    Ok(Transaction {
        seed: observation_seed(&current_operation_name),
        source: Some(source),
        intents,
        attributes: Attributes::new(),
    })
}

fn collect_commit(
    store: &Arc<jj_lib::store::Store>,
    commit_id: &CommitId,
    visible: bool,
    visible_commits: &mut BTreeSet<CommitId>,
    commits: &mut BTreeMap<CommitId, Commit>,
) -> Result<(), JjAdapterError> {
    if visible {
        visible_commits.insert(commit_id.clone());
    }
    if commits.contains_key(commit_id) {
        return Ok(());
    }
    let commit = store
        .get_commit(commit_id)
        .map_err(|error| JjAdapterError::Commit(error.to_string()))?;
    let parents = commit.parent_ids().to_vec();
    commits.insert(commit_id.clone(), commit);
    for parent_id in parents {
        collect_commit(store, &parent_id, visible, visible_commits, commits)?;
    }
    Ok(())
}

fn attributes(
    entries: impl IntoIterator<Item = (&'static str, Value<JjModel>)>,
) -> Attributes<JjModel> {
    entries
        .into_iter()
        .map(|(name, value)| (FieldName::from(name), value))
        .collect()
}

fn jj_relation(
    name: &'static str,
    from: impl Into<provenance_core::EntityAddress<JjModel>>,
    to: impl Into<provenance_core::EntityAddress<JjModel>>,
) -> Relation<JjModel> {
    Relation {
        schema: jj_schema_key(),
        relation_type: RelationType::new(JJ_NAMESPACE, name),
        from: external(from.into()),
        to: external(to.into()),
        attributes: Attributes::new(),
    }
}

fn format_timestamp(timestamp: jj_lib::backend::Timestamp) -> String {
    format!("{}@{}", timestamp.timestamp.0, timestamp.tz_offset)
}
