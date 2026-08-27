use std::fs;
use std::path::Path;
use std::process::Command;

use gix::hash::Kind;
use jj_lib::backend::CommitId;
use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::object_id::ObjectId;
use jj_lib::repo::Repo;
use jj_lib::settings::UserSettings;
use jj_lib::workspace::Workspace;
use provenance_core::{DefaultRules, Kernel, Service, State};
use provenance_jj::{JjAdapter, JjIdentity, JjModel, JjRepository, JjRuntime, ingest_repository};
use tempfile::TempDir;

pub(crate) type JjService = Service<JjModel, JjIdentity, DefaultRules, JjAdapter, JjRuntime>;

pub(crate) struct Fixture {
    pub(crate) dir: TempDir,
    settings: UserSettings,
}

impl Fixture {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().expect("temporary repository");
        let settings = test_settings();
        init_jj(dir.path(), &settings);
        Self { dir, settings }
    }

    pub(crate) fn write_file(&self, name: &str, contents: &str) {
        fs::write(self.dir.path().join(name), contents).expect("write fixture file");
    }

    pub(crate) fn commit(&self, message: &str) {
        run_jj(self.dir.path(), ["commit", "-m", message]);
    }
    #[allow(dead_code)]
    pub(crate) fn root_operation_id(&self) -> String {
        self.repository()
            .operation_ancestry()
            .expect("JJ operation ancestry")
            .into_iter()
            .find(|operation| {
                operation
                    .id()
                    .hex()
                    .chars()
                    .any(|character| character != '0')
            })
            .expect("non-root JJ operation")
            .id()
            .hex()
    }

    #[allow(dead_code)]
    pub(crate) fn restore_operation(&self, operation_id: &str) {
        run_jj(self.dir.path(), ["operation", "restore", operation_id]);
    }

    #[allow(dead_code)]
    pub(crate) fn abandon_operations_since(&self, operation_id: &str) {
        let range = format!("{operation_id}..@-");
        run_jj(self.dir.path(), ["operation", "abandon", range.as_str()]);
    }

    #[allow(dead_code)]
    pub(crate) fn gc(&self) {
        run_jj(self.dir.path(), ["util", "gc", "--expire", "now"]);
    }

    pub(crate) fn repository(&self) -> JjRepository {
        JjRepository::load(&self.settings, self.dir.path()).expect("load JJ repository")
    }
    #[allow(dead_code)]
    pub(crate) fn squash(&self) {
        run_jj(self.dir.path(), ["squash"]);
    }

    pub(crate) fn service(&self) -> JjService {
        let repository = self.repository();
        Service {
            kernel: Kernel::new(JjIdentity),
            adapter: JjAdapter,
            runtime: JjRuntime::new(repository.repo().clone()),
            state: State::new(),
        }
    }
    pub(crate) fn ingest(&self, service: &mut JjService) {
        ingest_repository(service, &self.repository()).expect("ingest JJ ancestry");
    }

    pub(crate) fn head_commit(&self) -> String {
        let repository = self.repository();
        let mut commit_id = repository
            .repo()
            .view()
            .wc_commit_ids()
            .values()
            .next()
            .expect("JJ workspace commit")
            .clone();
        loop {
            let commit = repository
                .repo()
                .store()
                .get_commit(&commit_id)
                .expect("JJ workspace commit object");
            if !pollster::block_on(commit.is_empty(repository.repo().as_ref()))
                .expect("JJ commit emptiness")
            {
                return commit.id().hex();
            }
            commit_id = commit
                .parent_ids()
                .first()
                .cloned()
                .expect("non-root commit");
        }
    }
    #[allow(dead_code)]
    pub(crate) fn commit_exists(&self, commit_id: &str) -> bool {
        let commit_id = CommitId::try_from_hex(commit_id.as_bytes()).expect("valid JJ commit id");
        self.repository()
            .repo()
            .store()
            .get_commit(&commit_id)
            .is_ok()
    }

    #[allow(dead_code)]
    pub(crate) fn git_prune(&self) {
        run_git(
            self.dir.path(),
            ["reflog", "expire", "--expire=now", "--all"],
        );
        run_git(self.dir.path(), ["gc", "--prune=now", "--aggressive"]);
    }
}

fn init_jj(directory: &Path, settings: &UserSettings) {
    pollster::block_on(Workspace::init_internal_git(
        settings,
        directory,
        Kind::Sha1,
    ))
    .expect("initialize jj");
}

fn test_settings() -> UserSettings {
    let mut config = StackedConfig::with_defaults();
    let layer = ConfigLayer::parse(
        ConfigSource::CommandArg,
        r#"
[user]
name = "Provenance Test"
email = "provenance@example.invalid"
"#,
    )
    .expect("test config");
    config.add_layer(layer);
    UserSettings::from_config(config).expect("JJ user settings")
}

fn run_jj<const N: usize>(directory: &Path, args: [&str; N]) {
    let status = Command::new("jj")
        .args(["--config", "user.name=Provenance Test"])
        .args(["--config", "user.email=provenance@example.invalid"])
        .arg("-R")
        .arg(directory)
        .args(args)
        .status()
        .expect("run jj");
    assert!(status.success(), "jj command failed: {args:?}");
}
#[allow(dead_code)]
fn run_git<const N: usize>(directory: &Path, args: [&str; N]) {
    let status = Command::new("git")
        .arg("--git-dir")
        .arg(directory.join(".jj").join("repo").join("store").join("git"))
        .args(args)
        .status()
        .expect("run git");
    assert!(status.success(), "git command failed: {args:?}");
}
