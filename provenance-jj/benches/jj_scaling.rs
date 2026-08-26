#![allow(clippy::too_many_arguments)]

#[cfg(any(
    feature = "sqlite-index",
    feature = "duckdb-index",
    feature = "doltlite-index",
    feature = "turso-index",
    feature = "lbug-index",
    feature = "redb-index",
    feature = "heed-index",
    feature = "mnestic-index",
))]
mod enabled {
    use std::env;
    use std::fmt::Write as _;
    use std::fs;
    use std::hint::black_box;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::LazyLock;
    use std::time::{Duration, Instant};

    use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group};
    use jj_lib::backend::CommitId;
    use jj_lib::commit::Commit;
    use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
    use jj_lib::merged_tree::MergedTree;
    use jj_lib::object_id::ObjectId;
    use jj_lib::op_store::RefTarget;
    use jj_lib::ref_name::RefNameBuf;
    use jj_lib::repo::Repo;
    use jj_lib::settings::UserSettings;
    use jj_lib::transaction::Transaction;
    use provenance_core::{Kernel, QueryEngine, State};
    use provenance_indexing_backend::BackendIndex;
    use provenance_jj::{
        DirectoryProvenanceStore, JjAdapter, JjIdentity, JjRepository, JjRuntime, JjService,
        ingest_repository, observe_repository, open_service, why_jj_commit,
    };
    use tempfile::TempDir;

    const BASE_SUBMISSIONS: usize = 1_000_000;
    const BRANCHES_AT_FULL_SCALE: usize = 200;
    const FILES_AT_FULL_SCALE: usize = 100;
    const DIRECTORIES_AT_FULL_SCALE: usize = 10;
    const DEFAULT_FIXTURE_ROOT: &str = "/tmp/provenance-jj-bench-fixtures";
    const STACK_WIDTH: usize = 4;

    const SPECS: [RepositorySpec; 5] = [
        RepositorySpec {
            name: "repo1",
            scale_tenths: 1,
        },
        RepositorySpec {
            name: "repo2",
            scale_tenths: 2,
        },
        RepositorySpec {
            name: "repo3",
            scale_tenths: 3,
        },
        RepositorySpec {
            name: "repo4",
            scale_tenths: 4,
        },
        RepositorySpec {
            name: "repo5",
            scale_tenths: 5,
        },
    ];

    static FIXTURES: LazyLock<FixtureSet> = LazyLock::new(|| {
        let fixture_started = Instant::now();
        let max_submissions = env::var("PROVENANCE_JJ_BENCH_MAX_SUBMISSIONS")
            .ok()
            .map(|value| {
                value.parse::<usize>().unwrap_or_else(|_| {
                    panic!("invalid PROVENANCE_JJ_BENCH_MAX_SUBMISSIONS: {value}")
                })
            });
        let selected_repositories = env::var("PROVENANCE_JJ_BENCH_REPOS").ok().map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        });
        let repositories = SPECS
            .into_iter()
            .filter(|spec| {
                selected_repositories
                    .as_ref()
                    .map_or(true, |names| names.iter().any(|name| name == spec.name))
            })
            .map(|spec| build_repository(spec, max_submissions))
            .collect::<Vec<_>>();
        assert!(
            !repositories.is_empty(),
            "PROVENANCE_JJ_BENCH_REPOS selected no known repositories"
        );
        eprintln!(
            "JJ benchmark fixture setup: {:.3}s",
            fixture_started.elapsed().as_secs_f64()
        );
        let dashboard = write_dashboard(&repositories);
        eprintln!("JJ benchmark dashboard: {}", dashboard.display());
        FixtureSet {
            repositories,
            _dashboard: dashboard,
        }
    });
    #[derive(Clone, Copy, Debug)]
    struct RepositorySpec {
        name: &'static str,
        scale_tenths: usize,
    }

    impl RepositorySpec {
        fn requested_submissions(self) -> usize {
            BASE_SUBMISSIONS * self.scale_tenths / 5
        }

        fn branches(self) -> usize {
            BRANCHES_AT_FULL_SCALE * self.scale_tenths / 5
        }

        fn files(self) -> usize {
            FILES_AT_FULL_SCALE * self.scale_tenths / 5
        }

        fn directories(self) -> usize {
            DIRECTORIES_AT_FULL_SCALE * self.scale_tenths / 5
        }
    }

    struct FixtureSet {
        repositories: Vec<RepositoryFixture>,
        _dashboard: PathBuf,
    }

    struct BenchmarkDirectory {
        path: PathBuf,
    }

    impl BenchmarkDirectory {
        fn persistent(path: PathBuf) -> Self {
            fs::create_dir_all(&path).expect("create persistent benchmark fixture directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    struct RepositoryFixture {
        spec: RepositorySpec,
        submissions: usize,
        branches: usize,
        files: usize,
        directories: usize,
        unmerged_branches: usize,
        simple_branches: usize,
        stacked_branches: usize,
        _repository_dir: BenchmarkDirectory,
        _baseline_repository: JjRepository,
        _current_repository: JjRepository,
        _baseline_store_dir: BenchmarkDirectory,
        _current_store_dir: BenchmarkDirectory,
        baseline_store: DirectoryProvenanceStore,
        current_store: DirectoryProvenanceStore,
        baseline_operations: usize,
        current_operations: usize,
        query_cases: Vec<QueryCase>,
    }

    struct QueryCase {
        name: &'static str,
        commit_id: String,
    }

    fn fixtures() -> &'static FixtureSet {
        &FIXTURES
    }

    fn build_repository(spec: RepositorySpec, max_submissions: Option<usize>) -> RepositoryFixture {
        let branches = spec.branches();
        let files = spec.files();
        let directories = spec.directories();
        let submissions = effective_submissions(spec, max_submissions);
        let ordinary_submissions = submissions / 2;
        let branch_budget = ordinary_submissions / branches;
        assert_eq!(ordinary_submissions % branches, 0);
        assert!(
            branch_budget >= 2,
            "benchmark fixture needs at least two commits per branch"
        );

        let unmerged_branches = branches / 10;
        let simple_branches = branches / 2;
        let stacked_branches = branches - unmerged_branches - simple_branches;
        assert_eq!(stacked_branches % STACK_WIDTH, 0);
        assert_eq!(files % directories, 0);

        let (repository_dir, cached_baseline_operation) = repository_directory(spec, submissions);
        if let Some(baseline_operation) = cached_baseline_operation {
            return load_cached_repository(
                spec,
                submissions,
                branches,
                files,
                directories,
                unmerged_branches,
                simple_branches,
                stacked_branches,
                repository_dir,
                baseline_operation,
            );
        }

        eprintln!(
            "building {}: submissions={} ({} ordinary, {} same-change amendments), branches={} ({} unmerged, {} simple, {} stacked), files={}, directories={}",
            spec.name,
            submissions,
            ordinary_submissions,
            submissions - ordinary_submissions,
            branches,
            unmerged_branches,
            simple_branches,
            stacked_branches,
            files,
            directories,
        );

        init_jj(repository_dir.path());
        populate_files(repository_dir.path(), files, directories);

        let settings = benchmark_settings();
        let loaded = JjRepository::load(&settings, repository_dir.path())
            .expect("load initialized JJ benchmark repository");
        let mut repo = loaded.repo().clone();
        let base_id = non_empty_head(&repo);
        let base_commit = repo
            .store()
            .get_commit(&base_id)
            .expect("load benchmark base commit");
        let tree = base_commit.tree();
        let main_name: RefNameBuf = "main".into();
        repo = set_bookmark(
            repo,
            &main_name,
            base_id.clone(),
            "create benchmark main bookmark",
        );
        let mut main_head = base_id;
        let mut branch_number = 0usize;
        let mut unmerged_heads = Vec::with_capacity(unmerged_branches);

        for _ in 0..unmerged_branches {
            let branch_name: RefNameBuf = format!("unmerged-{branch_number:04}").into();
            let (next_repo, branch_head) = create_branch(
                repo,
                &tree,
                main_head.clone(),
                branch_budget,
                branch_name,
                &format!("{} unmerged branch", spec.name),
            );
            repo = next_repo;
            unmerged_heads.push(branch_head.id().clone());
            branch_number += 1;
        }

        for _ in 0..simple_branches {
            let branch_name: RefNameBuf = format!("simple-{branch_number:04}").into();
            let mut transaction = repo.start_transaction();
            let branch_head = write_chain(
                &mut transaction,
                main_head.clone(),
                &tree,
                branch_budget - 1,
                &format!("{} simple branch", spec.name),
            );
            transaction.repo_mut().set_local_bookmark_target(
                branch_name.as_ref(),
                RefTarget::normal(branch_head.id().clone()),
            );
            let merge = write_merge(
                &mut transaction,
                main_head.clone(),
                branch_head.id().clone(),
                &tree,
                &format!("{} simple branch merge", spec.name),
            );
            transaction.repo_mut().set_local_bookmark_target(
                main_name.as_ref(),
                RefTarget::normal(merge.id().clone()),
            );
            repo = pollster::block_on(transaction.commit("benchmark simple branch"))
                .expect("publish simple branch transaction");
            main_head = merge.id().clone();
            branch_number += 1;
        }

        for stack_number in 0..(stacked_branches / STACK_WIDTH) {
            let mut stack_parent = main_head.clone();
            for depth in 0..STACK_WIDTH {
                let branch_name: RefNameBuf = format!("stack-{stack_number:04}-{depth:02}").into();
                let count = if depth + 1 == STACK_WIDTH {
                    branch_budget - 1
                } else {
                    branch_budget
                };
                let (next_repo, branch_head) = create_branch(
                    repo,
                    &tree,
                    stack_parent,
                    count,
                    branch_name,
                    &format!("{} stacked branch", spec.name),
                );
                repo = next_repo;
                stack_parent = branch_head.id().clone();
            }

            let mut transaction = repo.start_transaction();
            let merge = write_merge(
                &mut transaction,
                main_head.clone(),
                stack_parent,
                &tree,
                &format!("{} stacked branch merge", spec.name),
            );
            transaction.repo_mut().set_local_bookmark_target(
                main_name.as_ref(),
                RefTarget::normal(merge.id().clone()),
            );
            repo = pollster::block_on(transaction.commit("benchmark stacked branch merge"))
                .expect("publish stacked branch merge transaction");
            main_head = merge.id().clone();
        }

        let baseline_repository = JjRepository::load(&settings, repository_dir.path())
            .expect("load baseline JJ benchmark repository");
        let baseline_main = main_head.clone();

        let amendment_count = submissions - ordinary_submissions;
        let mut transaction = repo.start_transaction();
        let mut current = repo
            .store()
            .get_commit(&main_head)
            .expect("load main commit for amendments");
        let change_id = current.change_id().clone();
        for amendment in 0..amendment_count {
            let next = pollster::block_on(
                transaction
                    .repo_mut()
                    .new_commit(vec![current.id().clone()], current.tree())
                    .set_change_id(change_id.clone())
                    .set_predecessors(vec![current.id().clone()])
                    .set_description(format!("{} amendment {amendment:06}", spec.name))
                    .write(),
            )
            .expect("write benchmark same-change amendment");
            current = next;
        }
        transaction
            .repo_mut()
            .set_local_bookmark_target(main_name.as_ref(), RefTarget::normal(current.id().clone()));
        let workspace_name = transaction
            .repo()
            .view()
            .wc_commit_ids()
            .keys()
            .next()
            .cloned();
        if let Some(workspace_name) = workspace_name {
            transaction
                .repo_mut()
                .set_wc_commit(workspace_name, current.id().clone())
                .expect("move benchmark working copy");
        }
        repo = pollster::block_on(transaction.commit("benchmark same-change amendments"))
            .expect("publish benchmark amendment transaction");
        let current_repository = JjRepository::load(&settings, repository_dir.path())
            .expect("load current JJ benchmark repository");

        let baseline_store_dir = storage_directory(&repository_dir, "baseline-store");
        materialize_store_if_missing(&baseline_repository, baseline_store_dir.path());
        let current_store_dir = storage_directory(&repository_dir, "current-store");
        materialize_store_if_missing(&current_repository, current_store_dir.path());
        let baseline_store = DirectoryProvenanceStore::open(baseline_store_dir.path())
            .expect("open baseline provenance store");
        let current_store = DirectoryProvenanceStore::open(current_store_dir.path())
            .expect("open current provenance store");
        let baseline_operations = baseline_store
            .reachable_operation_count()
            .expect("count baseline provenance operations");
        let current_operations = current_store
            .reachable_operation_count()
            .expect("count current provenance operations");
        assert_eq!(current_operations, baseline_operations + 1);
        write_fixture_metadata(repository_dir.path(), &baseline_repository.operation_id());

        let branch_query = unmerged_heads
            .first()
            .cloned()
            .unwrap_or_else(|| main_head.clone());
        let query_cases = vec![
            QueryCase {
                name: "main-head",
                commit_id: current.id().hex(),
            },
            QueryCase {
                name: "pre-amend",
                commit_id: baseline_main.hex(),
            },
            QueryCase {
                name: "unmerged-branch",
                commit_id: branch_query.hex(),
            },
        ];

        let _ = repo;
        RepositoryFixture {
            spec,
            submissions,
            branches,
            files,
            directories,
            unmerged_branches,
            simple_branches,
            stacked_branches,
            _repository_dir: repository_dir,
            _baseline_repository: baseline_repository,
            _current_repository: current_repository,
            _baseline_store_dir: baseline_store_dir,
            _current_store_dir: current_store_dir,
            baseline_store,
            current_store,
            baseline_operations,
            current_operations,
            query_cases,
        }
    }
    fn repository_directory(
        spec: RepositorySpec,
        submissions: usize,
    ) -> (BenchmarkDirectory, Option<String>) {
        let root = env::var_os("PROVENANCE_JJ_BENCH_FIXTURE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_FIXTURE_ROOT));
        let path = root.join(spec.name).join(submissions.to_string());
        let metadata = path.join("fixture.meta");
        if metadata.exists()
            && path.join(".jj").exists()
            && path.join("baseline-store/heads/manifest").exists()
            && path.join("current-store/heads/manifest").exists()
        {
            let baseline_operation =
                fs::read_to_string(metadata).expect("read benchmark fixture metadata");
            return (
                BenchmarkDirectory::persistent(path),
                Some(baseline_operation.trim().to_owned()),
            );
        }
        if path.exists() {
            fs::remove_dir_all(&path).expect("remove incomplete benchmark fixture");
        }
        (BenchmarkDirectory::persistent(path), None)
    }

    fn storage_directory(
        repository_directory: &BenchmarkDirectory,
        name: &str,
    ) -> BenchmarkDirectory {
        BenchmarkDirectory::persistent(repository_directory.path().join(name))
    }

    fn materialize_store_if_missing(repository: &JjRepository, path: &Path) {
        if path.join("heads").join("manifest").exists() {
            return;
        }
        materialize_store(repository, path);
    }

    fn write_fixture_metadata(path: &Path, baseline_operation: &str) {
        fs::write(path.join("fixture.meta"), baseline_operation)
            .expect("write benchmark fixture metadata");
    }

    #[allow(clippy::too_many_arguments)]
    fn load_cached_repository(
        spec: RepositorySpec,
        submissions: usize,
        branches: usize,
        files: usize,
        directories: usize,
        unmerged_branches: usize,
        simple_branches: usize,
        stacked_branches: usize,
        repository_dir: BenchmarkDirectory,
        baseline_operation_id: String,
    ) -> RepositoryFixture {
        let settings = benchmark_settings();
        let current_repository = JjRepository::load(&settings, repository_dir.path())
            .expect("load cached current JJ benchmark repository");
        let baseline_operation = current_repository
            .operation_ancestry()
            .expect("load cached JJ operation ancestry")
            .into_iter()
            .find(|operation| operation.id().hex() == baseline_operation_id)
            .expect("cached baseline operation is present");
        let baseline_repository = current_repository
            .at_operation(baseline_operation.id())
            .expect("load cached baseline JJ repository");
        let baseline_store_dir = storage_directory(&repository_dir, "baseline-store");
        let current_store_dir = storage_directory(&repository_dir, "current-store");
        let baseline_store = DirectoryProvenanceStore::open(baseline_store_dir.path())
            .expect("open cached baseline provenance store");
        let current_store = DirectoryProvenanceStore::open(current_store_dir.path())
            .expect("open cached current provenance store");
        let baseline_operations = baseline_store
            .reachable_operation_count()
            .expect("count cached baseline operations");
        let current_operations = current_store
            .reachable_operation_count()
            .expect("count cached current operations");
        assert_eq!(current_operations, baseline_operations + 1);

        let current_commit = non_empty_head(current_repository.repo());
        let baseline_commit = non_empty_head(baseline_repository.repo());
        let branch_query = current_repository
            .repo()
            .view()
            .heads()
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| current_commit.clone());
        eprintln!(
            "reusing {}: submissions={}, baseline_operations={}, current_operations={}",
            spec.name, submissions, baseline_operations, current_operations
        );
        RepositoryFixture {
            spec,
            submissions,
            branches,
            files,
            directories,
            unmerged_branches,
            simple_branches,
            stacked_branches,
            _repository_dir: repository_dir,
            _baseline_repository: baseline_repository,
            _current_repository: current_repository,
            _baseline_store_dir: baseline_store_dir,
            _current_store_dir: current_store_dir,
            baseline_store,
            current_store,
            baseline_operations,
            current_operations,
            query_cases: vec![
                QueryCase {
                    name: "main-head",
                    commit_id: current_commit.hex(),
                },
                QueryCase {
                    name: "pre-amend",
                    commit_id: baseline_commit.hex(),
                },
                QueryCase {
                    name: "unmerged-branch",
                    commit_id: branch_query.hex(),
                },
            ],
        }
    }

    fn effective_submissions(spec: RepositorySpec, max_submissions: Option<usize>) -> usize {
        let requested = spec.requested_submissions();
        let minimum = spec.branches() * 4;
        let Some(maximum) = max_submissions else {
            return requested;
        };
        let unit = spec.branches() * 2;
        let capped = requested.min(maximum).max(minimum);
        (capped / unit) * unit
    }

    fn create_branch(
        repo: std::sync::Arc<jj_lib::repo::ReadonlyRepo>,
        tree: &MergedTree,
        parent: CommitId,
        count: usize,
        branch_name: RefNameBuf,
        description: &str,
    ) -> (std::sync::Arc<jj_lib::repo::ReadonlyRepo>, Commit) {
        let mut transaction = repo.start_transaction();
        let head = write_chain(&mut transaction, parent, tree, count, description);
        transaction
            .repo_mut()
            .set_local_bookmark_target(branch_name.as_ref(), RefTarget::normal(head.id().clone()));
        let repo = pollster::block_on(transaction.commit("benchmark branch"))
            .expect("publish benchmark branch transaction");
        (repo, head)
    }

    fn write_chain(
        transaction: &mut Transaction,
        parent: CommitId,
        tree: &MergedTree,
        count: usize,
        description: &str,
    ) -> Commit {
        let mut parent = parent;
        let mut last = None;
        for sequence in 0..count {
            let commit = pollster::block_on(
                transaction
                    .repo_mut()
                    .new_commit(vec![parent], tree.clone())
                    .set_description(format!("{description} {sequence:06}"))
                    .write(),
            )
            .expect("write benchmark commit");
            parent = commit.id().clone();
            last = Some(commit);
        }
        last.expect("benchmark branch must contain a commit")
    }

    fn write_merge(
        transaction: &mut Transaction,
        main_parent: CommitId,
        branch_parent: CommitId,
        tree: &MergedTree,
        description: &str,
    ) -> Commit {
        pollster::block_on(
            transaction
                .repo_mut()
                .new_commit(vec![main_parent, branch_parent], tree.clone())
                .set_description(description)
                .write(),
        )
        .expect("write benchmark merge commit")
    }

    fn materialize_store(repository: &JjRepository, path: &Path) {
        let mut service =
            open_service(repository, path).expect("open benchmark provenance service");
        ingest_repository(&mut service, repository).expect("ingest benchmark JJ repository");
    }

    fn non_empty_head(repo: &std::sync::Arc<jj_lib::repo::ReadonlyRepo>) -> CommitId {
        let candidates = repo
            .view()
            .wc_commit_ids()
            .values()
            .chain(repo.view().heads().iter())
            .cloned()
            .collect::<Vec<_>>();
        for mut commit_id in candidates {
            loop {
                let commit = repo
                    .store()
                    .get_commit(&commit_id)
                    .expect("load JJ benchmark head");
                if !pollster::block_on(commit.is_empty(repo.as_ref())).expect("check JJ head") {
                    return commit_id;
                }
                let Some(parent_id) = commit.parent_ids().first() else {
                    break;
                };
                commit_id = parent_id.clone();
            }
        }
        panic!("JJ benchmark repository has no non-empty head");
    }

    fn init_jj(directory: &Path) {
        let status = Command::new("jj")
            .args(["--config", "user.name=Provenance Benchmark"])
            .args([
                "--config",
                "user.email=provenance-benchmark@example.invalid",
            ])
            .args(["git", "init", "--no-colocate"])
            .arg(directory)
            .status()
            .expect("initialize JJ benchmark repository");
        assert!(status.success(), "jj init failed");
    }

    fn populate_files(directory: &Path, files: usize, directories: usize) {
        let files_per_directory = files / directories;
        for directory_number in 0..directories {
            let directory_path = directory.join(format!("dir{directory_number:02}"));
            fs::create_dir_all(&directory_path).expect("create benchmark file directory");
            for file_number in 0..files_per_directory {
                let file_path = directory_path.join(format!("file{file_number:03}.txt"));
                fs::write(
                    file_path,
                    format!(
                        "JJ benchmark fixture directory={directory_number} file={file_number}\n"
                    ),
                )
                .expect("write benchmark fixture file");
            }
        }
        run_jj(directory, ["commit", "-m", "benchmark fixture files"]);
    }

    fn run_jj<const N: usize>(directory: &Path, args: [&str; N]) {
        let status = Command::new("jj")
            .args(["--config", "user.name=Provenance Benchmark"])
            .args([
                "--config",
                "user.email=provenance-benchmark@example.invalid",
            ])
            .arg("-R")
            .arg(directory)
            .args(args)
            .status()
            .expect("run JJ benchmark setup command");
        assert!(status.success(), "jj command failed: {args:?}");
    }

    fn benchmark_settings() -> UserSettings {
        let mut config = StackedConfig::with_defaults();
        let layer = ConfigLayer::parse(
            ConfigSource::CommandArg,
            r#"
[user]
name = "Provenance Benchmark"
email = "provenance-benchmark@example.invalid"
"#,
        )
        .expect("parse benchmark JJ settings");
        config.add_layer(layer);
        UserSettings::from_config(config).expect("build benchmark JJ settings")
    }

    fn set_bookmark(
        repo: std::sync::Arc<jj_lib::repo::ReadonlyRepo>,
        name: &RefNameBuf,
        commit_id: CommitId,
        description: &str,
    ) -> std::sync::Arc<jj_lib::repo::ReadonlyRepo> {
        let mut transaction = repo.start_transaction();
        transaction
            .repo_mut()
            .set_local_bookmark_target(name.as_ref(), RefTarget::normal(commit_id));
        pollster::block_on(transaction.commit(description)).expect("publish JJ bookmark")
    }
    fn load_authoritative_state(store_path: &Path) -> State<provenance_jj::JjModel> {
        let store = DirectoryProvenanceStore::open(store_path)
            .expect("open authoritative provenance store");
        let mut state = State::new();
        for operation in store
            .reachable_operations()
            .expect("read reachable authoritative operations")
        {
            state
                .operations
                .declare(operation.id.clone(), operation)
                .expect("declare authoritative operation");
        }
        state.rebuild().expect("rebuild authoritative state");
        state
    }

    fn query_cases(
        fixture: &RepositoryFixture,
    ) -> Vec<provenance_core::Query<provenance_jj::JjModel>> {
        fixture
            .query_cases
            .iter()
            .map(|query_case| why_jj_commit(&query_case.commit_id))
            .collect()
    }

    fn assert_query_equivalence(
        state: &State<provenance_jj::JjModel>,
        index: &BenchmarkIndex,
        query: &provenance_core::Query<provenance_jj::JjModel>,
    ) {
        let raw = state.execute(query).expect("execute raw state query");
        let indexed = index.execute(query).expect("execute indexed query");
        assert_eq!(raw, indexed, "raw and indexed query results differ");
    }

    fn open_persistent_index() -> (TempDir, BenchmarkIndex) {
        let directory = TempDir::new().expect("create persistent index directory");
        let path = directory.path().join("projection");
        let index = BenchmarkIndex::open(&path).expect("open persistent benchmark index");
        (directory, index)
    }

    fn remove_index_path(path: &Path) {
        if path.is_dir() {
            fs::remove_dir_all(path).expect("remove persistent benchmark index directory");
        } else if path.exists() {
            fs::remove_file(path).expect("remove persistent benchmark index file");
        }
    }

    fn persistent_index_path() -> (TempDir, PathBuf) {
        let directory = TempDir::new().expect("create cold index directory");
        let path = directory_path(&directory);
        (directory, path)
    }

    fn directory_path(directory: &TempDir) -> PathBuf {
        directory.path().join("projection")
    }

    fn raw_service(repository: &JjRepository) -> JjService {
        JjService {
            kernel: Kernel::new(JjIdentity),
            adapter: JjAdapter,
            runtime: JjRuntime::new(repository.repo().clone()),
            state: State::new(),
        }
    }

    fn benchmark_raw_ingest_query(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_raw_ingest_query");
        for fixture in &fixture_set.repositories {
            for query_case in &fixture.query_cases {
                let query = why_jj_commit(&query_case.commit_id);
                group.bench_with_input(
                    BenchmarkId::new(query_case.name, fixture.spec.name),
                    &query,
                    |benchmark, query| {
                        benchmark.iter(|| {
                            let mut service = raw_service(&fixture._current_repository);
                            ingest_repository(&mut service, &fixture._current_repository)
                                .expect("ingest JJ repository into raw state");
                            black_box(
                                service
                                    .state
                                    .execute(black_box(query))
                                    .expect("execute raw JJ query"),
                            );
                        });
                    },
                );
            }
        }
        group.finish();
    }

    fn benchmark_raw_observation(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_raw_observation");
        for fixture in &fixture_set.repositories {
            group.bench_with_input(
                BenchmarkId::from_parameter(fixture.spec.name),
                fixture,
                |benchmark, fixture| {
                    benchmark.iter(|| {
                        let transaction = observe_repository(&fixture._current_repository)
                            .expect("observe current JJ repository");
                        black_box(transaction.intents.len());
                    });
                },
            );
        }
        group.finish();
    }

    fn benchmark_authority_recovery(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_authority_recovery");
        for fixture in &fixture_set.repositories {
            group.bench_with_input(
                BenchmarkId::from_parameter(fixture.spec.name),
                fixture,
                |benchmark, fixture| {
                    benchmark.iter(|| {
                        let state = load_authoritative_state(fixture._current_store_dir.path());
                        black_box(state.operations.len());
                    });
                },
            );
        }
        group.finish();
    }

    fn benchmark_raw_query_performance(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_raw_state_query_performance");
        for fixture in &fixture_set.repositories {
            let state = load_authoritative_state(fixture._current_store_dir.path());
            for query_case in &fixture.query_cases {
                let query = why_jj_commit(&query_case.commit_id);
                group.bench_with_input(
                    BenchmarkId::new(query_case.name, fixture.spec.name),
                    &query,
                    |benchmark, query| {
                        benchmark.iter(|| {
                            black_box(state.execute(black_box(query)).expect("execute raw query"));
                        });
                    },
                );
            }
        }
        group.finish();
    }

    fn benchmark_persistent_query_performance(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_persistent_index_query_performance");
        for fixture in &fixture_set.repositories {
            let (directory, mut index) = open_persistent_index();
            index
                .rebuild_from(&fixture.current_store)
                .expect("build persistent benchmark query index");
            let state = load_authoritative_state(fixture._current_store_dir.path());
            for query in query_cases(fixture) {
                assert_query_equivalence(&state, &index, &query);
            }
            for query_case in &fixture.query_cases {
                let query = why_jj_commit(&query_case.commit_id);
                group.bench_with_input(
                    BenchmarkId::new(query_case.name, fixture.spec.name),
                    &query,
                    |benchmark, query| {
                        benchmark.iter(|| {
                            black_box(
                                index
                                    .execute(black_box(query))
                                    .expect("execute persistent indexed query"),
                            );
                        });
                    },
                );
            }
            black_box(directory);
        }
        group.finish();
    }

    fn benchmark_reopen_query(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_persistent_index_reopen_query");
        for fixture in &fixture_set.repositories {
            let (directory, mut index) = open_persistent_index();
            index
                .rebuild_from(&fixture.current_store)
                .expect("build persistent reopen index");
            drop(index);
            let path = directory_path(&directory);
            for query_case in &fixture.query_cases {
                let query = why_jj_commit(&query_case.commit_id);
                group.bench_with_input(
                    BenchmarkId::new(query_case.name, fixture.spec.name),
                    &query,
                    |benchmark, query| {
                        benchmark.iter(|| {
                            let index =
                                BenchmarkIndex::open(&path).expect("reopen persistent index");
                            black_box(
                                index
                                    .execute(black_box(query))
                                    .expect("execute reopened indexed query"),
                            );
                        });
                    },
                );
            }
        }
        group.finish();
    }

    fn benchmark_cold_query(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_cold_query_end_to_end");
        for fixture in &fixture_set.repositories {
            let (directory, path) = persistent_index_path();
            for query_case in &fixture.query_cases {
                let query = why_jj_commit(&query_case.commit_id);
                group.bench_with_input(
                    BenchmarkId::new(format!("raw-{}", query_case.name), fixture.spec.name),
                    &query,
                    |benchmark, query| {
                        benchmark.iter(|| {
                            let state = load_authoritative_state(fixture._current_store_dir.path());
                            black_box(
                                state
                                    .execute(black_box(query))
                                    .expect("execute cold raw query"),
                            );
                        });
                    },
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("persistent-{}", query_case.name), fixture.spec.name),
                    &query,
                    |benchmark, query| {
                        benchmark.iter(|| {
                            remove_index_path(&path);
                            let mut index =
                                BenchmarkIndex::open(&path).expect("open cold benchmark index");
                            index
                                .rebuild_from(&fixture.current_store)
                                .expect("rebuild cold benchmark index");
                            black_box(
                                index
                                    .execute(black_box(query))
                                    .expect("execute cold indexed query"),
                            );
                        });
                    },
                );
            }
            black_box(directory);
        }
        group.finish();
    }

    fn benchmark_batched_queries(c: &mut Criterion) {
        const QUERY_COUNTS: [usize; 4] = [1, 10, 100, 1_000];
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_batched_queries");
        for fixture in &fixture_set.repositories {
            let state = load_authoritative_state(fixture._current_store_dir.path());
            let queries = query_cases(fixture);
            let (directory, mut index) = open_persistent_index();
            index
                .rebuild_from(&fixture.current_store)
                .expect("build persistent batched query index");
            for query_count in QUERY_COUNTS {
                group.bench_function(
                    BenchmarkId::new(format!("raw-q{query_count}"), fixture.spec.name),
                    |benchmark| {
                        benchmark.iter(|| {
                            let mut entities = 0usize;
                            for index in 0..query_count {
                                entities += state
                                    .execute(&queries[index % queries.len()])
                                    .expect("execute batched raw query")
                                    .entities
                                    .len();
                            }
                            black_box(entities);
                        });
                    },
                );
                group.bench_function(
                    BenchmarkId::new(format!("persistent-q{query_count}"), fixture.spec.name),
                    |benchmark| {
                        benchmark.iter(|| {
                            let mut entities = 0usize;
                            for query_index in 0..query_count {
                                entities += index
                                    .execute(&queries[query_index % queries.len()])
                                    .expect("execute batched indexed query")
                                    .entities
                                    .len();
                            }
                            black_box(entities);
                        });
                    },
                );
            }
            drop(index);
            black_box(directory);
        }
        group.finish();
    }

    type BenchmarkIndex = BackendIndex<provenance_jj::JjModel>;

    fn benchmark_index_recreation(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_index_recreation_from_scratch");
        for fixture in &fixture_set.repositories {
            group.bench_with_input(
                BenchmarkId::from_parameter(fixture.spec.name),
                fixture,
                |benchmark, fixture| {
                    benchmark.iter_batched(
                        || open_persistent_index(),
                        |(directory, mut index)| {
                            index
                                .rebuild_from(&fixture.current_store)
                                .expect("recreate benchmark index");
                            black_box(
                                index
                                    .operation_count()
                                    .expect("count recreated benchmark operations"),
                            );
                            drop(index);
                            black_box(directory);
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
        group.finish();
    }

    fn benchmark_index_update(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_index_update_after_one_operation");
        for fixture in &fixture_set.repositories {
            group.bench_with_input(
                BenchmarkId::from_parameter(fixture.spec.name),
                fixture,
                |benchmark, fixture| {
                    benchmark.iter_batched(
                        || {
                            let (directory, mut index) = open_persistent_index();
                            index
                                .rebuild_from(&fixture.baseline_store)
                                .expect("build persistent benchmark baseline index");
                            (directory, index)
                        },
                        |(directory, mut index)| {
                            index
                                .update_from(&fixture.current_store)
                                .expect("update benchmark index");
                            black_box(
                                index
                                    .operation_count()
                                    .expect("count updated benchmark operations"),
                            );
                            drop(index);
                            black_box(directory);
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
        group.finish();
    }

    fn benchmark_query_performance(c: &mut Criterion) {
        let fixture_set = fixtures();
        let mut group = c.benchmark_group("jj_query_performance");
        for fixture in &fixture_set.repositories {
            let (directory, mut index) = open_persistent_index();
            index
                .rebuild_from(&fixture.current_store)
                .expect("build persistent benchmark query index");
            for query_case in &fixture.query_cases {
                let query = why_jj_commit(&query_case.commit_id);
                group.bench_with_input(
                    BenchmarkId::new(query_case.name, fixture.spec.name),
                    &query,
                    |benchmark, query| {
                        benchmark.iter(|| {
                            black_box(index.execute(black_box(query)).expect("execute JJ query"));
                        });
                    },
                );
            }
            drop(index);
            black_box(directory);
        }
        group.finish();
    }

    fn write_dashboard(repositories: &[RepositoryFixture]) -> PathBuf {
        let dashboard_dir = target_dir().join("jj-benchmark");
        fs::create_dir_all(&dashboard_dir).expect("create JJ benchmark dashboard directory");
        let mut html = String::from(
            "<!doctype html><html><head><meta charset=\"utf-8\"><title>JJ provenance benchmarks</title><style>body{font:16px system-ui,sans-serif;margin:2rem;color:#222}table{border-collapse:collapse}th,td{border:1px solid #ccc;padding:.45rem .7rem;text-align:right}th:first-child,td:first-child{text-align:left}code{background:#f3f3f3;padding:.1rem .25rem}</style></head><body><h1>JJ provenance benchmarks</h1><p>Criterion's generated graphs and static reports: <a href=\"../criterion/report/index.html\">open dashboard</a>.</p><p>Submissions count generated JJ commit objects: half ordinary commits and half same-change amendments.</p><table><thead><tr><th>Repository</th><th>Submissions</th><th>Branches</th><th>Unmerged</th><th>Simple</th><th>Stacked</th><th>Files</th><th>Directories</th><th>Baseline operations</th><th>Current operations</th></tr></thead><tbody>",
        );
        for repository in repositories {
            let _ = write!(
                html,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(repository.spec.name),
                repository.submissions,
                repository.branches,
                repository.unmerged_branches,
                repository.simple_branches,
                repository.stacked_branches,
                repository.files,
                repository.directories,
                repository.baseline_operations,
                repository.current_operations,
            );
        }
        html.push_str("</tbody></table></body></html>\n");
        let dashboard = dashboard_dir.join("index.html");
        fs::write(&dashboard, html).expect("write JJ benchmark dashboard");
        dashboard
    }

    fn target_dir() -> PathBuf {
        if let Some(target) = env::var_os("CARGO_TARGET_DIR") {
            let target = PathBuf::from(target);
            if target.is_absolute() {
                return target;
            }
            return env::current_dir()
                .expect("read benchmark working directory")
                .join(target);
        }
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target")
    }

    fn escape_html(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;")
    }

    criterion_group! {
        name = jj_scaling;
        config = Criterion::default()
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(1));
        targets = benchmark_raw_observation, benchmark_raw_ingest_query,
            benchmark_authority_recovery, benchmark_raw_query_performance,
            benchmark_persistent_query_performance, benchmark_reopen_query,
            benchmark_cold_query, benchmark_batched_queries, benchmark_index_recreation,
            benchmark_index_update, benchmark_query_performance
    }
}

#[cfg(any(
    feature = "sqlite-index",
    feature = "duckdb-index",
    feature = "doltlite-index",
    feature = "turso-index",
    feature = "lbug-index",
    feature = "redb-index",
    feature = "heed-index",
    feature = "mnestic-index",
))]
criterion::criterion_main!(enabled::jj_scaling);

#[cfg(not(any(
    feature = "sqlite-index",
    feature = "duckdb-index",
    feature = "doltlite-index",
    feature = "turso-index",
    feature = "lbug-index",
    feature = "redb-index",
    feature = "heed-index",
    feature = "mnestic-index",
)))]
fn main() {
    eprintln!("enable one database backend feature to run jj_scaling");
}
