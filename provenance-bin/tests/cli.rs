use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;
use wit_component::ComponentEncoder;

struct Fixture {
    _root: TempDir,
    repository: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("CLI fixture directory");
        let repository = root.path().join("repository");
        fs::create_dir_all(&repository).expect("repository directory");
        let status = Command::new("jj")
            .args([
                "--config",
                "user.name=Provenance",
                "--config",
                "user.email=provenance@localhost",
                "git",
                "init",
                "--colocate",
            ])
            .arg(&repository)
            .status()
            .expect("initialize JJ repository");
        assert!(status.success(), "JJ init failed");
        Self {
            _root: root,
            repository,
        }
    }

    fn commit(&self) -> String {
        fs::write(self.repository.join("README"), "CLI hook\n").expect("write fixture file");
        run_jj(&self.repository, ["commit", "-m", "CLI hook"]);
        let output = Command::new("jj")
            .args([
                "--config",
                "user.name=Provenance",
                "--config",
                "user.email=provenance@localhost",
                "-R",
                self.repository.to_str().expect("repository path is UTF-8"),
                "log",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id",
            ])
            .output()
            .expect("read fixture commit id");
        assert!(output.status.success(), "jj log failed");
        String::from_utf8(output.stdout)
            .expect("commit id is UTF-8")
            .trim()
            .to_owned()
    }
}
struct DgFixture {
    _root: TempDir,
    project: PathBuf,
    home: PathBuf,
    dg: PathBuf,
    captures: PathBuf,
    component: Option<PathBuf>,
}

impl DgFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("DG fixture directory");
        let project = root.path().join("project");
        let home = root.path().join("home");
        let captures = root.path().join("captures");
        fs::create_dir_all(&project).expect("create DG project");
        fs::create_dir_all(&home).expect("create DG HOME");
        fs::create_dir_all(&captures).expect("create DG capture directory");

        let fixture = Self {
            _root: root,
            project,
            home,
            dg: std::env::var_os("DG_BIN")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("dg")),
            captures,
            component: None,
        };
        fixture.run_dg(&["init"]);
        let status = Command::new("jj")
            .args([
                "--config",
                "user.name=Provenance",
                "--config",
                "user.email=provenance@localhost",
                "git",
                "init",
                "--colocate",
            ])
            .current_dir(&fixture.project)
            .status()
            .expect("initialize JJ alongside DG");
        assert!(status.success(), "JJ init failed");
        fixture
    }

    fn install_hooks(&mut self, component: &Path) {
        self.component = Some(component.to_owned());
        let hooks = self.project.join(".dg/hooks");
        fs::create_dir_all(&hooks).expect("create DG hook directory");
        for event in ["create", "update", "delete"] {
            let hook = hooks.join(format!("on_{event}"));
            fs::write(
                &hook,
                "#!/bin/sh\n\
                 set -eu\n\
                 payload=\"$DG_CAPTURE_DIR/$2.json\"\n\
                 result=\"$DG_CAPTURE_DIR/$2.result\"\n\
                 cat > \"$payload\"\n\
                 \"$JJ_PROV_BIN\" ingest --with-plugin \"$DG_COMPONENT\" --plugin-id dg --path \"$DG_PROJECT\" \"$@\" < \"$payload\" > \"$result\" 2>&1\n\
                ",
            )
            .expect("write DG hook");
            let mut permissions = fs::metadata(&hook).expect("stat DG hook").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&hook, permissions).expect("make DG hook executable");
        }
    }

    fn install_hooks_with_plugin(&self, component: &Path) {
        let output = Command::new(env!("CARGO_BIN_EXE_jj-prov"))
            .args([
                "hook",
                "install",
                "--plugin",
                component.to_str().expect("component path is UTF-8"),
                "--plugin-id",
                "dg",
                "--path",
                self.project.to_str().expect("project path is UTF-8"),
            ])
            .current_dir(&self.project)
            .output()
            .expect("install DG hooks");
        assert!(
            output.status.success(),
            "DG hook installation failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run_dg(&self, args: &[&str]) -> std::process::Output {
        let mut command = Command::new(&self.dg);
        command
            .arg("--root")
            .arg(&self.project)
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("XDG_DATA_HOME", self.home.join("data"))
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "dg E2E")
            .env("GIT_AUTHOR_EMAIL", "dg-e2e@example.invalid")
            .env("GIT_COMMITTER_NAME", "dg E2E")
            .env("GIT_COMMITTER_EMAIL", "dg-e2e@example.invalid")
            .current_dir(&self.project)
            .args(args);
        if let Some(component) = &self.component {
            command
                .env("JJ_PROV_BIN", env!("CARGO_BIN_EXE_jj-prov"))
                .env("DG_COMPONENT", component)
                .env("DG_CAPTURE_DIR", &self.captures)
                .env("DG_PROJECT", &self.project);
        }
        let output = command.output().expect("execute DG");
        assert!(
            output.status.success(),
            "dg {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn payload(&self, event: &str) -> Vec<u8> {
        fs::read(self.captures.join(format!("{event}.json")))
            .unwrap_or_else(|error| panic!("read {event} hook payload: {error}"))
    }

    fn hook_result(&self, event: &str) -> String {
        fs::read_to_string(self.captures.join(format!("{event}.result")))
            .unwrap_or_else(|error| panic!("read {event} hook result: {error}"))
    }
}

fn run_jj<const N: usize>(repository: &Path, args: [&str; N]) {
    let status = Command::new("jj")
        .args([
            "--config",
            "user.name=Provenance",
            "--config",
            "user.email=provenance@localhost",
            "-R",
            repository.to_str().expect("repository path is UTF-8"),
        ])
        .args(args)
        .status()
        .expect("run jj");
    assert!(status.success(), "jj command failed: {args:?}");
}

fn plugin_component(package: &str, artifact: &str) -> Vec<u8> {
    let target = TempDir::new().expect("plugin target directory");
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "-p",
            package,
            "--release",
            "--target",
            "wasm32-unknown-unknown",
            "--target-dir",
        ])
        .arg(target.path())
        .status()
        .expect("build plugin");
    assert!(status.success(), "plugin build failed");

    let module_path = target
        .path()
        .join(format!("wasm32-unknown-unknown/release/{artifact}.wasm"));
    let module = fs::read(&module_path).expect("read plugin");
    ComponentEncoder::default()
        .module(&module)
        .expect("read plugin component metadata")
        .encode()
        .expect("encode plugin component")
}

fn hello_component() -> Vec<u8> {
    plugin_component("provenance-plugin-hello", "provenance_plugin_hello")
}

fn dg_component() -> Vec<u8> {
    plugin_component("provenance-plugin-dg", "provenance_plugin_dg")
}

#[test]
fn hook_ingestion_and_why_use_the_extracted_binary() {
    let fixture = Fixture::new();
    let commit = fixture.commit();
    let component = fixture._root.path().join("hello.component.wasm");
    fs::write(&component, hello_component()).expect("write plugin component");

    let payload = serde_json::json!({
        "current-commit": {
            "entity": {"namespace": "jj", "kind": "commit", "id": commit}
        }
    })
    .to_string();
    let binary = env!("CARGO_BIN_EXE_jj-prov");
    let hook_args = [
        "ingest",
        "--with-plugin",
        component.to_str().expect("component path is UTF-8"),
        "--plugin-id",
        "hello-tracker",
        "--path",
        fixture
            .repository
            .to_str()
            .expect("repository path is UTF-8"),
        "--event",
        "create",
        "--id",
        "turn-1",
    ];

    let mut first = Command::new(binary);
    first
        .args(hook_args)
        .current_dir(&fixture.repository)
        .env("BD_BIN", "missing-bd-for-cli-test");
    let first = run_with_input(&mut first, payload.as_bytes());
    assert!(first.status.success(), "plugin hook failed: {first:?}");
    assert!(String::from_utf8_lossy(&first.stdout).contains("hello-tracker"));

    let mut second = Command::new(binary);
    second
        .args(hook_args)
        .current_dir(&fixture.repository)
        .env("BD_BIN", "missing-bd-for-cli-test");
    let second = run_with_input(&mut second, payload.as_bytes());
    assert!(
        second.status.success(),
        "plugin hook replay failed: {second:?}"
    );

    let why = Command::new(binary)
        .args([
            "why",
            &format!("jj://commit/{commit}"),
            "--path",
            fixture
                .repository
                .to_str()
                .expect("repository path is UTF-8"),
        ])
        .current_dir(&fixture.repository)
        .output()
        .expect("run why command");
    assert!(why.status.success(), "why failed: {:?}", why.stderr);
    let why_output = String::from_utf8(why.stdout).expect("why output is UTF-8");
    assert!(why_output.contains("@chronicle 1"));
    assert!(why_output.contains("implemented-by"));
}

#[test]
fn dg_hook_arguments_are_ingested_by_the_extracted_binary() {
    let fixture = Fixture::new();
    fixture.commit();
    let component = fixture._root.path().join("dg.component.wasm");
    fs::write(&component, dg_component()).expect("write dg component");
    let payload = serde_json::json!({
        "path": "docs/adr-001.md",
        "body": "# Record a decision",
        "frontmatter": {"status": "proposed", "tags": ["architecture"]},
        "sections": []
    })
    .to_string();
    let mut command = Command::new(env!("CARGO_BIN_EXE_jj-prov"));
    command
        .args([
            "ingest",
            "--with-plugin",
            component.to_str().expect("component path is UTF-8"),
            "--plugin-id",
            "dg",
            "--path",
            fixture
                .repository
                .to_str()
                .expect("repository path is UTF-8"),
            "ADR-001",
            "create",
        ])
        .current_dir(&fixture.repository);
    let result = run_with_input(&mut command, payload.as_bytes());
    assert!(result.status.success(), "dg hook failed: {result:?}");
    assert!(String::from_utf8_lossy(&result.stdout).contains("ingested dg hook source"));
}

#[test]
fn dg_plugin_installs_and_runs_real_document_hooks() {
    let fixture = DgFixture::new();
    let component = fixture._root.path().join("dg component.wasm");
    fs::write(&component, dg_component()).expect("write DG component");

    fixture.install_hooks_with_plugin(&component);
    let hook_contents = ["create", "update", "delete"]
        .into_iter()
        .map(|event| {
            let path = fixture.project.join(format!(".dg/hooks/on_{event}"));
            let contents = fs::read_to_string(&path).expect("read installed DG hook");
            let mode = fs::metadata(&path)
                .expect("stat installed DG hook")
                .permissions()
                .mode();
            assert_ne!(mode & 0o111, 0, "installed DG hook must be executable");
            assert!(contents.contains("ingest --with-plugin"));
            assert!(contents.contains("--plugin-id dg"));
            (event, contents)
        })
        .collect::<Vec<_>>();

    fixture.install_hooks_with_plugin(&component);
    for (event, contents) in hook_contents {
        let path = fixture.project.join(format!(".dg/hooks/on_{event}"));
        assert_eq!(
            fs::read_to_string(path).expect("reread installed DG hook"),
            contents
        );
    }

    let payload = serde_json::json!({
        "path": "docs/adr-001.md",
        "body": "# Installed hook",
        "frontmatter": {"status": "proposed", "tags": []},
        "sections": []
    })
    .to_string();
    let hook = fixture.project.join(".dg/hooks/on_create");
    let mut command = Command::new(&hook);
    command
        .args(["ADR-001", "create"])
        .current_dir(&fixture.project);
    let result = run_with_input(&mut command, payload.as_bytes());
    assert!(result.status.success(), "installed hook failed: {result:?}");
    assert!(String::from_utf8_lossy(&result.stdout).contains("ingested dg hook source"));

    let create = fixture.run_dg(&["new", "adr", "Installed hooks"]);
    assert!(
        !String::from_utf8_lossy(&create.stderr).contains("hook exited"),
        "DG create hook failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    let update = fixture.run_dg(&["set", "ADR-001", "status=accepted"]);
    assert!(
        !String::from_utf8_lossy(&update.stderr).contains("hook exited"),
        "DG update hook failed: {}",
        String::from_utf8_lossy(&update.stderr)
    );
    let delete = fixture.run_dg(&["delete", "ADR-001"]);
    assert!(
        !String::from_utf8_lossy(&delete.stderr).contains("hook exited"),
        "DG delete hook failed: {}",
        String::from_utf8_lossy(&delete.stderr)
    );
}

#[test]
fn exact_dg_hooks_dispatch_through_jj_prov_and_replay_idempotently() {
    let mut fixture = DgFixture::new();
    let component = fixture._root.path().join("dg.component.wasm");
    fs::write(&component, dg_component()).expect("write DG component");
    fixture.install_hooks(&component);

    fixture.run_dg(&["new", "adr", "Record a decision"]);
    let create_result = fs::read_to_string(fixture.captures.join("create.result"))
        .expect("read create hook result");
    assert!(
        create_result.contains("ingested dg hook source"),
        "create hook did not invoke jj-prov: {create_result}"
    );
    let create_payload = fixture.payload("create");

    fixture.run_dg(&["set", "ADR-001", "status=accepted"]);
    assert!(
        fixture
            .hook_result("update")
            .contains("ingested dg hook source"),
        "update hook did not invoke jj-prov"
    );
    let update_payload = fixture.payload("update");

    fixture.run_dg(&["delete", "ADR-001"]);
    let delete_result = fixture.hook_result("delete");
    let delete_payload = fixture.payload("delete");
    assert!(
        delete_result.contains("ingested dg hook source"),
        "delete hook did not invoke jj-prov: {delete_result}\n\
         payload: {}",
        String::from_utf8_lossy(&delete_payload)
    );

    let before_replay = run_ingest(&fixture.project);
    let mut replay = Command::new(env!("CARGO_BIN_EXE_jj-prov"));
    replay
        .args([
            "ingest",
            "--with-plugin",
            component.to_str().expect("component path is UTF-8"),
            "--plugin-id",
            "dg",
            "--path",
            fixture.project.to_str().expect("project path is UTF-8"),
            "ADR-001",
            "update",
        ])
        .current_dir(&fixture.project);
    let replay = run_with_input(&mut replay, &update_payload);
    assert!(replay.status.success(), "DG replay failed: {replay:?}");

    let after_replay = run_ingest(&fixture.project);
    assert_eq!(
        before_replay, after_replay,
        "replaying an identical DG hook must not add a provenance operation",
    );
    assert!(
        !create_payload.is_empty(),
        "the exact DG create payload must reach the hook"
    );
}

fn run_ingest(repository: &Path) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_jj-prov"))
        .args([
            "ingest",
            "--path",
            repository.to_str().expect("repository path is UTF-8"),
        ])
        .current_dir(repository)
        .output()
        .expect("run provenance ingest");
    assert!(
        output.status.success(),
        "provenance ingest failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("provenance output is UTF-8")
}

#[test]
fn beads_hook_payload_is_ingested_by_the_extracted_binary() {
    let fixture = Fixture::new();
    fixture.commit();
    let component = fixture._root.path().join("beads.component.wasm");
    fs::write(
        &component,
        plugin_component("provenance-plugin-beads", "provenance_plugin_beads"),
    )
    .expect("write Beads component");
    let payload = serde_json::json!({
        "id": "demo-1",
        "title": "Track hook",
        "status": "open",
        "priority": 1,
        "issue_type": "task",
        "created_at": "2026-09-01T00:00:00Z",
        "updated_at": "2026-09-01T00:00:01Z",
        "depends_on": []
    })
    .to_string();
    let mut command = Command::new(env!("CARGO_BIN_EXE_jj-prov"));
    command
        .args([
            "ingest",
            "--with-plugin",
            component.to_str().expect("component path is UTF-8"),
            "--plugin-id",
            "beads",
            "--path",
            fixture
                .repository
                .to_str()
                .expect("repository path is UTF-8"),
            "--event",
            "create",
            "--id",
            "demo-1",
        ])
        .current_dir(&fixture.repository)
        .env("BD_BIN", "missing-bd-for-cli-test");
    let result = run_with_input(&mut command, payload.as_bytes());
    assert!(result.status.success(), "Beads hook failed: {result:?}");
    assert!(String::from_utf8_lossy(&result.stdout).contains("ingested beads hook source"));
}
fn run_with_input(command: &mut Command, input: &[u8]) -> std::process::Output {
    use std::{io::Write, process::Stdio};

    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn command");
    child
        .stdin
        .as_mut()
        .expect("command stdin")
        .write_all(input)
        .expect("write command stdin");
    child.wait_with_output().expect("wait for command")
}
