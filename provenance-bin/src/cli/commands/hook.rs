use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
};

use clap::{Args, Subcommand};
use provenance_plugin::{PluginManager, bindings};

use crate::{cli::load_repository, errors::CliError};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub(crate) enum Command {
    /// Install source lifecycle hooks using a plugin-provided file plan.
    Install(InstallOptions),
}

#[derive(Debug, Args, PartialEq, Eq)]
pub(crate) struct InstallOptions {
    /// WASM component that provides the installation plan.
    #[arg(
        long = "plugin",
        visible_alias = "with-plugin",
        value_name = "COMPONENT"
    )]
    component: PathBuf,
    /// Require a particular component manifest ID.
    #[arg(long = "plugin-id", value_name = "ID")]
    plugin_id: Option<String>,
    /// Project path. Defaults to the current directory.
    #[arg(short = 'R', long = "path", value_name = "PATH")]
    path: Option<PathBuf>,
}

pub(crate) fn execute<W>(command: &Command, output: &mut W) -> Result<(), CliError>
where
    W: Write,
{
    match command {
        Command::Install(options) => install(options, output),
    }
}

fn install<W>(options: &InstallOptions, output: &mut W) -> Result<(), CliError>
where
    W: Write,
{
    let invocation_path = options.path.as_deref().unwrap_or(Path::new("."));
    let repository = load_repository(invocation_path)?;
    let project_root = repository
        .workspace_root()
        .unwrap_or(invocation_path)
        .to_owned();
    let component_path = fs::canonicalize(super::ingest_hook::resolve_component_path(
        &options.component,
        invocation_path,
        &project_root,
    ))?;
    let component = fs::read(&component_path)?;

    let mut plugins = PluginManager::new()?;
    let manifest = plugins.register_component(&component)?;
    if let Some(expected) = options.plugin_id.as_deref()
        && expected != manifest.id
    {
        return Err(CliError::Hook(format!(
            "component manifest id `{}` does not match requested plugin id `{expected}`",
            manifest.id
        )));
    }
    if !manifest
        .capabilities
        .iter()
        .any(|capability| capability == "install-hooks")
    {
        return Err(CliError::Hook(format!(
            "plugin `{}` does not declare the `install-hooks` capability",
            manifest.id
        )));
    }

    let executable = std::env::current_exe()?
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            CliError::Hook(
                "jj-prov executable path must be valid UTF-8 for an installed hook".into(),
            )
        })?;
    let component = component_path.to_str().map(str::to_owned).ok_or_else(|| {
        CliError::Hook("plugin component path must be valid UTF-8 for an installed hook".into())
    })?;
    let plan = plugins.install(
        &manifest.id,
        bindings::InstallRequest {
            executable,
            component,
        },
    )?;
    let operation_count = plan.operations.len();
    apply_plan(&project_root, plan)?;

    writeln!(
        output,
        "installed {} hook file operations for plugin `{}` in {}",
        operation_count,
        manifest.id,
        project_root.display()
    )?;
    Ok(())
}

fn apply_plan(root: &Path, plan: bindings::InstallPlan) -> Result<(), CliError> {
    let root = root.canonicalize().map_err(|error| {
        CliError::Hook(format!(
            "cannot resolve project root {}: {error}",
            root.display()
        ))
    })?;
    let mut seen = BTreeSet::new();
    let mut operations = Vec::with_capacity(plan.operations.len());

    for operation in plan.operations {
        let target = safe_target(&root, &operation.path)?;
        if !seen.insert(target.clone()) {
            return Err(CliError::Hook(format!(
                "plugin installation contains duplicate target `{}`",
                operation.path
            )));
        }
        let current = match fs::symlink_metadata(&target) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(CliError::Hook(format!(
                        "refusing to install through symlink `{}`",
                        target.display()
                    )));
                }
                if !metadata.is_file() {
                    return Err(CliError::Hook(format!(
                        "plugin installation target `{}` is not a file",
                        target.display()
                    )));
                }
                Some(fs::read(&target)?)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let content = desired_content(&operation, current.as_deref())?;
        operations.push((target, content, operation.executable));
    }

    for (target, content, executable) in operations {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        if !target.exists() || fs::read(&target)? != content {
            fs::write(&target, content)?;
        }
        set_executable(&target, executable)?;
    }
    Ok(())
}

fn desired_content(
    operation: &bindings::FileOperation,
    current: Option<&[u8]>,
) -> Result<Vec<u8>, CliError> {
    let requested = operation.content.as_bytes();
    match operation.strategy {
        bindings::FileStrategy::Create => match current {
            None => Ok(requested.to_vec()),
            Some(existing) if existing == requested => Ok(existing.to_vec()),
            Some(_) => Err(CliError::Hook(format!(
                "refusing to overwrite existing hook `{}`; remove it or make it match the plugin plan",
                operation.path
            ))),
        },
        bindings::FileStrategy::Replace => match current {
            None => Err(CliError::Hook(format!(
                "cannot replace missing hook `{}`",
                operation.path
            ))),
            Some(existing) if existing == requested => Ok(existing.to_vec()),
            Some(existing)
                if operation
                    .expected
                    .as_deref()
                    .is_some_and(|expected| expected.as_bytes() == existing) =>
            {
                Ok(requested.to_vec())
            }
            Some(_) => Err(CliError::Hook(format!(
                "refusing to replace modified hook `{}`; expected content did not match",
                operation.path
            ))),
        },
        bindings::FileStrategy::Append => match current {
            None => Ok(requested.to_vec()),
            Some(existing) if requested.is_empty() => Ok(existing.to_vec()),
            Some(existing)
                if existing
                    .windows(requested.len())
                    .any(|window| window == requested) =>
            {
                Ok(existing.to_vec())
            }
            Some(existing) => {
                let mut content = existing.to_vec();
                content.extend_from_slice(requested);
                Ok(content)
            }
        },
    }
}

fn safe_target(root: &Path, relative: &str) -> Result<PathBuf, CliError> {
    let path = Path::new(relative);
    if relative.trim().is_empty() || path.is_absolute() {
        return Err(CliError::Hook(format!(
            "plugin installation path `{relative}` must be a non-empty relative path"
        )));
    }
    let mut target = root.to_owned();
    let components = path.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            return Err(CliError::Hook(format!(
                "plugin installation path `{relative}` contains an unsafe component"
            )));
        };
        target.push(name);
        if let Ok(metadata) = fs::symlink_metadata(&target) {
            if metadata.file_type().is_symlink() {
                return Err(CliError::Hook(format!(
                    "refusing to install through symlink `{}`",
                    target.display()
                )));
            }
            if index + 1 < components.len() && !metadata.is_dir() {
                return Err(CliError::Hook(format!(
                    "plugin installation parent `{}` is not a directory",
                    target.display()
                )));
            }
        }
    }
    Ok(target)
}

fn set_executable(path: &Path, executable: bool) -> Result<(), CliError> {
    let mut permissions = fs::metadata(path)?.permissions();
    let mode = if executable {
        permissions.mode() | 0o111
    } else {
        permissions.mode() & !0o111
    };
    permissions.set_mode(mode);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(
        path: &str,
        content: &str,
        strategy: bindings::FileStrategy,
        expected: Option<&str>,
    ) -> bindings::FileOperation {
        bindings::FileOperation {
            path: path.into(),
            content: content.into(),
            strategy,
            executable: false,
            expected: expected.map(str::to_owned),
        }
    }

    #[test]
    fn replace_requires_matching_expected_content() {
        let operation = operation("hook", "new", bindings::FileStrategy::Replace, Some("old"));
        assert_eq!(
            desired_content(&operation, Some(b"old")).expect("matching replacement"),
            b"new"
        );
        assert!(desired_content(&operation, Some(b"other")).is_err());
    }

    #[test]
    fn append_is_idempotent_and_accepts_empty_content() {
        let append = operation("hook", "block", bindings::FileStrategy::Append, None);
        assert_eq!(
            desired_content(&append, Some(b"prefixblock")).expect("existing block"),
            b"prefixblock"
        );
        let empty = operation("", "", bindings::FileStrategy::Append, None);
        assert_eq!(
            desired_content(&empty, Some(b"existing")).expect("empty append"),
            b"existing"
        );
    }

    #[test]
    fn rejects_unsafe_install_paths() {
        let root = tempfile::tempdir().expect("install root");
        assert!(safe_target(root.path(), "../outside").is_err());
        assert!(safe_target(root.path(), "/outside").is_err());
    }
}
