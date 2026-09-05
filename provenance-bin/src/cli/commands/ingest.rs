use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};

use clap::Args;
use provenance_jj::ingest_repository;

use crate::{
    cli::{load_repository, open_provenance_service},
    errors::CliError,
};

#[derive(Debug, Args, PartialEq, Eq)]
pub(crate) struct Command {
    /// Positional repository path, or source hook arguments with a plugin.
    #[arg(value_name = "ARGS", trailing_var_arg = true)]
    positionals: Vec<String>,
    /// JJ workspace path.
    #[arg(short = 'R', long = "path", value_name = "PATH")]
    option_path: Option<PathBuf>,
    /// WASM component to invoke for source hook ingestion.
    #[arg(long = "with-plugin", value_name = "COMPONENT")]
    with_plugin: Option<PathBuf>,
    /// Require a particular component manifest ID.
    #[arg(long = "plugin-id", value_name = "ID")]
    plugin_id: Option<String>,
    /// Hook cursor event.
    #[arg(long = "event", visible_alias = "cursor", value_name = "EVENT")]
    event: Option<String>,
    /// Hook entity, document, or issue ID.
    #[arg(
        long = "id",
        visible_aliases = ["entity-id", "document-id", "issue-id"],
        value_name = "ID"
    )]
    entity_id: Option<String>,
    /// Override the source address ID.
    #[arg(long = "source-id", value_name = "ID")]
    source_id: Option<String>,
    /// Source address kind.
    #[arg(long = "kind", value_name = "KIND", default_value = "hook")]
    kind: String,
}

impl Command {
    fn path(&self) -> &Path {
        self.option_path
            .as_deref()
            .or_else(|| self.positionals.first().map(Path::new))
            .unwrap_or(Path::new("."))
    }

    fn has_plugin_options(&self) -> bool {
        self.plugin_id.is_some()
            || self.event.is_some()
            || self.entity_id.is_some()
            || self.source_id.is_some()
            || self.kind != "hook"
    }

    fn hook_options(&self) -> super::ingest_hook::HookOptions {
        super::ingest_hook::HookOptions {
            component: self.with_plugin.clone(),
            plugin_id: self.plugin_id.clone(),
            event: self.event.clone(),
            entity_id: self.entity_id.clone(),
            source_id: self.source_id.clone(),
            kind: self.kind.clone(),
            path: self.option_path.clone(),
            positionals: self.positionals.clone(),
        }
    }
}

pub(crate) fn execute<R, W>(
    command: &Command,
    input: &mut R,
    output: &mut W,
) -> Result<(), CliError>
where
    R: Read,
    W: Write,
{
    if command.with_plugin.is_some() {
        return super::ingest_hook::execute(command.hook_options(), input, output);
    }
    if command.has_plugin_options() {
        return Err(CliError::Usage(
            "--plugin-id, --event, --id, --source-id, and --kind require --with-plugin".into(),
        ));
    }
    if command.positionals.len() > 1 {
        return Err(CliError::Usage(
            "ingest accepts one repository PATH without --with-plugin".into(),
        ));
    }

    let path = command.path();
    let repository = load_repository(path)?;
    let mut service = open_provenance_service(&repository, path)?;
    ingest_repository(&mut service, &repository)?;
    writeln!(
        output,
        "ingested {} provenance operations",
        service.runtime.published_len()?
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::{Cli, CliCommand};

    use super::*;

    #[test]
    fn resolves_default_path() {
        let command = Command {
            positionals: Vec::new(),
            option_path: None,
            with_plugin: None,
            plugin_id: None,
            event: None,
            entity_id: None,
            source_id: None,
            kind: "hook".into(),
        };
        assert_eq!(command.path(), Path::new("."));
    }

    #[test]
    fn parses_plugin_and_source_hook_arguments() {
        let cli = Cli::try_parse_from([
            "jj-prov",
            "ingest",
            "--with-plugin",
            "dg.wasm",
            "ADR-001",
            "update",
        ])
        .expect("parse plugin ingestion");
        let command = match cli.command {
            CliCommand::Ingest(command) => command,
            _ => panic!("expected ingest command"),
        };
        assert_eq!(command.with_plugin.as_deref(), Some(Path::new("dg.wasm")));
        assert_eq!(command.positionals, ["ADR-001", "update"]);
    }

    #[test]
    fn preserves_hook_arguments_after_separator() {
        let cli = Cli::try_parse_from([
            "jj-prov",
            "ingest",
            "--with-plugin",
            "plugin.wasm",
            "--",
            "--event",
            "update",
        ])
        .expect("parse hook arguments");
        let command = match cli.command {
            CliCommand::Ingest(command) => command,
            _ => panic!("expected ingest command"),
        };
        assert_eq!(command.positionals, ["--event", "update"]);
    }
}
