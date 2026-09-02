pub(crate) mod commands;

use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand, error::ErrorKind};
use jj_lib::{
    config::{ConfigLayer, ConfigSource, StackedConfig},
    settings::UserSettings,
};
use provenance_jj::{JjRepository, open_service};

use crate::errors::CliError;

pub(crate) const DEFAULT_WHY_DEPTH: usize = 3;

#[derive(Debug, Parser)]
#[command(
    name = "jj-prov",
    version,
    about = "Capture and inspect repository provenance.",
    subcommand_required = true
)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Ingest all currently reachable JJ operations.
    Ingest(commands::ingest::Command),
    /// Explain why an entity exists.
    Why(commands::why::Command),
    /// Ingest a source hook observation through a WASM plugin.
    #[command(name = "ingest-hook", visible_alias = "hook")]
    IngestHook(commands::ingest_hook::HookOptions),
}

/// Runs `jj-prov` using the supplied argument, input, and output streams.
///
/// Keeping stream ownership at this boundary makes hook ingestion testable
/// without changing the executable's stdin/stdout behavior.
pub fn run<I, S, R, W>(arguments: I, mut input: R, mut output: W) -> Result<(), CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
    R: Read,
    W: Write,
{
    let arguments =
        std::iter::once("jj-prov".to_owned()).chain(arguments.into_iter().map(Into::into));
    let cli = match Cli::try_parse_from(arguments) {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            output.write_all(error.to_string().as_bytes())?;
            return Ok(());
        }
        Err(error) => return Err(CliError::Usage(error.to_string())),
    };

    match cli.command {
        CliCommand::Ingest(command) => commands::ingest::execute(&command, &mut output)?,
        CliCommand::Why(command) => commands::why::execute(&command, &mut output)?,
        CliCommand::IngestHook(options) => {
            commands::ingest_hook::execute(options, &mut input, &mut output)?;
        }
    }
    Ok(())
}

pub(crate) fn load_repository(path: &Path) -> Result<JjRepository, CliError> {
    JjRepository::load(&settings()?, path).map_err(CliError::from)
}

pub(crate) fn provenance_store_path(repository: &JjRepository, fallback: &Path) -> PathBuf {
    repository
        .workspace_root()
        .unwrap_or(fallback)
        .join(".jj")
        .join("provenance")
}

pub(crate) fn settings() -> Result<UserSettings, CliError> {
    let mut config = StackedConfig::with_defaults();
    let layer = ConfigLayer::parse(
        ConfigSource::CommandArg,
        r#"
[user]
name = "Provenance"
email = "provenance@localhost"
"#,
    )
    .map_err(|error| CliError::Settings(error.to_string()))?;
    config.add_layer(layer);
    UserSettings::from_config(config).map_err(|error| CliError::Settings(error.to_string()))
}

pub(crate) fn open_provenance_service(
    repository: &JjRepository,
    fallback: &Path,
) -> Result<provenance_jj::JjService, CliError> {
    open_service(repository, provenance_store_path(repository, fallback)).map_err(CliError::from)
}

#[cfg(test)]
mod tests {
    use std::io;

    use clap::Parser;

    use super::*;

    #[test]
    fn renders_help_through_run_output() {
        let mut output = Vec::new();
        run(["--help"], io::empty(), &mut output).expect("render help");
        let help = String::from_utf8(output).expect("help is UTF-8");
        assert!(help.contains("Usage: jj-prov <COMMAND>"));
        assert!(help.contains("ingest-hook"));
    }

    #[test]
    fn accepts_hook_aliases() {
        let cli = Cli::try_parse_from([
            "jj-prov",
            "hook",
            "--component",
            "plugin.wasm",
            "--cursor",
            "update",
            "--document-id",
            "ADR-001",
            "--repo",
            "repo",
            "ADR-001",
            "update",
        ])
        .expect("parse hook aliases");
        assert!(matches!(cli.command, CliCommand::IngestHook(_)));
    }

    #[test]
    fn returns_clap_errors_without_exiting() {
        let error = run(["why", "--depth", "nope"], io::empty(), Vec::new())
            .expect_err("invalid depth should fail");
        assert!(matches!(
            error,
            CliError::Usage(message) if message.contains("invalid value")
        ));
    }
}
