use std::{
    io::Write,
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
    /// JJ workspace path. Defaults to the current directory.
    #[arg(value_name = "PATH", conflicts_with = "option_path")]
    positional_path: Option<PathBuf>,
    /// JJ workspace path.
    #[arg(short = 'R', long = "path", value_name = "PATH")]
    option_path: Option<PathBuf>,
}

impl Command {
    fn path(&self) -> &Path {
        self.option_path
            .as_deref()
            .or(self.positional_path.as_deref())
            .unwrap_or(Path::new("."))
    }
}

pub(crate) fn execute<W>(command: &Command, output: &mut W) -> Result<(), CliError>
where
    W: Write,
{
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
    use super::*;

    #[test]
    fn resolves_default_path() {
        let command = Command {
            positional_path: None,
            option_path: None,
        };
        assert_eq!(command.path(), Path::new("."));
    }
}
