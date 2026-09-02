use std::{
    io::Write,
    path::{Path, PathBuf},
};

use clap::Args;
use provenance_core::Query;
use provenance_jj::compile_why;
use provenance_output_tty::{ChronicleQuery, render_chronicle};

use crate::{
    cli::{DEFAULT_WHY_DEPTH, load_repository, open_provenance_service},
    errors::CliError,
};

#[derive(Debug, Args, PartialEq, Eq)]
pub(crate) struct Command {
    /// JJ URI to explain.
    #[arg(value_name = "JJ-URI")]
    uri: String,
    /// JJ workspace path. Defaults to the current directory.
    #[arg(value_name = "PATH", conflicts_with = "option_path")]
    positional_path: Option<PathBuf>,
    /// JJ workspace path.
    #[arg(short = 'R', long = "path", value_name = "PATH")]
    option_path: Option<PathBuf>,
    /// Maximum explanation depth.
    #[arg(
        short = 'd',
        long = "depth",
        visible_alias = "max-depth",
        default_value_t = DEFAULT_WHY_DEPTH
    )]
    depth: usize,
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
    let service = open_provenance_service(&repository, path)?;

    let query = compile_why(&command.uri)?;
    let target = match query {
        Query::Explain { root, .. } => root,
        _ => {
            return Err(CliError::Hook(
                "why did not compile to an explain query".into(),
            ));
        }
    };
    let chronicle = render_chronicle(&service.state, &ChronicleQuery::why(target, command.depth))?;
    writeln!(output, "{chronicle}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_default_depth_and_path() {
        let command = Command {
            uri: "jj://commit/abc".into(),
            positional_path: None,
            option_path: None,
            depth: DEFAULT_WHY_DEPTH,
        };
        assert_eq!(command.path(), Path::new("."));
        assert_eq!(command.depth, DEFAULT_WHY_DEPTH);
    }
}
