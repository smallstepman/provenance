use std::path::PathBuf;

use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::settings::UserSettings;
use provenance_jj::{JjRepository, ingest_repository, open_service};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next().unwrap_or_default();
    if command != "ingest" {
        return Err("usage: jj-prov ingest [PATH]".into());
    }

    let path = PathBuf::from(arguments.next().unwrap_or_else(|| ".".to_owned()));
    if arguments.next().is_some() {
        return Err("usage: jj-prov ingest [PATH]".into());
    }

    let repository = JjRepository::load(&settings()?, &path)?;
    let workspace_root = repository
        .workspace_root()
        .unwrap_or(path.as_path())
        .to_owned();
    let store_path = workspace_root.join(".jj").join("provenance");
    let mut service = open_service(&repository, &store_path)?;
    ingest_repository(&mut service, &repository)?;
    println!(
        "ingested {} provenance operations",
        service.runtime.published_len()?
    );
    Ok(())
}

fn settings() -> Result<UserSettings, Box<dyn std::error::Error>> {
    let mut config = StackedConfig::with_defaults();
    let layer = ConfigLayer::parse(
        ConfigSource::CommandArg,
        r#"
[user]
name = "Provenance"
email = "provenance@localhost"
"#,
    )?;
    config.add_layer(layer);
    Ok(UserSettings::from_config(config)?)
}
