use std::{
    env,
    ffi::OsString,
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};

use clap::Args;
use provenance_jj::{ingest_plugin_with_causal_parents, ingest_repository, jj_operation};
use provenance_plugin::{PluginManager, bindings};
use serde_json::{Map, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::{
    cli::{load_repository, open_provenance_service},
    errors::CliError,
};

#[derive(Debug, Args, PartialEq, Eq)]
pub(crate) struct HookOptions {
    /// WASM component to invoke.
    #[arg(
        long = "plugin",
        visible_aliases = ["component", "wasm", "plugin-path"],
        value_name = "COMPONENT"
    )]
    component: Option<PathBuf>,
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
    /// JJ workspace path.
    #[arg(
        short = 'R',
        long = "path",
        visible_alias = "repo",
        value_name = "PATH"
    )]
    path: Option<PathBuf>,
    /// Arguments supplied by the source hook.
    #[arg(value_name = "HOOK-ARGS", trailing_var_arg = true)]
    positionals: Vec<String>,
}

#[derive(Clone, Debug)]
struct HookContext {
    event: Option<String>,
    entity_id: Option<String>,
    source_id: Option<String>,
    kind: String,
}

#[derive(Debug)]
struct ResolvedHookOptions {
    component: PathBuf,
    plugin_id: Option<String>,
    event: Option<String>,
    entity_id: Option<String>,
    source_id: Option<String>,
    kind: String,
    path: PathBuf,
    positionals: Vec<String>,
}

pub(crate) fn execute<R, W>(
    options: HookOptions,
    input: &mut R,
    output: &mut W,
) -> Result<(), CliError>
where
    R: Read,
    W: Write,
{
    let raw = read_hook_input(input)?;
    let payload: JsonValue = serde_json::from_slice(&raw)?;
    let invocation = resolve_hook_options(options)?;
    let repository = load_repository(&invocation.path)?;
    let workspace_root = repository
        .workspace_root()
        .unwrap_or(invocation.path.as_path())
        .to_owned();

    let component_path =
        resolve_component_path(&invocation.component, &invocation.path, &workspace_root);
    let component = fs::read(component_path)?;
    let mut plugins = PluginManager::new()?;
    let manifest = plugins.register_component(&component)?;
    if let Some(expected) = invocation.plugin_id.as_deref()
        && expected != manifest.id
    {
        return Err(CliError::Hook(format!(
            "component manifest id `{}` does not match requested plugin id `{expected}`",
            manifest.id
        )));
    }

    let context = hook_context(&invocation, &payload)?;
    let request = observation_request(
        &manifest.namespace,
        &raw,
        &payload,
        &context,
        &workspace_root,
    )?;
    let source = request.source.clone();

    let mut service = open_provenance_service(&repository, invocation.path.as_path())?;
    // Hook observations must descend from every currently reachable
    // provenance head, not only the JJ operation that triggered the hook.
    // Otherwise repeated hooks become sibling operations whose later
    // schema-less facts can be replayed before the operation that registered
    // their schema.
    ingest_repository(&mut service, &repository)?;
    let mut causal_parents = service
        .state
        .operation_heads
        .iter()
        .filter_map(|id| {
            service
                .state
                .operation(id)
                .and_then(|operation| operation.source.as_ref())
                .cloned()
        })
        .collect::<Vec<_>>();
    causal_parents.push(jj_operation(repository.operation_id()));
    ingest_plugin_with_causal_parents(
        &mut service,
        &plugins,
        &manifest.id,
        request,
        causal_parents,
    )?;

    writeln!(
        output,
        "ingested {} hook source {}://{}/{}",
        manifest.id, source.namespace, source.kind, source.id
    )?;
    Ok(())
}

fn resolve_component_path(
    component: &Path,
    invocation_path: &Path,
    workspace_root: &Path,
) -> PathBuf {
    if component.is_absolute() || component.exists() {
        return component.to_owned();
    }
    let from_invocation = invocation_path.join(component);
    if from_invocation.exists() {
        return from_invocation;
    }
    let from_workspace = workspace_root.join(component);
    if from_workspace.exists() {
        return from_workspace;
    }
    component.to_owned()
}

fn read_hook_input<R>(input: &mut R) -> Result<Vec<u8>, CliError>
where
    R: Read,
{
    let mut raw = Vec::new();
    input.read_to_end(&mut raw)?;
    if raw.iter().all(u8::is_ascii_whitespace) {
        return Err(CliError::Hook("stdin payload must not be empty".into()));
    }
    Ok(raw)
}

fn resolve_hook_options(options: HookOptions) -> Result<ResolvedHookOptions, CliError> {
    let mut positionals = options.positionals;
    let path = options
        .path
        .or_else(|| {
            positionals
                .iter()
                .position(|value| Path::new(value).join(".jj").is_dir())
                .map(|index| PathBuf::from(positionals.remove(index)))
        })
        .unwrap_or_else(|| PathBuf::from("."));
    let plugin_id = options.plugin_id.or_else(|| {
        env::var("JJ_PROV_PLUGIN_ID")
            .ok()
            .or_else(|| env::var("PROVENANCE_PLUGIN_ID").ok())
    });
    let component = options
        .component
        .or_else(|| {
            positionals
                .iter()
                .position(|value| {
                    let path = Path::new(value);
                    path.is_file()
                        || path
                            .extension()
                            .is_some_and(|extension| extension.eq_ignore_ascii_case("wasm"))
                })
                .map(|index| positionals.remove(index))
                .map(PathBuf::from)
        })
        .or_else(|| env::var_os("JJ_PROV_PLUGIN").map(PathBuf::from))
        .or_else(|| env::var_os("PROVENANCE_PLUGIN").map(PathBuf::from))
        .or_else(|| {
            plugin_id.as_deref().map(|plugin_id| {
                path.join(".jj")
                    .join("provenance")
                    .join("plugins")
                    .join(format!("{plugin_id}.wasm"))
            })
        })
        .ok_or_else(|| {
            CliError::Usage("ingest-hook requires --plugin COMPONENT (or JJ_PROV_PLUGIN)".into())
        })?;

    Ok(ResolvedHookOptions {
        component,
        plugin_id,
        event: options.event.or_else(|| {
            env::var("JJ_PROV_HOOK_EVENT")
                .ok()
                .or_else(|| env::var("PROVENANCE_HOOK_EVENT").ok())
        }),
        entity_id: options.entity_id.or_else(|| {
            env::var("JJ_PROV_HOOK_ID")
                .ok()
                .or_else(|| env::var("PROVENANCE_HOOK_ID").ok())
        }),
        source_id: options.source_id,
        kind: options.kind,
        path,
        positionals,
    })
}

fn hook_context(
    options: &ResolvedHookOptions,
    payload: &JsonValue,
) -> Result<HookContext, CliError> {
    let mut event = options.event.clone();
    let mut entity_id = options.entity_id.clone();
    for positional in &options.positionals {
        if event.is_none() && is_hook_event(positional) {
            event = Some(positional.clone());
        } else if entity_id.is_none() {
            entity_id = Some(positional.clone());
        } else {
            return Err(CliError::Usage(format!(
                "could not interpret hook argument `{positional}`; pass --event and --id explicitly"
            )));
        }
    }

    let snapshot = unwrap_data(payload);
    if event.is_none() {
        event = string_field(payload, "event").or_else(|| string_field(snapshot, "event"));
    }
    if entity_id.is_none() {
        entity_id = string_field(snapshot, "id")
            .or_else(|| string_field(snapshot, "document_id"))
            .or_else(|| string_field(snapshot, "document-id"));
        if entity_id.is_none()
            && let Some(after) = payload.get("after")
        {
            entity_id = string_field(after, "id").or_else(|| string_field(after, "document_id"));
        }
    }

    Ok(HookContext {
        event,
        entity_id,
        source_id: options.source_id.clone(),
        kind: options.kind.clone(),
    })
}

fn observation_request(
    namespace: &str,
    raw: &[u8],
    payload: &JsonValue,
    context: &HookContext,
    repository_path: &Path,
) -> Result<bindings::ObservationRequest, CliError> {
    if payload
        .as_object()
        .is_some_and(|object| object.contains_key("source") || object.contains_key("attributes"))
    {
        return canonical_request(payload, context);
    }
    match namespace {
        "beads" => beads_request(payload, context, repository_path),
        "dg" => dg_request(raw, payload, context),
        _ => generic_request(raw, payload, context, namespace),
    }
}

fn canonical_request(
    payload: &JsonValue,
    context: &HookContext,
) -> Result<bindings::ObservationRequest, CliError> {
    let object = payload
        .as_object()
        .ok_or_else(|| CliError::Hook("canonical hook payload must be a JSON object".into()))?;
    let source = object
        .get("source")
        .ok_or_else(|| CliError::Hook("canonical hook payload is missing source".into()))?;
    let source = parse_entity_address(source, "source")?;
    let cursor = object
        .get("cursor")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .or_else(|| context.event.clone());
    let attributes =
        parse_attributes(object.get("attributes").ok_or_else(|| {
            CliError::Hook("canonical hook payload is missing attributes".into())
        })?)?;
    Ok(bindings::ObservationRequest {
        source,
        cursor,
        attributes,
    })
}

fn beads_request(
    payload: &JsonValue,
    context: &HookContext,
    repository_path: &Path,
) -> Result<bindings::ObservationRequest, CliError> {
    let snapshot = unwrap_data(payload);
    let event = context
        .event
        .clone()
        .or_else(|| string_field(payload, "event"))
        .ok_or_else(|| CliError::Hook("Beads hook event is missing".into()))?;
    if !matches!(event.as_str(), "create" | "update" | "close") {
        return Err(CliError::Hook(format!(
            "Beads hook event must be create, update, or close, got `{event}`"
        )));
    }
    let issue_id = context
        .entity_id
        .clone()
        .or_else(|| string_field(snapshot, "id"))
        .ok_or_else(|| CliError::Hook("Beads hook issue id is missing".into()))?;
    let updated_at = string_field(snapshot, "updated_at")
        .or_else(|| string_field(snapshot, "updated-at"))
        .ok_or_else(|| CliError::Hook("Beads hook snapshot is missing updated_at".into()))?;
    let source_id = context
        .source_id
        .clone()
        .unwrap_or_else(|| format!("{event}/{issue_id}/{updated_at}"));

    let mut attributes = Vec::new();
    for (json_name, plugin_name) in [
        ("id", "issue-id"),
        ("title", "title"),
        ("description", "description"),
        ("status", "status"),
        ("issue_type", "issue-type"),
        ("assignee", "assignee"),
        ("created_at", "created-at"),
        ("created_by", "created-by"),
        ("updated_at", "updated-at"),
        ("started_at", "started-at"),
        ("closed_at", "closed-at"),
        ("close_reason", "close-reason"),
        ("external_ref", "external-ref"),
        ("parent_id", "parent-id"),
    ] {
        push_optional_string(&mut attributes, snapshot, json_name, plugin_name);
    }
    if let Some(priority) = snapshot
        .as_object()
        .and_then(|object| object.get("priority"))
    {
        attributes.push(bindings::Attribute {
            name: "priority".into(),
            value: json_to_plugin_value(priority)?,
        });
    }
    if let Some(labels) = snapshot
        .as_object()
        .and_then(|object| first_present(object, &["labels"]))
    {
        push_string_list(&mut attributes, "labels", labels)?;
    }
    let dependencies = if let Some(value) = snapshot
        .as_object()
        .and_then(|object| first_present(object, &["depends_on", "depends-on", "dependencies"]))
    {
        string_list(value, "depends_on")?
    } else {
        beads_dependencies(repository_path, &issue_id)?
    };
    if !dependencies.is_empty() {
        attributes.push(string_list_attribute("depends-on", dependencies));
    }

    Ok(bindings::ObservationRequest {
        source: hook_source("beads", &source_id),
        cursor: Some(event),
        attributes,
    })
}

fn dg_request(
    raw: &[u8],
    payload: &JsonValue,
    context: &HookContext,
) -> Result<bindings::ObservationRequest, CliError> {
    let event = context
        .event
        .clone()
        .or_else(|| string_field(payload, "event"))
        .ok_or_else(|| CliError::Hook("dg hook event is missing".into()))?;
    if !matches!(event.as_str(), "create" | "update" | "delete") {
        return Err(CliError::Hook(format!(
            "dg hook event must be create, update, or delete, got `{event}`"
        )));
    }
    let current = if event == "update" {
        payload.get("after").unwrap_or(payload)
    } else {
        payload
    };
    let issue_id = context
        .entity_id
        .clone()
        .or_else(|| string_field(current, "id"))
        .ok_or_else(|| CliError::Hook("dg hook document id is missing".into()))?;
    let document_type = issue_id
        .split_once('-')
        .map(|(prefix, _)| prefix.to_ascii_lowercase())
        .unwrap_or_else(|| "document".into());
    let revision = format!("{:x}", Sha256::digest(raw));
    let source_id = context
        .source_id
        .clone()
        .unwrap_or_else(|| format!("{event}/{issue_id}/{revision}"));

    let mut attributes = vec![
        string_attribute("document-id", issue_id.clone()),
        string_attribute("document-type", document_type.clone()),
    ];
    for (json_name, plugin_name) in [("path", "path"), ("title", "title"), ("body", "body")] {
        push_optional_string(&mut attributes, current, json_name, plugin_name);
    }
    let frontmatter = current.get("frontmatter");
    if let Some(frontmatter) = frontmatter.and_then(JsonValue::as_object) {
        attributes.push(bindings::Attribute {
            name: "frontmatter".into(),
            value: bindings::Value::MapValue(scalar_map(frontmatter)?),
        });
        attributes.push(string_attribute(
            "frontmatter-json",
            serde_json::to_string(frontmatter)?,
        ));
        for (json_name, plugin_name) in
            [("status", "status"), ("author", "author"), ("date", "date")]
        {
            push_optional_string(
                &mut attributes,
                &JsonValue::Object(frontmatter.clone()),
                json_name,
                plugin_name,
            );
        }
        if let Some(tags) = first_present(frontmatter, &["tags"]) {
            push_string_list(&mut attributes, "tags", tags)?;
        }
        for name in [
            "supersedes",
            "enables",
            "triggers",
            "depends_on",
            "implements",
            "conflicts_with",
            "related",
        ] {
            if let Some(value) = frontmatter.get(name) {
                push_string_list(&mut attributes, name.replace('_', "-").as_str(), value)?;
            }
        }
    }
    if let Some(sections) = current.get("sections") {
        attributes.push(string_attribute(
            "sections-json",
            serde_json::to_string(sections)?,
        ));
    }
    attributes.push(string_attribute(
        "payload-json",
        String::from_utf8_lossy(raw).into_owned(),
    ));
    attributes.push(string_attribute("revision", revision));
    if event == "update" {
        for (json_name, plugin_name) in [
            ("before", "before-json"),
            ("after", "after-json"),
            ("diff", "diff-json"),
        ] {
            if let Some(value) = payload.get(json_name) {
                attributes.push(string_attribute(plugin_name, serde_json::to_string(value)?));
            }
        }
    }

    Ok(bindings::ObservationRequest {
        source: hook_source("dg", &source_id),
        cursor: Some(event),
        attributes,
    })
}

fn generic_request(
    raw: &[u8],
    payload: &JsonValue,
    context: &HookContext,
    namespace: &str,
) -> Result<bindings::ObservationRequest, CliError> {
    let source_id = context.source_id.clone().unwrap_or_else(|| {
        let event = context.event.as_deref().unwrap_or("observe");
        let id = context.entity_id.as_deref().unwrap_or("payload");
        format!("{event}/{id}/{:x}", Sha256::digest(raw))
    });
    let mut attributes = Vec::new();
    if let Some(object) = payload.as_object() {
        for (name, value) in object {
            if matches!(name.as_str(), "source" | "cursor" | "attributes" | "event") {
                continue;
            }
            attributes.push(bindings::Attribute {
                name: name.clone(),
                value: json_to_plugin_value(value)?,
            });
        }
    } else {
        return Err(CliError::Hook(
            "generic hook payload must be a JSON object".into(),
        ));
    }
    Ok(bindings::ObservationRequest {
        source: bindings::EntityAddress {
            namespace: namespace.into(),
            kind: context.kind.clone(),
            id: source_id,
        },
        cursor: context.event.clone(),
        attributes,
    })
}

fn beads_dependencies(repository_path: &Path, issue_id: &str) -> Result<Vec<String>, CliError> {
    let program = env::var_os("BD_BIN").unwrap_or_else(|| OsString::from("bd"));
    let output = match ProcessCommand::new(&program)
        .current_dir(repository_path)
        .args(["--json", "dep", "list", issue_id])
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(CliError::Io(error)),
    };
    if !output.status.success() {
        return Err(CliError::Hook(format!(
            "bd dep list failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let value: JsonValue = serde_json::from_slice(&output.stdout)?;
    let list = value
        .get("data")
        .or_else(|| value.get("dependencies"))
        .unwrap_or(&value);
    let Some(list) = list.as_array() else {
        return Err(CliError::Hook(
            "bd dep list returned JSON without a dependency array".into(),
        ));
    };
    list.iter()
        .map(|dependency| {
            dependency
                .get("id")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
                .ok_or_else(|| CliError::Hook("bd dependency is missing id".into()))
        })
        .collect()
}

fn parse_attributes(value: &JsonValue) -> Result<Vec<bindings::Attribute>, CliError> {
    match value {
        JsonValue::Array(attributes) => attributes
            .iter()
            .map(|attribute| {
                let object = attribute.as_object().ok_or_else(|| {
                    CliError::Hook("canonical attribute must be a JSON object".into())
                })?;
                let name = object
                    .get("name")
                    .and_then(JsonValue::as_str)
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| CliError::Hook("canonical attribute name is missing".into()))?;
                let value = object.get("value").ok_or_else(|| {
                    CliError::Hook(format!("canonical attribute `{name}` has no value"))
                })?;
                Ok(bindings::Attribute {
                    name: name.to_owned(),
                    value: json_to_plugin_value(value)?,
                })
            })
            .collect(),
        JsonValue::Object(attributes) => attributes
            .iter()
            .map(|(name, value)| {
                Ok(bindings::Attribute {
                    name: name.clone(),
                    value: json_to_plugin_value(value)?,
                })
            })
            .collect(),
        _ => Err(CliError::Hook(
            "canonical attributes must be an array or object".into(),
        )),
    }
}

fn json_to_plugin_value(value: &JsonValue) -> Result<bindings::Value, CliError> {
    Ok(match value {
        JsonValue::Null => bindings::Value::NullValue,
        JsonValue::Bool(value) => bindings::Value::Boolean(*value),
        JsonValue::Number(value) if value.is_i64() || value.is_u64() => {
            bindings::Value::IntegerText(value.to_string())
        }
        JsonValue::Number(value) => bindings::Value::DecimalText(value.to_string()),
        JsonValue::String(value) => bindings::Value::StringValue(value.clone()),
        JsonValue::Array(values) => bindings::Value::ListValue(
            values
                .iter()
                .map(json_to_scalar)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        JsonValue::Object(values) => {
            if let Some(entity) = values.get("entity").or_else(|| values.get("entity-value")) {
                return Ok(bindings::Value::EntityValue(parse_entity_ref(entity)?));
            }
            bindings::Value::MapValue(scalar_map(values)?)
        }
    })
}

fn json_to_scalar(value: &JsonValue) -> Result<bindings::ScalarValue, CliError> {
    Ok(match value {
        JsonValue::Null => bindings::ScalarValue::NullValue,
        JsonValue::Bool(value) => bindings::ScalarValue::Boolean(*value),
        JsonValue::Number(value) if value.is_i64() || value.is_u64() => {
            bindings::ScalarValue::IntegerText(value.to_string())
        }
        JsonValue::Number(value) => bindings::ScalarValue::DecimalText(value.to_string()),
        JsonValue::String(value) => bindings::ScalarValue::StringValue(value.clone()),
        JsonValue::Object(values) => {
            if let Some(entity) = values.get("entity").or_else(|| values.get("entity-value")) {
                bindings::ScalarValue::EntityValue(parse_entity_ref(entity)?)
            } else {
                return Err(CliError::Hook(
                    "nested JSON objects are not valid WIT scalar values".into(),
                ));
            }
        }
        JsonValue::Array(_) => {
            return Err(CliError::Hook(
                "nested JSON arrays are not valid WIT scalar values".into(),
            ));
        }
    })
}

fn required_json_string(
    object: &Map<String, JsonValue>,
    key: &str,
    field: &str,
) -> Result<String, CliError> {
    object
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| CliError::Hook(format!("{field} is missing non-empty {key}")))
}
fn parse_entity_address(
    value: &JsonValue,
    field: &str,
) -> Result<bindings::EntityAddress, CliError> {
    let object = value
        .as_object()
        .ok_or_else(|| CliError::Hook(format!("{field} must be an entity address object")))?;
    let namespace = required_json_string(object, "namespace", field)?;
    let kind = required_json_string(object, "kind", field)?;
    let id = required_json_string(object, "id", field)?;
    Ok(bindings::EntityAddress {
        namespace,
        kind,
        id,
    })
}

fn parse_entity_ref(value: &JsonValue) -> Result<bindings::EntityRef, CliError> {
    Ok(bindings::EntityRef::External(parse_entity_address(
        value, "entity",
    )?))
}
fn scalar_map(values: &Map<String, JsonValue>) -> Result<Vec<bindings::ScalarAttribute>, CliError> {
    let mut result = Vec::new();
    for (name, value) in values {
        if matches!(value, JsonValue::Array(_)) {
            continue;
        }
        if matches!(value, JsonValue::Object(object) if !object.contains_key("entity") && !object.contains_key("entity-value"))
        {
            continue;
        }
        result.push(bindings::ScalarAttribute {
            name: name.clone(),
            value: json_to_scalar(value)?,
        });
    }
    Ok(result)
}
fn push_optional_string(
    attributes: &mut Vec<bindings::Attribute>,
    object: &JsonValue,
    json_name: &str,
    plugin_name: &str,
) {
    if let Some(value) = string_field(object, json_name) {
        attributes.push(string_attribute(plugin_name, value));
    }
}

fn push_string_list(
    attributes: &mut Vec<bindings::Attribute>,
    name: &str,
    value: &JsonValue,
) -> Result<(), CliError> {
    let values = string_list(value, name)?;
    if !values.is_empty() {
        attributes.push(string_list_attribute(name, values));
    }
    Ok(())
}

fn string_list(value: &JsonValue, field: &str) -> Result<Vec<String>, CliError> {
    match value {
        JsonValue::String(value) => Ok(vec![value.clone()]),
        JsonValue::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| string_field(value, "id"))
                    .ok_or_else(|| {
                        CliError::Hook(format!("{field} must contain only strings or ids"))
                    })
            })
            .collect(),
        JsonValue::Null => Ok(Vec::new()),
        _ => Err(CliError::Hook(format!("{field} must be a string or list"))),
    }
}

fn string_attribute(name: &str, value: impl Into<String>) -> bindings::Attribute {
    bindings::Attribute {
        name: name.into(),
        value: bindings::Value::StringValue(value.into()),
    }
}

fn string_list_attribute(name: &str, values: Vec<String>) -> bindings::Attribute {
    bindings::Attribute {
        name: name.into(),
        value: bindings::Value::ListValue(
            values
                .into_iter()
                .map(bindings::ScalarValue::StringValue)
                .collect(),
        ),
    }
}

fn hook_source(namespace: &str, id: &str) -> bindings::EntityAddress {
    bindings::EntityAddress {
        namespace: namespace.into(),
        kind: "hook".into(),
        id: id.into(),
    }
}

fn first_present<'a>(object: &'a Map<String, JsonValue>, names: &[&str]) -> Option<&'a JsonValue> {
    names.iter().find_map(|name| object.get(*name))
}

fn string_field(value: &JsonValue, name: &str) -> Option<String> {
    value
        .as_object()
        .and_then(|object| object.get(name))
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
}

fn unwrap_data(value: &JsonValue) -> &JsonValue {
    let Some(object) = value.as_object() else {
        return value;
    };
    if object.contains_key("id")
        || object.contains_key("before")
        || object.contains_key("after")
        || object.contains_key("frontmatter")
    {
        return value;
    }
    match object.get("data") {
        Some(JsonValue::Object(_)) => object.get("data").unwrap_or(value),
        Some(JsonValue::Array(values)) => values.first().unwrap_or(value),
        _ => value,
    }
}

fn is_hook_event(value: &str) -> bool {
    value == "create" || value == "update" || value == "close" || value == "delete"
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::{Cli, CliCommand};

    use super::*;

    #[test]
    fn parses_dg_hook_arguments_in_document_first_order() {
        let args = vec![
            "jj-prov".to_owned(),
            "ingest-hook".to_owned(),
            "--plugin".to_owned(),
            "dg.wasm".to_owned(),
            "ADR-001".to_owned(),
            "update".to_owned(),
        ];
        let cli = Cli::try_parse_from(args).expect("parse hook");
        let options = match cli.command {
            CliCommand::IngestHook(options) => options,
            _ => panic!("expected hook command"),
        };
        assert_eq!(options.component.as_deref(), Some(Path::new("dg.wasm")));
        assert_eq!(options.positionals, ["ADR-001", "update"]);
        let resolved = ResolvedHookOptions {
            component: options.component.expect("component"),
            plugin_id: options.plugin_id,
            event: options.event,
            entity_id: options.entity_id,
            source_id: options.source_id,
            kind: options.kind,
            path: PathBuf::from("."),
            positionals: options.positionals,
        };
        let payload = serde_json::json!({"id": "ADR-001"});
        let context = hook_context(&resolved, &payload).expect("hook context");
        assert_eq!(context.entity_id.as_deref(), Some("ADR-001"));
        assert_eq!(context.event.as_deref(), Some("update"));
    }

    #[test]
    fn preserves_hook_arguments_after_separator() {
        let cli = Cli::try_parse_from([
            "jj-prov",
            "ingest-hook",
            "--plugin",
            "plugin.wasm",
            "--",
            "--event",
            "update",
        ])
        .expect("parse hook arguments");
        let options = match cli.command {
            CliCommand::IngestHook(options) => options,
            _ => panic!("expected hook command"),
        };
        assert_eq!(options.positionals, ["--event", "update"]);
    }

    #[test]
    fn maps_dg_update_payload_to_stable_typed_request() {
        let raw = br#"{"before":{"frontmatter":{"status":"proposed"}},"after":{"id":"ADR-001","path":"docs/adr-001.md","body":"body","frontmatter":{"status":"accepted","tags":["a"]},"sections":[]},"diff":{"id":"ADR-001"}}"#;
        let payload: JsonValue = serde_json::from_slice(raw).expect("payload");
        let context = HookContext {
            event: Some("update".into()),
            entity_id: Some("ADR-001".into()),
            source_id: None,
            kind: "hook".into(),
        };
        let request = dg_request(raw, &payload, &context).expect("dg request");
        assert_eq!(request.source.namespace, "dg");
        assert!(request.source.id.starts_with("update/ADR-001/"));
        assert_eq!(request.cursor.as_deref(), Some("update"));
        assert!(
            request
                .attributes
                .iter()
                .any(|attribute| attribute.name == "after-json")
        );
        assert!(request.attributes.iter().any(|attribute| {
            attribute.name == "tags" && matches!(attribute.value, bindings::Value::ListValue(_))
        }));
    }

    #[test]
    fn maps_beads_snapshot_and_preserves_dependency_attribute() {
        let payload = serde_json::json!({
            "id": "demo-1",
            "title": "Track hook",
            "status": "open",
            "priority": 1,
            "issue_type": "task",
            "created_at": "2026-09-01T00:00:00Z",
            "updated_at": "2026-09-01T00:00:01Z",
            "depends_on": ["demo-0"]
        });
        let context = HookContext {
            event: Some("create".into()),
            entity_id: Some("demo-1".into()),
            source_id: None,
            kind: "hook".into(),
        };
        let request =
            beads_request(&payload, &context, Path::new("/does/not/exist")).expect("Beads request");
        assert_eq!(request.source.namespace, "beads");
        assert_eq!(request.source.id, "create/demo-1/2026-09-01T00:00:01Z");
        assert!(request.attributes.iter().any(|attribute| {
            attribute.name == "depends-on"
                && matches!(attribute.value, bindings::Value::ListValue(_))
        }));
    }

    #[test]
    fn canonical_payload_preserves_entity_values() {
        let payload = serde_json::json!({
            "source": {"namespace":"tracker", "kind":"hook", "id":"create/1"},
            "cursor": "create",
            "attributes": {
                "current-commit": {"entity": {"namespace":"jj", "kind":"commit", "id":"abc"}}
            }
        });
        let context = HookContext {
            event: None,
            entity_id: None,
            source_id: None,
            kind: "hook".into(),
        };
        let request = canonical_request(&payload, &context).expect("canonical request");
        assert!(matches!(
            request.attributes[0].value,
            bindings::Value::EntityValue(bindings::EntityRef::External(_))
        ));
    }
}
