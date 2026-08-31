use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};

use crate::error::ProjectionError;

/// Identity inputs that keep harness sessions separate from repository sources.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayIdentityConfig {
    /// Stable name of the harness integration (`omp`, `prime`, `autolith`, ...).
    pub harness_id: String,
    /// Stable installation or workstation identifier.
    pub installation_id: String,
}

impl RelayIdentityConfig {
    /// Construct and validate a Relay identity namespace.
    pub fn new(
        harness_id: impl Into<String>,
        installation_id: impl Into<String>,
    ) -> Result<Self, ProjectionError> {
        let config = Self {
            harness_id: harness_id.into(),
            installation_id: installation_id.into(),
        };
        if config.harness_id.trim().is_empty() {
            return Err(ProjectionError::EmptyIdentity {
                field: "harness_id",
            });
        }
        if config.installation_id.trim().is_empty() {
            return Err(ProjectionError::EmptyIdentity {
                field: "installation_id",
            });
        }
        Ok(config)
    }

    /// Return the scoped session seed used by the agent WASM projection.
    pub fn session_seed(&self, raw_session: &str) -> String {
        scoped_seed(
            "session",
            &self.harness_id,
            &self.installation_id,
            raw_session,
        )
    }

    /// Return the scoped actor seed used by the agent WASM projection.
    pub fn actor_seed(&self, raw_actor: &str) -> String {
        scoped_seed("actor", &self.harness_id, &self.installation_id, raw_actor)
    }

    /// Return a deterministic fallback session seed for an unannotated root scope.
    pub fn fallback_session_seed(&self, root_scope_uuid: &str) -> String {
        self.session_seed(&format!("relay-root:{root_scope_uuid}"))
    }
}

fn scoped_seed(kind: &str, harness: &str, installation: &str, raw: &str) -> String {
    format!(
        "provenance-agent/v1\0{kind_len}:{kind}\0{harness_len}:{harness}\0{installation_len}:{installation}\0{raw_len}:{raw}",
        kind_len = kind.len(),
        harness_len = harness.len(),
        installation_len = installation.len(),
        raw_len = raw.len(),
    )
}

/// Select which Relay observations become provenance facts.
///
/// Link-only capture keeps the provenance graph sparse: only observations
/// carrying an explicit source operation are projected, together with the
/// lifecycle boundary needed to close a session that produced one. Audit
/// capture retains the previous all-event projection for explicit compliance
/// or debugging use.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayCaptureMode {
    #[default]
    LinkOnly,
    Audit,
}

/// Optional provider-specific URL templates for telemetry references.
///
/// Templates are expanded when an observation has the corresponding ID. The
/// supported placeholders are `{trace_id}`, `{span_id}`, `{provider}`, and
/// `{project}`; placeholder values are percent-encoded as URL components.
/// Keeping templates in configuration makes the provenance graph
/// provider-neutral while still allowing self-hosted and enterprise URL
/// layouts.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RelayTelemetryUrlConfig {
    /// Provider project, workspace, or equivalent identifier.
    #[serde(default)]
    pub project: Option<String>,
    /// URL opened by a user for a trace.
    #[serde(default, alias = "trace-ui-template")]
    pub trace_ui_template: Option<String>,
    /// URL returning the trace or trace-scoped records through the provider API.
    #[serde(default, alias = "trace-api-template")]
    pub trace_api_template: Option<String>,
    /// URL opened by a user for a span.
    #[serde(default, alias = "span-ui-template")]
    pub span_ui_template: Option<String>,
    /// URL returning the span through the provider API.
    #[serde(default, alias = "span-api-template")]
    pub span_api_template: Option<String>,
}

/// URLs derived from one telemetry correlation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RelayTelemetryUrls {
    /// Trace UI URL, when a trace ID and template are available.
    pub trace_ui: Option<String>,
    /// Trace API URL, when a trace ID and template are available.
    pub trace_api: Option<String>,
    /// Span UI URL, when a span ID and template are available.
    pub span_ui: Option<String>,
    /// Span API URL, when a span ID and template are available.
    pub span_api: Option<String>,
}

impl RelayTelemetryUrlConfig {
    /// Validate URL schemes, placeholders, and required object identifiers.
    pub fn validate(&self) -> Result<(), String> {
        validate_template(
            "telemetry_links.trace_ui_template",
            self.trace_ui_template.as_deref(),
            "trace_id",
        )?;
        validate_template(
            "telemetry_links.trace_api_template",
            self.trace_api_template.as_deref(),
            "trace_id",
        )?;
        validate_template(
            "telemetry_links.span_ui_template",
            self.span_ui_template.as_deref(),
            "span_id",
        )?;
        validate_template(
            "telemetry_links.span_api_template",
            self.span_api_template.as_deref(),
            "span_id",
        )?;
        let uses_project = [
            self.trace_ui_template.as_deref(),
            self.trace_api_template.as_deref(),
            self.span_ui_template.as_deref(),
            self.span_api_template.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|template| template.contains("{project}"));
        if uses_project
            && self
                .project
                .as_deref()
                .is_none_or(|project| project.trim().is_empty())
        {
            return Err("telemetry_links.project is required by a URL template".into());
        }
        Ok(())
    }

    /// Expand configured templates for the IDs observed on one event.
    pub fn render(
        &self,
        provider: Option<&str>,
        trace_id: Option<&str>,
        span_id: Option<&str>,
    ) -> RelayTelemetryUrls {
        RelayTelemetryUrls {
            trace_ui: render_template(
                self.trace_ui_template.as_deref(),
                provider,
                self.project.as_deref(),
                trace_id,
                span_id,
            ),
            trace_api: render_template(
                self.trace_api_template.as_deref(),
                provider,
                self.project.as_deref(),
                trace_id,
                span_id,
            ),
            span_ui: render_template(
                self.span_ui_template.as_deref(),
                provider,
                self.project.as_deref(),
                trace_id,
                span_id,
            ),
            span_api: render_template(
                self.span_api_template.as_deref(),
                provider,
                self.project.as_deref(),
                trace_id,
                span_id,
            ),
        }
    }
}

fn validate_template(
    field: &str,
    template: Option<&str>,
    required_placeholder: &str,
) -> Result<(), String> {
    let Some(template) = template else {
        return Ok(());
    };
    let template = template.trim();
    if template.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if !(template.starts_with("http://") || template.starts_with("https://")) {
        return Err(format!("{field} must use an http:// or https:// URL"));
    }
    if template.chars().any(char::is_whitespace) {
        return Err(format!("{field} must not contain whitespace"));
    }

    let mut remaining = template;
    while let Some(start) = remaining.find('{') {
        if remaining[..start].contains('}') {
            return Err(format!("{field} contains an unmatched closing brace"));
        }
        let after_start = &remaining[start + 1..];
        let Some(end) = after_start.find('}') else {
            return Err(format!("{field} contains an unterminated placeholder"));
        };
        let placeholder = &after_start[..end];
        if !matches!(placeholder, "trace_id" | "span_id" | "provider" | "project") {
            return Err(format!(
                "{field} contains unsupported placeholder {{{placeholder}}}"
            ));
        }
        remaining = &after_start[end + 1..];
    }
    if remaining.contains('}') {
        return Err(format!("{field} contains an unmatched closing brace"));
    }
    if !template.contains(&format!("{{{required_placeholder}}}")) {
        return Err(format!("{field} must contain {{{required_placeholder}}}"));
    }
    Ok(())
}

fn render_template(
    template: Option<&str>,
    provider: Option<&str>,
    project: Option<&str>,
    trace_id: Option<&str>,
    span_id: Option<&str>,
) -> Option<String> {
    let template = template?;
    let mut rendered = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find('{') {
        rendered.push_str(&remaining[..start]);
        let after_start = &remaining[start + 1..];
        let end = after_start.find('}')?;
        let placeholder = &after_start[..end];
        let value = match placeholder {
            "trace_id" => trace_id?,
            "span_id" => span_id?,
            "provider" => provider?,
            "project" => project?,
            _ => return None,
        };
        rendered.push_str(&percent_encode_component(value));
        remaining = &after_start[end + 1..];
    }
    if remaining.contains('}') {
        return None;
    }
    rendered.push_str(remaining);
    Some(rendered)
}

fn percent_encode_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    encoded
}

/// Native NeMo Relay plugin configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RelayPluginConfig {
    /// Harness identity used to qualify fallback and metadata-derived keys.
    #[serde(alias = "harness-id")]
    pub harness_id: String,
    /// Installation identity used to prevent cross-workstation collisions.
    #[serde(alias = "installation-id")]
    pub installation_id: String,
    /// Path to the agent projection WASM component.
    #[serde(alias = "plugin-path")]
    pub agent_plugin_path: PathBuf,
    /// Directory containing authoritative provenance operations and heads.
    #[serde(alias = "store-path")]
    pub store_path: PathBuf,
    /// Optional append-only local ATOF evidence file.
    #[serde(default, alias = "spool-path")]
    pub spool_path: Option<PathBuf>,
    /// Registered WASM plugin ID. Defaults to the bundled `agent` projection.
    #[serde(default = "default_agent_plugin_id", alias = "agent-plugin-id")]
    pub agent_plugin_id: String,
    /// Optional telemetry provider label for externally resolvable trace IDs.
    #[serde(default, alias = "telemetry-provider")]
    pub telemetry_provider: Option<String>,
    /// Optional URL templates for persisted telemetry links.
    #[serde(default, alias = "telemetry-urls", alias = "telemetry-links")]
    pub telemetry_links: RelayTelemetryUrlConfig,
    /// Whether the graph receives sparse attribution links or all ATOF events.
    #[serde(default, alias = "capture-mode")]
    pub capture_mode: RelayCaptureMode,
}

fn default_agent_plugin_id() -> String {
    "agent".into()
}

impl RelayPluginConfig {
    /// Decode a NeMo Relay component-local JSON object.
    pub fn from_map(config: &Map<String, Json>) -> Result<Self, String> {
        serde_json::from_value(Json::Object(config.clone()))
            .map_err(|error| format!("invalid provenance plugin config: {error}"))
    }

    /// Validate fields that can be checked without opening files.
    pub fn validate(&self) -> Result<RelayIdentityConfig, String> {
        if self.agent_plugin_path.as_os_str().is_empty() {
            return Err("agent_plugin_path must not be empty".into());
        }
        if self.store_path.as_os_str().is_empty() {
            return Err("store_path must not be empty".into());
        }
        if let Some(provider) = &self.telemetry_provider
            && provider.trim().is_empty()
        {
            return Err("telemetry_provider must not be empty".into());
        }
        self.telemetry_links.validate()?;
        if self.agent_plugin_id.trim().is_empty() {
            return Err("agent_plugin_id must not be empty".into());
        }
        RelayIdentityConfig::new(&self.harness_id, &self.installation_id)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config_json() -> serde_json::Value {
        json!({
            "harness_id": "omp",
            "installation_id": "workstation-a",
            "agent_plugin_path": "/tmp/agent.wasm",
            "store_path": "/tmp/provenance"
        })
    }

    #[test]
    fn capture_mode_defaults_to_link_only() {
        let config: RelayPluginConfig =
            serde_json::from_value(config_json()).expect("decode default config");
        assert_eq!(config.capture_mode, RelayCaptureMode::LinkOnly);
    }

    #[test]
    fn capture_mode_decodes_audit() {
        let mut value = config_json();
        value["capture_mode"] = json!("audit");
        let config: RelayPluginConfig = serde_json::from_value(value).expect("decode audit config");
        assert_eq!(config.capture_mode, RelayCaptureMode::Audit);
    }
    #[test]
    fn telemetry_provider_decodes_and_validates() {
        let mut value = config_json();
        value["telemetry-provider"] = json!("phoenix");
        let config: RelayPluginConfig =
            serde_json::from_value(value).expect("decode telemetry provider");
        assert_eq!(config.telemetry_provider.as_deref(), Some("phoenix"));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn telemetry_provider_rejects_blank_value() {
        let mut value = config_json();
        value["telemetry_provider"] = json!("  ");
        let config: RelayPluginConfig =
            serde_json::from_value(value).expect("decode blank telemetry provider");
        assert_eq!(
            config.validate().expect_err("blank provider must fail"),
            "telemetry_provider must not be empty"
        );
    }
    #[test]
    fn telemetry_url_templates_decode_and_render_encoded_values() {
        let config = RelayTelemetryUrlConfig {
            project: Some("omp".into()),
            trace_ui_template: Some("https://phoenix.example/redirects/traces/{trace_id}".into()),
            trace_api_template: Some(
                "https://phoenix.example/v1/projects/{project}/spans?trace_id={trace_id}".into(),
            ),
            span_ui_template: Some("https://phoenix.example/redirects/spans/{span_id}".into()),
            span_api_template: Some(
                "https://phoenix.example/v1/projects/{project}/spans?span_id={span_id}".into(),
            ),
        };
        assert!(config.validate().is_ok());
        let urls = config.render(Some("phoenix cloud"), Some("trace/1"), Some("span/1"));
        assert_eq!(
            urls.trace_ui.as_deref(),
            Some("https://phoenix.example/redirects/traces/trace%2F1")
        );
        assert_eq!(
            urls.trace_api.as_deref(),
            Some("https://phoenix.example/v1/projects/omp/spans?trace_id=trace%2F1")
        );
        assert_eq!(
            urls.span_ui.as_deref(),
            Some("https://phoenix.example/redirects/spans/span%2F1")
        );
        assert_eq!(
            urls.span_api.as_deref(),
            Some("https://phoenix.example/v1/projects/omp/spans?span_id=span%2F1")
        );
    }

    #[test]
    fn telemetry_url_templates_require_matching_ids() {
        let mut value = config_json();
        value["telemetry_links"] = json!({
            "trace_ui_template": "http://127.0.0.1:6006/redirects/traces/{span_id}"
        });
        let config: RelayPluginConfig =
            serde_json::from_value(value).expect("decode telemetry URL config");
        assert_eq!(
            config.validate().expect_err("wrong placeholder must fail"),
            "telemetry_links.trace_ui_template must contain {trace_id}"
        );
    }
    #[test]
    fn telemetry_url_templates_reject_closing_brace_before_placeholder() {
        let mut value = config_json();
        value["telemetry_links"] = json!({
            "trace_ui_template": "http://127.0.0.1:6006/redirects}/traces/{trace_id}"
        });
        let config: RelayPluginConfig =
            serde_json::from_value(value).expect("decode telemetry URL config");
        assert_eq!(
            config.validate().expect_err("unmatched brace must fail"),
            "telemetry_links.trace_ui_template contains an unmatched closing brace"
        );
    }
}
