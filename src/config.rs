use anyhow::{Context, Result, bail};
use dirs::home_dir;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::warn;

const DEFAULT_GATEWAY_HOST: &str = "127.0.0.1";
const DEFAULT_GATEWAY_PORT: u16 = 3769;
const LEGACY_OPENCLAW_WORKSPACE_SEGMENT: &str = ".openclaw/workspace/";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub version: u32,
    pub brain: BrainConfig,
    pub gateway: GatewayConfig,
    pub slack: SlackConfig,
    pub agents: Vec<AgentConfig>,
    pub memory: MemoryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrainConfig {
    pub provider: String,
    pub workspace: String,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackConfig {
    pub enabled: bool,
    #[serde(default)]
    pub mode: SlackMode,
    pub bot_token: Option<String>,
    pub signing_secret: Option<String>,
    pub app_token: Option<String>,
    pub default_channel: Option<String>,
    pub bot_user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SlackMode {
    #[default]
    #[serde(alias = "http")]
    EventsApi,
    #[serde(alias = "socket_mode")]
    Socket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub id: String,
    pub name: String,
    pub soul: String,
    pub workspace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    pub state_path: String,
    pub max_conversation_items: usize,
    pub max_memory_items: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        let workspace = home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("work")
            .to_string_lossy()
            .to_string();
        let state_path = default_config_dir()
            .join("state.json")
            .to_string_lossy()
            .to_string();

        Self {
            version: 1,
            brain: BrainConfig {
                provider: "codex-cli".to_string(),
                workspace,
                model: None,
            },
            gateway: GatewayConfig {
                host: DEFAULT_GATEWAY_HOST.to_string(),
                port: DEFAULT_GATEWAY_PORT,
            },
            slack: SlackConfig {
                enabled: false,
                mode: SlackMode::EventsApi,
                bot_token: None,
                signing_secret: None,
                app_token: None,
                default_channel: None,
                bot_user_id: None,
            },
            agents: vec![AgentConfig {
                id: "main".to_string(),
                name: "Main Brain".to_string(),
                soul: "You are the primary OpenOrchestrator brain. Be concise, pragmatic, and action-oriented."
                    .to_string(),
                workspace: None,
            }],
            memory: MemoryConfig {
                state_path,
                max_conversation_items: 10_000,
                max_memory_items: 5_000,
            },
        }
    }
}

pub fn default_config_dir() -> PathBuf {
    if let Some(home) = home_dir() {
        return home.join(".openorchestrator");
    }
    PathBuf::from(".openorchestrator")
}

pub fn default_config_path() -> PathBuf {
    std::env::var("OPENORCHESTRATOR_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_config_dir().join("openorchestrator.json"))
}

pub fn resolve_path(input: &str) -> PathBuf {
    if let Some(stripped) = input.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(stripped);
        }
    }
    PathBuf::from(input)
}

pub async fn load_config(path: &Path) -> Result<AppConfig> {
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("failed reading config {}", path.display()))?;
    let cfg = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed parsing config {}", path.display()))?;
    Ok(cfg)
}

pub async fn load_config_with_migration(path: &Path) -> Result<AppConfig> {
    let mut cfg = load_config(path).await?;
    if migrate_legacy_openclaw_workspace_bindings(&mut cfg) {
        if let Err(err) = save_config(path, &cfg).await {
            warn!(
                config_path = %path.display(),
                error = %err,
                "failed to persist migrated workspace bindings; continuing with in-memory config"
            );
        }
    }
    Ok(cfg)
}

pub async fn load_or_default(path: &Path) -> Result<AppConfig> {
    if path.exists() {
        load_config(path).await
    } else {
        Ok(AppConfig::default())
    }
}

pub async fn save_config(path: &Path, cfg: &AppConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed creating config directory {}", parent.display()))?;
    }
    let data = serde_json::to_vec_pretty(cfg)?;
    fs::write(path, data)
        .await
        .with_context(|| format!("failed writing config {}", path.display()))?;
    Ok(())
}

pub fn migrate_legacy_openclaw_workspace_bindings(cfg: &mut AppConfig) -> bool {
    let mut changed = false;
    let brain_workspace = cfg.brain.workspace.clone();

    for agent in &mut cfg.agents {
        let Some(original_workspace) = agent.workspace.clone() else {
            continue;
        };
        if !is_legacy_openclaw_workspace(&original_workspace) {
            continue;
        }

        let normalized_workspace =
            normalize_workspace_binding(&original_workspace, &brain_workspace);
        if normalized_workspace == original_workspace {
            continue;
        }

        agent.workspace = Some(normalized_workspace.clone());
        if agent.soul.contains(&original_workspace) {
            agent.soul = agent
                .soul
                .replace(&original_workspace, &normalized_workspace);
        }
        changed = true;
    }

    changed
}

pub fn normalize_workspace_binding(workspace: &str, brain_workspace: &str) -> String {
    let Some(suffix) = legacy_openclaw_workspace_suffix(workspace) else {
        return workspace.to_string();
    };

    let root = resolve_path(brain_workspace);
    if suffix.is_empty() {
        return root.to_string_lossy().to_string();
    }
    root.join(suffix).to_string_lossy().to_string()
}

fn is_legacy_openclaw_workspace(workspace: &str) -> bool {
    legacy_openclaw_workspace_suffix(workspace).is_some()
}

fn legacy_openclaw_workspace_suffix(workspace: &str) -> Option<&str> {
    let trimmed = workspace.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(value) = trimmed.strip_prefix("~/.openclaw/workspace/") {
        return Some(value);
    }
    if trimmed == "~/.openclaw/workspace" {
        return Some("");
    }

    if let Some(value) = trimmed.strip_prefix(LEGACY_OPENCLAW_WORKSPACE_SEGMENT) {
        return Some(value);
    }
    if trimmed == ".openclaw/workspace" {
        return Some("");
    }

    let marker = format!("/{}", LEGACY_OPENCLAW_WORKSPACE_SEGMENT);
    if let Some(position) = trimmed.find(&marker) {
        let start = position + marker.len();
        return Some(&trimmed[start..]);
    }

    None
}

pub fn set_config_path_value(cfg: &mut AppConfig, dotted_path: &str, value: &str) -> Result<()> {
    let mut root: Value = serde_json::to_value(&*cfg)?;
    let parts: Vec<&str> = dotted_path
        .split('.')
        .filter(|part| !part.is_empty())
        .collect();
    if parts.is_empty() {
        bail!("path must not be empty");
    }

    let parsed_value: Value =
        serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));

    let mut cursor = &mut root;
    for key in &parts[..parts.len() - 1] {
        if !cursor.is_object() {
            *cursor = Value::Object(serde_json::Map::new());
        }
        let obj = cursor.as_object_mut().context("invalid config object")?;
        if !obj.contains_key(*key) {
            obj.insert((*key).to_string(), Value::Object(serde_json::Map::new()));
        }
        cursor = obj.get_mut(*key).context("missing key")?;
    }

    let final_key = parts[parts.len() - 1];
    if !cursor.is_object() {
        *cursor = Value::Object(serde_json::Map::new());
    }
    let obj = cursor.as_object_mut().context("invalid config object")?;
    obj.insert(final_key.to_string(), parsed_value);

    *cfg = serde_json::from_value(root).context("failed rebuilding typed config after set")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_workspace_binding_rewrites_legacy_openclaw_workspace() {
        let normalized = normalize_workspace_binding(
            "/Users/keszeyd/.openclaw/workspace/automation/secrella",
            "/Users/keszeyd/work",
        );
        assert_eq!(normalized, "/Users/keszeyd/work/automation/secrella");
    }

    #[test]
    fn migrate_legacy_openclaw_workspace_bindings_updates_agent_workspace_and_soul() {
        let mut cfg = AppConfig::default();
        cfg.brain.workspace = "/Users/keszeyd/work".to_string();
        cfg.agents.push(AgentConfig {
            id: "secrella_1".to_string(),
            name: "secrella_1".to_string(),
            soul: "Workspace: /Users/keszeyd/.openclaw/workspace/automation/secrella".to_string(),
            workspace: Some("/Users/keszeyd/.openclaw/workspace/automation/secrella".to_string()),
        });

        let changed = migrate_legacy_openclaw_workspace_bindings(&mut cfg);
        assert!(changed);

        let migrated = cfg
            .agents
            .iter()
            .find(|agent| agent.id == "secrella_1")
            .expect("expected migrated agent");
        assert_eq!(
            migrated.workspace.as_deref(),
            Some("/Users/keszeyd/work/automation/secrella")
        );
        assert!(
            migrated
                .soul
                .contains("/Users/keszeyd/work/automation/secrella")
        );
    }
}
