use anyhow::{Context, Result, bail};
use dirs::home_dir;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::fs;

const DEFAULT_GATEWAY_HOST: &str = "127.0.0.1";
const DEFAULT_GATEWAY_PORT: u16 = 3769;

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
    pub bot_token: Option<String>,
    pub signing_secret: Option<String>,
    pub app_token: Option<String>,
    pub default_channel: Option<String>,
    pub bot_user_id: Option<String>,
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

pub fn set_config_path_value(cfg: &mut AppConfig, dotted_path: &str, value: &str) -> Result<()> {
    let mut root: Value = serde_json::to_value(&*cfg)?;
    let parts: Vec<&str> = dotted_path.split('.').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        bail!("path must not be empty");
    }

    let parsed_value: Value = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));

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
