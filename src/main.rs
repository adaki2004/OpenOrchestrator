mod codex;
mod config;
mod gateway;
mod onboarding;
mod orchestrator;
mod state;
mod tui;

use anyhow::Result;
use clap::{Parser, Subcommand};
use codex::CodexRunner;
use config::{
    default_config_path, load_config_with_migration, resolve_path, save_config,
    set_config_path_value,
};
use orchestrator::Orchestrator;
use state::StateStore;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "openorchestrator",
    version,
    about = "Codex-first open-source agent orchestrator"
)]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Interactive setup: Codex + Slack + gateway config
    Onboard,

    /// Run the always-on gateway
    Gateway {
        #[command(subcommand)]
        command: GatewayCommand,
    },

    /// Open terminal chat UI attached to the gateway
    Tui {
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        agent: Option<String>,
    },

    /// Read or mutate config values
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[derive(Debug, Subcommand)]
enum GatewayCommand {
    /// Start gateway HTTP server
    Run {
        #[arg(long)]
        host: Option<String>,
        #[arg(long)]
        port: Option<u16>,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Print current config
    Show,

    /// Set a value using dotted path notation (JSON value or raw string)
    Set { path: String, value: String },

    /// Print the active config path
    Path,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .without_time()
        .init();

    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(default_config_path);

    match cli.command {
        Command::Onboard => {
            onboarding::run_onboarding(Some(config_path)).await?;
        }
        Command::Gateway { command } => match command {
            GatewayCommand::Run { host, port } => {
                onboarding::ensure_config_exists(&config_path).await?;
                let cfg = load_config_with_migration(&config_path).await?;
                let state_path = resolve_path(&cfg.memory.state_path);
                let state = StateStore::load(
                    &state_path,
                    cfg.memory.max_conversation_items,
                    cfg.memory.max_memory_items,
                )
                .await?;

                let cfg_handle = Arc::new(RwLock::new(cfg));
                let orchestrator = Arc::new(Orchestrator::new(
                    config_path.clone(),
                    cfg_handle,
                    state,
                    CodexRunner::default(),
                ));

                gateway::run_gateway(orchestrator, host, port).await?;
            }
        },
        Command::Tui {
            url,
            session,
            agent,
        } => {
            onboarding::ensure_config_exists(&config_path).await?;
            let cfg = load_config_with_migration(&config_path).await?;
            tui::run_tui(cfg, url, session, agent).await?;
        }
        Command::Config { command } => {
            onboarding::ensure_config_exists(&config_path).await?;
            match command {
                ConfigCommand::Show => {
                    let cfg = load_config_with_migration(&config_path).await?;
                    println!("{}", serde_json::to_string_pretty(&cfg)?);
                }
                ConfigCommand::Set { path, value } => {
                    let mut cfg = load_config_with_migration(&config_path).await?;
                    set_config_path_value(&mut cfg, &path, &value)?;
                    save_config(&config_path, &cfg).await?;
                    println!("updated {} in {}", path, config_path.display());
                }
                ConfigCommand::Path => {
                    println!("{}", config_path.display());
                }
            }
        }
    }

    Ok(())
}
