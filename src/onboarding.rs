use crate::codex::CodexRunner;
use crate::config::{AppConfig, default_config_path, load_or_default, save_config};
use anyhow::{Context, Result};
use dialoguer::{Confirm, Input, Password, theme::ColorfulTheme};
use std::path::{Path, PathBuf};
use tokio::process::Command;

pub async fn run_onboarding(path: Option<PathBuf>) -> Result<PathBuf> {
    let config_path = path.unwrap_or_else(default_config_path);
    let mut cfg = load_or_default(&config_path).await?;

    let theme = ColorfulTheme::default();

    println!("OpenOrchestrator onboarding");
    println!("Config file: {}", config_path.display());

    let codex = CodexRunner::default();
    let mut logged_in = codex.check_login_status().await.unwrap_or(false);
    if !logged_in {
        println!("Codex login not detected.");
        let run_login = Confirm::with_theme(&theme)
            .with_prompt("Run `codex login` now?")
            .default(true)
            .interact()?;

        if run_login {
            run_codex_login().await?;
            logged_in = codex.check_login_status().await.unwrap_or(false);
        }
    }

    if !logged_in {
        println!(
            "Warning: Codex is still not logged in. Gateway can start, but agent turns will fail until `codex login` succeeds."
        );
    }

    cfg.brain.workspace = Input::with_theme(&theme)
        .with_prompt("Brain workspace path")
        .default(cfg.brain.workspace.clone())
        .interact_text()?;

    let model_default = cfg.brain.model.clone().unwrap_or_default();
    let model = Input::with_theme(&theme)
        .with_prompt("Codex model override (optional, leave blank for default)")
        .default(model_default)
        .interact_text()?;
    cfg.brain.model = if model.trim().is_empty() {
        None
    } else {
        Some(model.trim().to_string())
    };

    cfg.gateway.host = Input::with_theme(&theme)
        .with_prompt("Gateway host")
        .default(cfg.gateway.host.clone())
        .interact_text()?;

    cfg.gateway.port = Input::with_theme(&theme)
        .with_prompt("Gateway port")
        .default(cfg.gateway.port)
        .interact_text()?;

    let slack_enabled = Confirm::with_theme(&theme)
        .with_prompt("Enable Slack integration (Events API mode)?")
        .default(cfg.slack.enabled)
        .interact()?;

    cfg.slack.enabled = slack_enabled;

    if slack_enabled {
        let existing_bot = cfg.slack.bot_token.clone().unwrap_or_default();
        let bot_token = Password::with_theme(&theme)
            .with_prompt("Slack bot token (xoxb-...)")
            .allow_empty_password(true)
            .with_confirmation("Confirm bot token", "Tokens do not match")
            .interact()?;

        let signing_secret = Password::with_theme(&theme)
            .with_prompt("Slack signing secret (recommended)")
            .allow_empty_password(true)
            .with_confirmation("Confirm signing secret", "Values do not match")
            .interact()?;

        let app_token = Password::with_theme(&theme)
            .with_prompt("Slack app token (xapp-..., optional for future socket mode)")
            .allow_empty_password(true)
            .with_confirmation("Confirm app token", "Values do not match")
            .interact()?;

        let default_channel = Input::with_theme(&theme)
            .with_prompt("Slack default fallback channel (optional)")
            .default(cfg.slack.default_channel.clone().unwrap_or_default())
            .interact_text()?;

        cfg.slack.bot_token = Some(if bot_token.trim().is_empty() {
            existing_bot
        } else {
            bot_token
        })
        .filter(|value| !value.trim().is_empty());

        cfg.slack.signing_secret = Some(signing_secret).filter(|value| !value.trim().is_empty());
        cfg.slack.app_token = Some(app_token).filter(|value| !value.trim().is_empty());
        cfg.slack.default_channel = Some(default_channel).filter(|value| !value.trim().is_empty());
    } else {
        cfg.slack.bot_token = None;
        cfg.slack.signing_secret = None;
        cfg.slack.app_token = None;
        cfg.slack.default_channel = None;
        cfg.slack.bot_user_id = None;
    }

    if let Some(main_agent) = cfg.agents.iter_mut().find(|agent| agent.id == "main") {
        main_agent.name = "Main Brain".to_string();
        main_agent.soul = Input::with_theme(&theme)
            .with_prompt("Main brain soul prompt")
            .default(main_agent.soul.clone())
            .interact_text()?;
    }

    save_config(&config_path, &cfg).await?;

    println!("Wrote config to {}", config_path.display());
    println!("Next steps:");
    println!("  1) openorchestrator gateway run");
    println!("  2) openorchestrator tui");

    Ok(config_path)
}

async fn run_codex_login() -> Result<()> {
    let status = Command::new("codex")
        .arg("login")
        .status()
        .await
        .context("failed to launch `codex login`")?;
    if !status.success() {
        anyhow::bail!("`codex login` exited with status {}", status);
    }
    Ok(())
}

pub async fn ensure_config_exists(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    let cfg = AppConfig::default();
    save_config(path, &cfg).await
}
