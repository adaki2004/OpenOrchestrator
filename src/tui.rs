use crate::config::AppConfig;
use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

#[derive(Debug, Serialize)]
struct ChatRequest {
    text: String,
    source: Option<String>,
    user: Option<String>,
    session: Option<String>,
    agent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    run_id: String,
    agent_id: String,
    session: String,
    reply: String,
}

pub async fn run_tui(
    cfg: AppConfig,
    gateway_url: Option<String>,
    session_override: Option<String>,
    agent_override: Option<String>,
) -> Result<()> {
    let base_url =
        gateway_url.unwrap_or_else(|| format!("http://{}:{}", cfg.gateway.host, cfg.gateway.port));
    let api_url = format!("{}/api/chat", base_url.trim_end_matches('/'));

    let client = Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(900))
        .build()
        .context("failed building http client")?;

    let mut session = session_override.unwrap_or_else(|| "tui:main".to_string());
    let mut agent = agent_override.unwrap_or_else(|| "main".to_string());

    println!("OpenOrchestrator TUI");
    println!("Gateway: {}", base_url);
    println!("Session: {}", session);
    println!("Agent: {}", agent);
    println!("Type /help for local help. /exit to quit.");

    let stdin = io::stdin();
    loop {
        print!("you> ");
        io::stdout().flush().ok();

        let mut line = String::new();
        let bytes_read = stdin.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.eq_ignore_ascii_case("/exit") {
            break;
        }

        if line.eq_ignore_ascii_case("/help") {
            println!("Local TUI controls:");
            println!("  /session <id>      switch active session");
            println!("  /agent <id>        switch active agent");
            println!("  /who               show active context");
            println!("  /exit              quit tui");
            println!(
                "\nEverything else is sent to OpenOrchestrator (including /task, /spawn, /remember, etc)."
            );
            continue;
        }

        if line.eq_ignore_ascii_case("/who") {
            println!("session={} agent={}", session, agent);
            continue;
        }

        if let Some(next) = line.strip_prefix("/session ") {
            let next = next.trim();
            if !next.is_empty() {
                session = next.to_string();
                println!("session -> {}", session);
            }
            continue;
        }

        if let Some(next) = line.strip_prefix("/agent ") {
            let next = next.trim();
            if !next.is_empty() {
                agent = next.to_string();
                println!("agent -> {}", agent);
            }
            continue;
        }

        let request = ChatRequest {
            text: line.to_string(),
            source: Some("tui".to_string()),
            user: Some(whoami()),
            session: Some(session.clone()),
            agent_id: Some(agent.clone()),
        };

        let response = client
            .post(&api_url)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("failed sending request to {}", api_url))?;

        if !response.status().is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            println!("gateway error: {}", body);
            continue;
        }

        let chat = response
            .json::<ChatResponse>()
            .await
            .context("failed parsing gateway response")?;

        let short_run = chat.run_id.chars().take(8).collect::<String>();
        println!(
            "{} [{}|run:{}]> {}",
            chat.agent_id, chat.session, short_run, chat.reply
        );
    }

    Ok(())
}

fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "tui-user".to_string())
}
