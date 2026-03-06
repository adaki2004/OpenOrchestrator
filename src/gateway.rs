use crate::config::{SlackMode, normalize_workspace_binding, resolve_path};
use crate::orchestrator::{IncomingMessage, Orchestrator};
use crate::state::TaskItem;
use anyhow::{Context, Result, bail};
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::fs;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

type HmacSha256 = Hmac<Sha256>;
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;
const MAX_CHAT_TEXT_CHARS: usize = 16_000;
const MAX_CHAT_FIELD_CHARS: usize = 128;
const MAX_SLACK_ATTACHMENTS: usize = 5;
const MAX_SLACK_ATTACHMENT_BYTES: u64 = 20 * 1024 * 1024;
const MAX_SLACK_OUTPUT_UPLOADS: usize = 3;

#[derive(Debug, Deserialize)]
pub struct ChatRequest {
    pub text: String,
    pub source: Option<String>,
    pub user: Option<String>,
    pub session: Option<String>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ChatResponse {
    pub run_id: String,
    pub agent_id: String,
    pub session: String,
    pub reply: String,
}

#[derive(Clone)]
struct SlackRuntime {
    enabled: bool,
    mode: SlackMode,
    bot_token: Option<String>,
    app_token: Option<String>,
    signing_secret: Option<String>,
    bot_user_id: Option<String>,
    default_channel: Option<String>,
}

#[derive(Clone)]
struct GatewayState {
    orchestrator: Arc<Orchestrator>,
    started_at: Instant,
    http_client: Client,
    slack: SlackRuntime,
}

#[derive(Debug, Clone)]
struct SlackAttachment {
    name: String,
    download_url: String,
    size_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    uptime_seconds: u64,
    slack_enabled: bool,
}

pub async fn run_gateway(
    orchestrator: Arc<Orchestrator>,
    host_override: Option<String>,
    port_override: Option<u16>,
) -> Result<()> {
    let cfg = orchestrator.config_handle().read().await.clone();
    let host = host_override.unwrap_or_else(|| cfg.gateway.host.clone());
    let port = port_override.unwrap_or(cfg.gateway.port);

    let http_client = Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("failed building HTTP client")?;

    let mut slack_runtime = SlackRuntime {
        enabled: cfg.slack.enabled,
        mode: cfg.slack.mode.clone(),
        bot_token: cfg.slack.bot_token.clone(),
        app_token: cfg.slack.app_token.clone(),
        signing_secret: cfg.slack.signing_secret.clone(),
        bot_user_id: cfg.slack.bot_user_id.clone(),
        default_channel: cfg.slack.default_channel.clone(),
    };

    validate_slack_runtime(&slack_runtime)?;

    if slack_runtime.enabled && slack_runtime.bot_user_id.is_none() {
        if let Some(token) = slack_runtime.bot_token.as_deref() {
            match fetch_slack_bot_user_id(&http_client, token).await {
                Ok(user_id) => {
                    info!(%user_id, "resolved Slack bot user id via auth.test");
                    slack_runtime.bot_user_id = Some(user_id);
                }
                Err(err) => {
                    warn!(error = %err, "could not resolve Slack bot user id; mention gating may be less accurate");
                }
            }
        }
    }

    let state = GatewayState {
        orchestrator,
        started_at: Instant::now(),
        http_client,
        slack: slack_runtime,
    };

    let app = build_router(state.clone());

    if state.slack.enabled && state.slack.mode == SlackMode::Socket {
        let socket_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_slack_socket_mode(socket_state).await {
                error!(error = %err, "slack socket mode stopped");
            }
        });
    }

    let listener = TcpListener::bind((host.as_str(), port))
        .await
        .with_context(|| format!("failed binding gateway at {}:{}", host, port))?;

    info!(
        "OpenOrchestrator gateway listening on http://{}:{}",
        host, port
    );
    axum::serve(listener, app)
        .await
        .context("gateway server failed")?;

    Ok(())
}

fn build_router(state: GatewayState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/chat", post(chat))
        .route("/api/tasks", get(tasks))
        .route("/slack/events", post(slack_events))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

async fn health(State(state): State<GatewayState>) -> impl IntoResponse {
    let payload = HealthResponse {
        status: "ok",
        uptime_seconds: state.started_at.elapsed().as_secs(),
        slack_enabled: state.slack.enabled,
    };
    (StatusCode::OK, axum::Json(payload))
}

async fn tasks(State(state): State<GatewayState>) -> impl IntoResponse {
    let tasks: Vec<TaskItem> = state.orchestrator.state().await.list_tasks().await;
    (StatusCode::OK, axum::Json(tasks))
}

async fn chat(
    State(state): State<GatewayState>,
    axum::Json(req): axum::Json<ChatRequest>,
) -> impl IntoResponse {
    let text = req.text.trim().to_string();
    if text.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "text must not be empty"})),
        )
            .into_response();
    }

    if exceeds_char_limit(&text, MAX_CHAT_TEXT_CHARS) {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            axum::Json(json!({"error": format!("text must be <= {} chars", MAX_CHAT_TEXT_CHARS)})),
        )
            .into_response();
    }

    let source = match normalize_source(req.source) {
        Ok(value) => value,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": message})),
            )
                .into_response();
        }
    };
    let user = match normalize_optional_chat_field(req.user, "user") {
        Ok(value) => value,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": message})),
            )
                .into_response();
        }
    };
    let session = match normalize_optional_chat_field(req.session, "session") {
        Ok(value) => value,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": message})),
            )
                .into_response();
        }
    };
    let agent_id = match normalize_optional_chat_field(req.agent_id, "agent_id") {
        Ok(value) => value,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": message})),
            )
                .into_response();
        }
    };

    let incoming = IncomingMessage {
        source,
        user,
        session,
        agent_id,
        text,
    };

    match state.orchestrator.handle_message(incoming).await {
        Ok(reply) => (
            StatusCode::OK,
            axum::Json(ChatResponse {
                run_id: reply.run_id,
                agent_id: reply.agent_id,
                session: reply.session,
                reply: reply.reply,
            }),
        )
            .into_response(),
        Err(err) => {
            error!(error = %err, "failed to handle chat request");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "internal server error"})),
            )
                .into_response()
        }
    }
}

async fn slack_events(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !state.slack.enabled {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": "slack integration disabled"})),
        )
            .into_response();
    }

    if state.slack.mode == SlackMode::Socket {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": "slack socket mode enabled; webhook endpoint disabled"})),
        )
            .into_response();
    }

    let secret = match state.slack.signing_secret.as_deref() {
        Some(secret) => secret,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "slack signing secret is not configured"})),
            )
                .into_response();
        }
    };

    if !verify_slack_signature(secret, &headers, &body) {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "invalid slack signature"})),
        )
            .into_response();
    }

    let payload: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": format!("invalid json: {}", err)})),
            )
                .into_response();
        }
    };

    let event_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
    if event_type == "url_verification" {
        let challenge = payload
            .get("challenge")
            .and_then(Value::as_str)
            .unwrap_or_default();
        return (StatusCode::OK, axum::Json(json!({"challenge": challenge}))).into_response();
    }

    if event_type == "event_callback" {
        let app_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = process_slack_event(app_state, payload).await {
                error!(error = %err, "failed handling slack event");
            }
        });
        return (StatusCode::OK, axum::Json(json!({"ok": true}))).into_response();
    }

    (StatusCode::OK, axum::Json(json!({"ok": true}))).into_response()
}

async fn process_slack_event(state: GatewayState, payload: Value) -> Result<()> {
    let event = payload.get("event").cloned().unwrap_or(Value::Null);
    let event_kind = event.get("type").and_then(Value::as_str).unwrap_or("");

    if event_kind != "app_mention" && event_kind != "message" {
        return Ok(());
    }

    if event.get("bot_id").is_some() {
        return Ok(());
    }

    if event
        .get("subtype")
        .and_then(Value::as_str)
        .is_some_and(|subtype| subtype != "thread_broadcast")
    {
        return Ok(());
    }

    let channel = event
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let user = event
        .get("user")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let text_raw = event
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    if channel.is_empty() || text_raw.trim().is_empty() {
        return Ok(());
    }

    if should_ignore_channel_message(&state.slack, &channel, event_kind, &text_raw) {
        return Ok(());
    }

    let thread_ts = event
        .get("thread_ts")
        .and_then(Value::as_str)
        .or_else(|| event.get("ts").and_then(Value::as_str))
        .map(ToString::to_string);
    let session = format!(
        "slack:{}:{}",
        channel,
        thread_ts.clone().unwrap_or_else(|| "root".to_string())
    );

    let mut cleaned_text = clean_slack_mention(&state.slack, &text_raw)
        .trim()
        .to_string();
    match build_slack_attachment_context(&state, &event, &session).await {
        Ok(Some(context)) => {
            if !cleaned_text.is_empty() {
                cleaned_text.push_str("\n\n");
            }
            cleaned_text.push_str(&context);
        }
        Ok(None) => {}
        Err(err) => {
            warn!(error = %err, "failed to process Slack attachments");
        }
    }

    let cleaned_text = cleaned_text.trim().to_string();
    if cleaned_text.is_empty() {
        return Ok(());
    }

    let reply = match state
        .orchestrator
        .handle_message(IncomingMessage {
            source: "slack".to_string(),
            user,
            session: Some(session.clone()),
            agent_id: None,
            text: cleaned_text,
        })
        .await
    {
        Ok(reply) => reply,
        Err(err) => {
            let error_text = format!("[openorchestrator error] {}", err);
            error!(
                channel = %channel,
                session = %session,
                error = %err,
                "failed processing Slack message"
            );
            if let Some(token) = state.slack.bot_token.as_deref() {
                if let Err(send_err) = send_slack_message(
                    &state.http_client,
                    token,
                    &channel,
                    &error_text,
                    thread_ts.as_deref(),
                    state.slack.default_channel.as_deref(),
                )
                .await
                {
                    warn!(error = %send_err, "failed sending Slack error message");
                }
            }
            return Ok(());
        }
    };

    if let Some(token) = state.slack.bot_token.as_deref() {
        send_slack_message(
            &state.http_client,
            token,
            &channel,
            &reply.reply,
            thread_ts.as_deref(),
            state.slack.default_channel.as_deref(),
        )
        .await?;

        if let Err(err) = upload_reply_output_files_to_slack(
            &state,
            token,
            &channel,
            thread_ts.as_deref(),
            state.slack.default_channel.as_deref(),
            &reply.reply,
        )
        .await
        {
            warn!(error = %err, "failed uploading reply output files to Slack");
        }
    }

    Ok(())
}

fn should_ignore_channel_message(
    slack: &SlackRuntime,
    channel: &str,
    event_kind: &str,
    text: &str,
) -> bool {
    if event_kind == "app_mention" {
        return false;
    }

    let is_public_or_private_channel = channel.starts_with('C') || channel.starts_with('G');
    if !is_public_or_private_channel {
        return false;
    }

    if let Some(bot_user_id) = slack.bot_user_id.as_deref() {
        let mention = format!("<@{}>", bot_user_id);
        return !text.contains(&mention);
    }

    false
}

fn clean_slack_mention(slack: &SlackRuntime, text: &str) -> String {
    if let Some(bot_user_id) = slack.bot_user_id.as_deref() {
        let mention = format!("<@{}>", bot_user_id);
        return text.replace(&mention, "").trim().to_string();
    }
    text.to_string()
}

async fn build_slack_attachment_context(
    state: &GatewayState,
    event: &Value,
    session: &str,
) -> Result<Option<String>> {
    let token = match state.slack.bot_token.as_deref() {
        Some(value) if !value.trim().is_empty() => value,
        _ => return Ok(None),
    };

    let attachments = extract_slack_attachments(event);
    if attachments.is_empty() {
        return Ok(None);
    }

    let workspaces = collect_agent_workspaces(state).await;
    let session_component = sanitize_path_component(session, "slack-session");
    let mut lines = vec!["Attached files saved to agent workspaces:".to_string()];
    let mut saved_count = 0usize;
    let mut index = 0usize;

    for attachment in attachments.into_iter().take(MAX_SLACK_ATTACHMENTS) {
        index += 1;
        if attachment
            .size_bytes
            .is_some_and(|size| size > MAX_SLACK_ATTACHMENT_BYTES)
        {
            lines.push(format!(
                "- {} (skipped: file is larger than {} bytes)",
                attachment.name, MAX_SLACK_ATTACHMENT_BYTES
            ));
            continue;
        }

        let bytes =
            match download_slack_attachment(&state.http_client, token, &attachment.download_url)
                .await
            {
                Ok(bytes) => bytes,
                Err(err) => {
                    lines.push(format!("- {} (download failed: {})", attachment.name, err));
                    continue;
                }
            };

        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SLACK_ATTACHMENT_BYTES {
            lines.push(format!(
                "- {} (skipped: downloaded payload exceeded {} bytes)",
                attachment.name, MAX_SLACK_ATTACHMENT_BYTES
            ));
            continue;
        }

        let safe_name = sanitize_file_name(&attachment.name);
        let relative_path = PathBuf::from(".openorchestrator")
            .join("inbox")
            .join(&session_component)
            .join(format!("{:02}-{}", index, safe_name));

        let mut persisted_to_workspace = false;
        for workspace in &workspaces {
            match write_attachment_bytes(workspace, &relative_path, &bytes).await {
                Ok(()) => {
                    persisted_to_workspace = true;
                }
                Err(err) => {
                    warn!(
                        workspace = %workspace.display(),
                        attachment = %attachment.name,
                        error = %err,
                        "failed writing Slack attachment to workspace"
                    );
                }
            }
        }

        if persisted_to_workspace {
            saved_count += 1;
            lines.push(format!(
                "- {} -> {}",
                attachment.name,
                relative_path.display()
            ));
        } else {
            lines.push(format!(
                "- {} (save failed for all workspaces)",
                attachment.name
            ));
        }
    }

    if saved_count == 0 {
        return Ok(None);
    }

    lines.push("Use the saved local paths when running scripts.".to_string());
    Ok(Some(lines.join("\n")))
}

fn extract_slack_attachments(event: &Value) -> Vec<SlackAttachment> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();

    let Some(files) = event.get("files").and_then(Value::as_array) else {
        return output;
    };

    for item in files {
        let download_url = item
            .get("url_private_download")
            .and_then(Value::as_str)
            .or_else(|| item.get("url_private").and_then(Value::as_str))
            .unwrap_or_default()
            .trim();
        if download_url.is_empty() {
            continue;
        }

        let id = item
            .get("id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let dedupe_key = id.clone().unwrap_or_else(|| download_url.to_string());
        if !seen.insert(dedupe_key) {
            continue;
        }

        let name = item
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("attachment.bin")
            .to_string();
        let size_bytes = item.get("size").and_then(Value::as_u64);

        output.push(SlackAttachment {
            name,
            download_url: download_url.to_string(),
            size_bytes,
        });
    }

    output
}

async fn collect_agent_workspaces(state: &GatewayState) -> Vec<PathBuf> {
    let config_handle = state.orchestrator.config_handle();
    let cfg = config_handle.read().await;
    let mut output = Vec::new();
    let mut seen = HashSet::new();

    let normalized_brain_workspace =
        normalize_workspace_binding(&cfg.brain.workspace, &cfg.brain.workspace);
    let default_workspace = resolve_path(&normalized_brain_workspace);

    for agent in &cfg.agents {
        let workspace = agent
            .workspace
            .as_deref()
            .map(|value| normalize_workspace_binding(value, &cfg.brain.workspace))
            .map(|value| resolve_path(&value))
            .unwrap_or_else(|| default_workspace.clone());
        let key = workspace.to_string_lossy().to_string();
        if seen.insert(key) {
            output.push(workspace);
        }
    }

    if output.is_empty() {
        output.push(default_workspace);
    }

    output
}

async fn download_slack_attachment(client: &Client, token: &str, url: &str) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .context("failed calling Slack file URL")?;

    let status = response.status();
    if !status.is_success() {
        bail!("Slack file download failed with status {}", status);
    }

    let bytes = response
        .bytes()
        .await
        .context("failed reading Slack file bytes")?;
    Ok(bytes.to_vec())
}

async fn write_attachment_bytes(
    workspace: &Path,
    relative_path: &Path,
    bytes: &[u8],
) -> Result<()> {
    let full_path = workspace.join(relative_path);
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed creating attachment parent directory {}",
                parent.display()
            )
        })?;
    }

    fs::write(&full_path, bytes)
        .await
        .with_context(|| format!("failed writing attachment {}", full_path.display()))?;
    Ok(())
}

fn sanitize_file_name(value: &str) -> String {
    let sanitized = sanitize_path_component(value, "attachment.bin");
    let mut output = sanitized
        .chars()
        .take(96)
        .collect::<String>()
        .trim()
        .to_string();
    if output.is_empty() {
        output = "attachment.bin".to_string();
    }
    output
}

fn sanitize_path_component(value: &str, fallback: &str) -> String {
    let mut output = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();

    output = output.trim_matches('.').trim_matches('-').to_string();
    if output.is_empty() {
        fallback.to_string()
    } else {
        output
    }
}

fn resolve_slack_target_channel(channel: &str, default_channel: Option<&str>) -> String {
    if channel.trim().is_empty() {
        default_channel.unwrap_or("").trim().to_string()
    } else {
        channel.trim().to_string()
    }
}

fn extract_slack_output_paths(reply_text: &str) -> Vec<PathBuf> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();

    for token in reply_text.split_whitespace() {
        let Some(candidate) = extract_slack_output_path_token(token) else {
            continue;
        };

        let key = candidate.to_string();
        if seen.insert(key.clone()) {
            output.push(PathBuf::from(key));
        }
    }

    output
}

fn extract_slack_output_path_token(token: &str) -> Option<String> {
    let token = token
        .trim_matches(|ch: char| {
            matches!(
                ch,
                '`' | '"' | '\'' | ',' | ';' | ':' | '(' | ')' | '[' | ']' | '<' | '>'
            )
        })
        .trim_end_matches(|ch: char| ch == '.' || ch == '-');

    if token.is_empty() {
        return None;
    }

    if token.contains("://") {
        return None;
    }

    if let Some(outbox_idx) = token.find("/.openorchestrator/outbox/") {
        let absolute_start = token.find('/').unwrap_or(outbox_idx);
        return Some(token[absolute_start..].to_string());
    }

    if let Some(relative_start) = token.find(".openorchestrator/outbox/") {
        return Some(token[relative_start..].to_string());
    }

    None
}

fn candidate_output_upload_paths(path: &Path, workspace_roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();

    if path.is_absolute() {
        let key = path.to_string_lossy().to_string();
        if seen.insert(key) {
            output.push(path.to_path_buf());
        }
        return output;
    }

    if let Ok(cwd) = std::env::current_dir() {
        let candidate = cwd.join(path);
        let key = candidate.to_string_lossy().to_string();
        if seen.insert(key) {
            output.push(candidate);
        }
    }

    for workspace in workspace_roots {
        let candidate = workspace.join(path);
        let key = candidate.to_string_lossy().to_string();
        if seen.insert(key) {
            output.push(candidate);
        }
    }

    output
}

async fn resolve_output_path_for_upload(
    path: &Path,
    workspace_roots: &[PathBuf],
) -> Option<PathBuf> {
    for candidate in candidate_output_upload_paths(path, workspace_roots) {
        match fs::metadata(&candidate).await {
            Ok(metadata) if metadata.is_file() => return Some(candidate),
            _ => {}
        }
    }
    None
}

async fn upload_reply_output_files_to_slack(
    state: &GatewayState,
    token: &str,
    channel: &str,
    thread_ts: Option<&str>,
    default_channel: Option<&str>,
    reply_text: &str,
) -> Result<()> {
    let target_channel = resolve_slack_target_channel(channel, default_channel);
    if target_channel.is_empty() {
        return Ok(());
    }

    let output_paths = extract_slack_output_paths(reply_text);
    if output_paths.is_empty() {
        return Ok(());
    }

    let workspace_roots = collect_agent_workspaces(state).await;

    for path in output_paths.into_iter().take(MAX_SLACK_OUTPUT_UPLOADS) {
        let Some(resolved_path) = resolve_output_path_for_upload(&path, &workspace_roots).await
        else {
            warn!(
                path = %path.display(),
                "could not resolve output file path for Slack upload"
            );
            continue;
        };

        if let Err(err) = upload_slack_file_from_path(
            &state.http_client,
            token,
            &target_channel,
            thread_ts,
            &resolved_path,
        )
        .await
        {
            warn!(
                path = %resolved_path.display(),
                error = %err,
                "failed uploading output file to Slack"
            );
        }
    }

    Ok(())
}

async fn upload_slack_file_from_path(
    client: &Client,
    token: &str,
    channel: &str,
    thread_ts: Option<&str>,
    path: &Path,
) -> Result<()> {
    let metadata = fs::metadata(path)
        .await
        .with_context(|| format!("failed reading output file metadata {}", path.display()))?;
    if !metadata.is_file() {
        bail!("output path is not a file: {}", path.display());
    }

    if metadata.len() > MAX_SLACK_ATTACHMENT_BYTES {
        bail!(
            "output file {} exceeds max upload size {} bytes",
            path.display(),
            MAX_SLACK_ATTACHMENT_BYTES
        );
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("output file has invalid utf-8 name")?;
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("failed reading output file {}", path.display()))?;

    upload_slack_file_bytes(client, token, channel, thread_ts, file_name, bytes).await
}

async fn upload_slack_file_bytes(
    client: &Client,
    token: &str,
    channel: &str,
    thread_ts: Option<&str>,
    file_name: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let upload_init = client
        .post("https://slack.com/api/files.getUploadURLExternal")
        .bearer_auth(token)
        .form(&[
            ("filename", file_name.to_string()),
            ("length", bytes.len().to_string()),
        ])
        .send()
        .await
        .context("failed calling Slack files.getUploadURLExternal")?;

    let init_status = upload_init.status();
    let init_body: Value = upload_init
        .json()
        .await
        .context("failed parsing Slack files.getUploadURLExternal response")?;
    if !init_status.is_success() {
        bail!(
            "Slack files.getUploadURLExternal HTTP {}: {}",
            init_status,
            init_body
        );
    }
    if !init_body
        .get("ok")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        bail!("Slack files.getUploadURLExternal failed: {}", init_body);
    }

    let upload_url = init_body
        .get("upload_url")
        .and_then(Value::as_str)
        .context("Slack files.getUploadURLExternal missing upload_url")?;
    let file_id = init_body
        .get("file_id")
        .and_then(Value::as_str)
        .context("Slack files.getUploadURLExternal missing file_id")?;

    let upload_response = client
        .post(upload_url)
        .header("content-type", "application/octet-stream")
        .body(bytes)
        .send()
        .await
        .context("failed uploading file bytes to Slack upload_url")?;
    if !upload_response.status().is_success() {
        bail!(
            "Slack upload_url returned status {}",
            upload_response.status()
        );
    }

    let mut complete_payload = json!({
        "files": [{ "id": file_id, "title": file_name }],
        "channel_id": channel,
    });
    if let Some(thread_ts) = thread_ts.filter(|value| !value.trim().is_empty()) {
        complete_payload["thread_ts"] = Value::String(thread_ts.to_string());
    }

    let complete_response = client
        .post("https://slack.com/api/files.completeUploadExternal")
        .bearer_auth(token)
        .json(&complete_payload)
        .send()
        .await
        .context("failed calling Slack files.completeUploadExternal")?;

    let complete_status = complete_response.status();
    let complete_body: Value = complete_response
        .json()
        .await
        .context("failed parsing Slack files.completeUploadExternal response")?;
    if !complete_status.is_success() {
        bail!(
            "Slack files.completeUploadExternal HTTP {}: {}",
            complete_status,
            complete_body
        );
    }
    if !complete_body
        .get("ok")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        bail!(
            "Slack files.completeUploadExternal failed: {}",
            complete_body
        );
    }

    Ok(())
}

async fn send_slack_message(
    client: &Client,
    token: &str,
    channel: &str,
    text: &str,
    thread_ts: Option<&str>,
    default_channel: Option<&str>,
) -> Result<()> {
    let target_channel = resolve_slack_target_channel(channel, default_channel);
    if target_channel.is_empty() {
        warn!("Slack reply skipped because no channel was available");
        return Ok(());
    }

    let mut payload = json!({
        "channel": target_channel,
        "text": text,
    });

    if let Some(thread_ts) = thread_ts.filter(|value| !value.trim().is_empty()) {
        payload["thread_ts"] = Value::String(thread_ts.to_string());
    }

    let response = client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(token)
        .json(&payload)
        .send()
        .await
        .context("failed calling Slack chat.postMessage")?;

    let status = response.status();
    let value: Value = response
        .json()
        .await
        .context("failed parsing Slack chat.postMessage response")?;

    let ok = value.get("ok").and_then(Value::as_bool).unwrap_or(false);
    if !status.is_success() || !ok {
        warn!(status = %status, body = %value, "Slack message send failed");
    }

    Ok(())
}

fn validate_slack_runtime(slack: &SlackRuntime) -> Result<()> {
    if !slack.enabled {
        return Ok(());
    }

    if !is_present(slack.bot_token.as_deref()) {
        bail!("slack.enabled requires slack.bot_token for outbound replies");
    }

    match slack.mode {
        SlackMode::EventsApi => {
            if !is_present(slack.signing_secret.as_deref()) {
                bail!("slack.enabled with events_api mode requires slack.signing_secret");
            }
        }
        SlackMode::Socket => {
            if !is_present(slack.app_token.as_deref()) {
                bail!("slack.enabled with socket mode requires slack.app_token");
            }
        }
    }

    Ok(())
}

async fn run_slack_socket_mode(state: GatewayState) -> Result<()> {
    info!("starting Slack socket mode loop");
    loop {
        match run_slack_socket_connection(state.clone()).await {
            Ok(()) => {
                warn!("Slack socket mode disconnected; reconnecting");
            }
            Err(err) => {
                warn!(error = %err, "Slack socket mode error; reconnecting");
            }
        }
        sleep(Duration::from_secs(3)).await;
    }
}

async fn run_slack_socket_connection(state: GatewayState) -> Result<()> {
    let app_token = state
        .slack
        .app_token
        .as_deref()
        .context("slack.app_token is missing for socket mode")?;

    let socket_url = fetch_slack_socket_url(&state.http_client, app_token).await?;
    let (ws_stream, _response) = connect_async(socket_url.as_str())
        .await
        .context("failed connecting Slack socket websocket")?;
    info!("Slack socket mode connected");

    let (mut writer, mut reader) = ws_stream.split();
    while let Some(next_message) = reader.next().await {
        let message = match next_message {
            Ok(message) => message,
            Err(err) => {
                return Err(err).context("slack socket receive error");
            }
        };

        match message {
            Message::Text(text) => {
                let payload: Value = match serde_json::from_str(text.as_ref()) {
                    Ok(value) => value,
                    Err(err) => {
                        warn!(error = %err, "failed parsing Slack socket payload");
                        continue;
                    }
                };

                if let Some(envelope_id) = payload.get("envelope_id").and_then(Value::as_str) {
                    let ack = json!({"envelope_id": envelope_id}).to_string();
                    writer
                        .send(Message::Text(ack.into()))
                        .await
                        .context("failed sending Slack socket ack")?;
                }

                let envelope_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
                if envelope_type == "events_api" {
                    let callback_payload = payload.get("payload").cloned().unwrap_or(Value::Null);
                    let app_state = state.clone();
                    tokio::spawn(async move {
                        if let Err(err) = process_slack_event(app_state, callback_payload).await {
                            error!(error = %err, "failed handling Slack socket event");
                        }
                    });
                } else if envelope_type == "disconnect" {
                    warn!(body = %payload, "Slack requested socket disconnect");
                    break;
                }
            }
            Message::Ping(payload) => {
                writer
                    .send(Message::Pong(payload))
                    .await
                    .context("failed sending Slack socket pong")?;
            }
            Message::Close(frame) => {
                info!(?frame, "Slack socket closed");
                break;
            }
            Message::Binary(_) | Message::Pong(_) => {}
            _ => {}
        }
    }

    Ok(())
}

async fn fetch_slack_socket_url(client: &Client, app_token: &str) -> Result<String> {
    let response = client
        .post("https://slack.com/api/apps.connections.open")
        .bearer_auth(app_token)
        .send()
        .await
        .context("failed calling Slack apps.connections.open")?;

    let status = response.status();
    let value: Value = response
        .json()
        .await
        .context("failed parsing Slack apps.connections.open response")?;

    let ok = value.get("ok").and_then(Value::as_bool).unwrap_or(false);
    if !status.is_success() || !ok {
        bail!(
            "Slack apps.connections.open failed (status={}): {}",
            status,
            value
        );
    }

    let url = value
        .get("url")
        .and_then(Value::as_str)
        .context("Slack apps.connections.open missing websocket url")?;
    Ok(url.to_string())
}

fn verify_slack_signature(secret: &str, headers: &HeaderMap, body: &[u8]) -> bool {
    let signature = headers
        .get("x-slack-signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let timestamp = headers
        .get("x-slack-request-timestamp")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if signature.is_empty() || timestamp.is_empty() {
        return false;
    }

    let provided_digest = match signature.strip_prefix("v0=") {
        Some(value) => match hex::decode(value) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        },
        None => return false,
    };

    let ts = match timestamp.parse::<i64>() {
        Ok(value) => value,
        Err(_) => return false,
    };
    let age = (Utc::now().timestamp() - ts).abs();
    if age > 300 {
        return false;
    }

    let signed_payload = format!("v0:{}:{}", timestamp, String::from_utf8_lossy(body));
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(signed_payload.as_bytes());

    mac.verify_slice(&provided_digest).is_ok()
}

fn normalize_source(source: Option<String>) -> std::result::Result<String, String> {
    let candidate = source
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("api");

    if exceeds_char_limit(candidate, MAX_CHAT_FIELD_CHARS) {
        return Err(format!("source must be <= {} chars", MAX_CHAT_FIELD_CHARS));
    }

    Ok(candidate.to_string())
}

fn normalize_optional_chat_field(
    value: Option<String>,
    field_name: &str,
) -> std::result::Result<Option<String>, String> {
    match value
        .as_deref()
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        Some(normalized) if exceeds_char_limit(normalized, MAX_CHAT_FIELD_CHARS) => Err(format!(
            "{} must be <= {} chars",
            field_name, MAX_CHAT_FIELD_CHARS
        )),
        Some(normalized) => Ok(Some(normalized.to_string())),
        None => Ok(None),
    }
}

fn exceeds_char_limit(value: &str, limit: usize) -> bool {
    value.chars().count() > limit
}

fn is_present(value: Option<&str>) -> bool {
    value.is_some_and(|entry| !entry.trim().is_empty())
}

async fn fetch_slack_bot_user_id(client: &Client, token: &str) -> Result<String> {
    let response = client
        .post("https://slack.com/api/auth.test")
        .bearer_auth(token)
        .send()
        .await
        .context("failed calling Slack auth.test")?;

    let status = response.status();
    let value: Value = response
        .json()
        .await
        .context("failed parsing Slack auth.test response")?;

    if !status.is_success() {
        anyhow::bail!("Slack auth.test HTTP {}: {}", status, value);
    }

    let ok = value.get("ok").and_then(Value::as_bool).unwrap_or(false);
    if !ok {
        anyhow::bail!("Slack auth.test failed: {}", value);
    }

    let user_id = value
        .get("user_id")
        .and_then(Value::as_str)
        .context("Slack auth.test missing user_id")?;

    Ok(user_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex::CodexRunner;
    use crate::config::AppConfig;
    use crate::state::StateStore;
    use axum::body::{Body, to_bytes};
    use axum::http::{HeaderValue, Request};
    use serde_json::{Value, json};
    use std::path::PathBuf;
    use tokio::fs;
    use tokio::sync::RwLock;
    use tower::ServiceExt;
    use uuid::Uuid;

    async fn test_app(slack: SlackRuntime) -> (Router, PathBuf) {
        let base_dir =
            std::env::temp_dir().join(format!("openorchestrator-tests-{}", Uuid::new_v4()));
        fs::create_dir_all(&base_dir)
            .await
            .expect("failed to create test temp dir");

        let config_path = base_dir.join("openorchestrator.json");
        let state_path = base_dir.join("state.json");
        let state = StateStore::load(&state_path, 200, 200)
            .await
            .expect("failed to initialize state store");

        let orchestrator = Arc::new(Orchestrator::new(
            config_path,
            Arc::new(RwLock::new(AppConfig::default())),
            state,
            CodexRunner::default(),
        ));

        let http_client = Client::builder()
            .build()
            .expect("failed building http client for tests");

        let router = build_router(GatewayState {
            orchestrator,
            started_at: Instant::now(),
            http_client,
            slack,
        });

        (router, base_dir)
    }

    fn default_test_slack(enabled: bool) -> SlackRuntime {
        SlackRuntime {
            enabled,
            mode: SlackMode::EventsApi,
            bot_token: None,
            app_token: None,
            signing_secret: None,
            bot_user_id: None,
            default_channel: None,
        }
    }

    async fn read_json_response(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("failed to read response body");
        serde_json::from_slice(&bytes).expect("failed parsing json response")
    }

    #[tokio::test]
    async fn health_endpoint_returns_status() {
        let (app, temp_dir) = test_app(default_test_slack(false)).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/health")
                    .body(Body::empty())
                    .expect("failed building request"),
            )
            .await
            .expect("health request failed");

        assert_eq!(response.status(), StatusCode::OK);
        let payload = read_json_response(response).await;
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["slack_enabled"], false);
        assert!(payload["uptime_seconds"].as_u64().is_some());

        let _ = fs::remove_dir_all(temp_dir).await;
    }

    #[tokio::test]
    async fn api_chat_routes_commands_without_codex_execution() {
        let (app, temp_dir) = test_app(default_test_slack(false)).await;
        let request_body = json!({
            "text": "/help",
            "source": "api",
            "user": "tester",
            "session": "session-1",
            "agent_id": "main"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/chat")
                    .header("content-type", "application/json")
                    .body(Body::from(request_body.to_string()))
                    .expect("failed building request"),
            )
            .await
            .expect("chat request failed");

        assert_eq!(response.status(), StatusCode::OK);
        let payload = read_json_response(response).await;
        assert_eq!(payload["agent_id"], "main");
        assert_eq!(payload["session"], "session-1");
        let reply = payload["reply"]
            .as_str()
            .expect("reply should be a string response");
        assert!(reply.contains("OpenOrchestrator commands:"));
        assert!(reply.contains("/help"));
        assert!(
            !payload["run_id"]
                .as_str()
                .expect("run_id should exist")
                .is_empty()
        );

        let _ = fs::remove_dir_all(temp_dir).await;
    }

    #[test]
    fn slack_mode_validation_enforces_mode_specific_tokens() {
        let mut socket_runtime = default_test_slack(true);
        socket_runtime.mode = SlackMode::Socket;
        socket_runtime.bot_token = Some("xoxb-test".to_string());
        socket_runtime.app_token = None;
        socket_runtime.signing_secret = None;
        assert!(validate_slack_runtime(&socket_runtime).is_err());

        socket_runtime.app_token = Some("xapp-test".to_string());
        assert!(validate_slack_runtime(&socket_runtime).is_ok());

        let mut events_runtime = default_test_slack(true);
        events_runtime.mode = SlackMode::EventsApi;
        events_runtime.bot_token = Some("xoxb-test".to_string());
        events_runtime.signing_secret = None;
        assert!(validate_slack_runtime(&events_runtime).is_err());
    }

    #[test]
    fn verify_slack_signature_accepts_valid_request() {
        let secret = "topsecret";
        let body = br#"{"type":"event_callback"}"#;
        let timestamp = Utc::now().timestamp().to_string();
        let headers = build_signed_slack_headers(secret, &timestamp, body);

        assert!(verify_slack_signature(secret, &headers, body));
    }

    #[test]
    fn verify_slack_signature_rejects_invalid_cases() {
        let secret = "topsecret";
        let body = br#"{"type":"event_callback"}"#;
        let timestamp = Utc::now().timestamp().to_string();
        let valid_headers = build_signed_slack_headers(secret, &timestamp, body);

        assert!(!verify_slack_signature(
            secret,
            &valid_headers,
            br#"{"type":"url_verification"}"#
        ));

        let stale_timestamp = (Utc::now().timestamp() - 301).to_string();
        let stale_headers = build_signed_slack_headers(secret, &stale_timestamp, body);
        assert!(!verify_slack_signature(secret, &stale_headers, body));

        let mut malformed_headers = HeaderMap::new();
        malformed_headers.insert("x-slack-signature", HeaderValue::from_static("v0=1234"));
        malformed_headers.insert(
            "x-slack-request-timestamp",
            HeaderValue::from_static("not-a-number"),
        );
        assert!(!verify_slack_signature(secret, &malformed_headers, body));
    }

    #[test]
    fn extract_slack_attachments_parses_and_deduplicates_files() {
        let event = json!({
            "files": [
                {
                    "id": "F01",
                    "name": "posting-hours.csv",
                    "url_private_download": "https://files.example/F01",
                    "size": 42
                },
                {
                    "id": "F01",
                    "name": "posting-hours-duplicate.csv",
                    "url_private_download": "https://files.example/F01-duplicate",
                    "size": 42
                },
                {
                    "id": "F02",
                    "url_private": "https://files.example/F02"
                },
                {
                    "id": "F03",
                    "name": "missing-url"
                }
            ]
        });

        let attachments = extract_slack_attachments(&event);
        assert_eq!(attachments.len(), 2);
        assert_eq!(attachments[0].name, "posting-hours.csv");
        assert_eq!(attachments[0].download_url, "https://files.example/F01");
        assert_eq!(attachments[1].name, "attachment.bin");
        assert_eq!(attachments[1].download_url, "https://files.example/F02");
    }

    #[test]
    fn sanitize_path_component_and_file_name_are_safe() {
        assert_eq!(
            sanitize_path_component("../unsafe file", "fallback"),
            "unsafe-file"
        );
        assert_eq!(sanitize_path_component("!!!", "fallback"), "fallback");
        assert_eq!(sanitize_file_name(".."), "attachment.bin");
    }

    #[test]
    fn extract_slack_output_paths_uses_outbox_only_and_deduplicates() {
        let reply = r#"delegate:openclawd_1
`posting-hours` run completed on the attached file.

- `output`: `/Users/keszeyd/work/.openorchestrator/outbox/report_1.xlsx`
- `log`: `/Users/keszeyd/work/.openorchestrator/logs/report_1.log`
- duplicate: `/Users/keszeyd/work/.openorchestrator/outbox/report_1.xlsx`
"#;

        let paths = extract_slack_output_paths(reply);
        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0],
            PathBuf::from("/Users/keszeyd/work/.openorchestrator/outbox/report_1.xlsx")
        );
    }

    #[test]
    fn extract_slack_output_paths_trims_wrapping_punctuation() {
        let reply = "output=/Users/keszeyd/work/.openorchestrator/outbox/report_2.xlsx,";
        let paths = extract_slack_output_paths(reply);
        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0],
            PathBuf::from("/Users/keszeyd/work/.openorchestrator/outbox/report_2.xlsx")
        );
    }

    #[test]
    fn extract_slack_output_paths_supports_relative_outbox_paths() {
        let reply = "artifact=`.openorchestrator/outbox/report_3.xlsx`";
        let paths = extract_slack_output_paths(reply);
        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0],
            PathBuf::from(".openorchestrator/outbox/report_3.xlsx")
        );
    }

    #[test]
    fn candidate_output_upload_paths_includes_workspace_joins_for_relative_paths() {
        let relative = Path::new(".openorchestrator/outbox/report_4.xlsx");
        let workspaces = vec![
            PathBuf::from("/tmp/workspace-a"),
            PathBuf::from("/tmp/workspace-b"),
        ];
        let candidates = candidate_output_upload_paths(relative, &workspaces);

        assert!(candidates.contains(&PathBuf::from(
            "/tmp/workspace-a/.openorchestrator/outbox/report_4.xlsx"
        )));
        assert!(candidates.contains(&PathBuf::from(
            "/tmp/workspace-b/.openorchestrator/outbox/report_4.xlsx"
        )));
    }

    fn build_signed_slack_headers(secret: &str, timestamp: &str, body: &[u8]) -> HeaderMap {
        let payload = format!("v0:{}:{}", timestamp, String::from_utf8_lossy(body));
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("failed initializing hmac");
        mac.update(payload.as_bytes());
        let digest = mac.finalize().into_bytes();
        let signature = format!("v0={}", hex::encode(digest));

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-slack-signature",
            HeaderValue::from_str(&signature).expect("failed building signature header"),
        );
        headers.insert(
            "x-slack-request-timestamp",
            HeaderValue::from_str(timestamp).expect("failed building timestamp header"),
        );
        headers
    }
}
