use crate::orchestrator::{IncomingMessage, Orchestrator};
use crate::state::TaskItem;
use anyhow::{Context, Result};
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

type HmacSha256 = Hmac<Sha256>;

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
    bot_token: Option<String>,
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
        bot_token: cfg.slack.bot_token.clone(),
        signing_secret: cfg.slack.signing_secret.clone(),
        bot_user_id: cfg.slack.bot_user_id.clone(),
        default_channel: cfg.slack.default_channel.clone(),
    };

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

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/chat", post(chat))
        .route("/api/tasks", get(tasks))
        .route("/slack/events", post(slack_events))
        .with_state(state);

    let listener = TcpListener::bind((host.as_str(), port))
        .await
        .with_context(|| format!("failed binding gateway at {}:{}", host, port))?;

    info!("OpenOrchestrator gateway listening on http://{}:{}", host, port);
    axum::serve(listener, app)
        .await
        .context("gateway server failed")?;

    Ok(())
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

async fn chat(State(state): State<GatewayState>, axum::Json(req): axum::Json<ChatRequest>) -> impl IntoResponse {
    if req.text.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "text must not be empty"})),
        )
            .into_response();
    }

    let incoming = IncomingMessage {
        source: req.source.unwrap_or_else(|| "api".to_string()),
        user: req.user,
        session: req.session,
        agent_id: req.agent_id,
        text: req.text,
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
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": err.to_string()})),
        )
            .into_response(),
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

    if let Some(secret) = state.slack.signing_secret.as_deref() {
        if !verify_slack_signature(secret, &headers, &body) {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({"error": "invalid slack signature"})),
            )
                .into_response();
        }
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

    let cleaned_text = clean_slack_mention(&state.slack, &text_raw).trim().to_string();
    if cleaned_text.is_empty() {
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

    let reply = state
        .orchestrator
        .handle_message(IncomingMessage {
            source: "slack".to_string(),
            user,
            session: Some(session),
            agent_id: None,
            text: cleaned_text,
        })
        .await?;

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
    }

    Ok(())
}

fn should_ignore_channel_message(slack: &SlackRuntime, channel: &str, event_kind: &str, text: &str) -> bool {
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

async fn send_slack_message(
    client: &Client,
    token: &str,
    channel: &str,
    text: &str,
    thread_ts: Option<&str>,
    default_channel: Option<&str>,
) -> Result<()> {
    let target_channel = if channel.trim().is_empty() {
        default_channel.unwrap_or("")
    } else {
        channel
    };

    if target_channel.trim().is_empty() {
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

    if let Ok(ts) = timestamp.parse::<i64>() {
        let age = (Utc::now().timestamp() - ts).abs();
        if age > 300 {
            return false;
        }
    }

    let signed_payload = format!("v0:{}:{}", timestamp, String::from_utf8_lossy(body));
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(signed_payload.as_bytes());
    let digest = mac.finalize().into_bytes();
    let expected = format!("v0={}", hex::encode(digest));

    expected == signature
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
