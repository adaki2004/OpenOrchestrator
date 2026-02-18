use crate::config::SlackMode;
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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

type HmacSha256 = Hmac<Sha256>;
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;
const MAX_CHAT_TEXT_CHARS: usize = 16_000;
const MAX_CHAT_FIELD_CHARS: usize = 128;

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

    let cleaned_text = clean_slack_mention(&state.slack, &text_raw)
        .trim()
        .to_string();
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
