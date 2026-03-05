use crate::codex::CodexRunner;
use crate::config::{
    AgentConfig, AppConfig, normalize_workspace_binding, resolve_path, save_config,
};
use crate::state::{ConversationItem, StateStore, TaskItem, TaskStatus};
use anyhow::{Context, Result, bail};
use chrono::Utc;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

const MAX_PROMPT_USER_CHARS: usize = 4_000;
const MAX_PROMPT_HISTORY_ITEM_CHARS: usize = 1_000;
const MAX_PROMPT_HISTORY_TOTAL_CHARS: usize = 8_000;
const MAX_PROMPT_MEMORY_ITEM_CHARS: usize = 700;
const MAX_PROMPT_MEMORY_TOTAL_CHARS: usize = 4_000;

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub source: String,
    pub user: Option<String>,
    pub session: Option<String>,
    pub agent_id: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct AgentReply {
    pub run_id: String,
    pub agent_id: String,
    pub session: String,
    pub reply: String,
}

#[derive(Clone)]
pub struct Orchestrator {
    config_path: PathBuf,
    config: Arc<RwLock<AppConfig>>,
    state: StateStore,
    codex: CodexRunner,
}

impl Orchestrator {
    pub fn new(
        config_path: PathBuf,
        config: Arc<RwLock<AppConfig>>,
        state: StateStore,
        codex: CodexRunner,
    ) -> Self {
        Self {
            config_path,
            config,
            state,
            codex,
        }
    }

    pub fn config_handle(&self) -> Arc<RwLock<AppConfig>> {
        self.config.clone()
    }

    pub async fn state(&self) -> StateStore {
        self.state.clone()
    }

    pub async fn handle_message(&self, input: IncomingMessage) -> Result<AgentReply> {
        let session = self.resolve_session(&input);
        let run_id = Uuid::new_v4().to_string();

        let (agent, workspace, model) = self
            .resolve_agent_context(input.agent_id.as_deref())
            .await?;

        self.state
            .add_conversation(ConversationItem {
                id: 0,
                ts: Utc::now(),
                session: session.clone(),
                agent_id: agent.id.clone(),
                role: "user".to_string(),
                text: input.text.clone(),
                source: input.source.clone(),
                user: input.user.clone(),
            })
            .await?;

        let reply = if input.text.trim_start().starts_with('/') {
            self.handle_command(&agent, &session, &input.text).await?
        } else if let Some(reply) = self.try_inline_spawn(&input.text).await? {
            reply
        } else if let Some(reply) = self.try_inline_delegate(&session, &input.text).await? {
            reply
        } else {
            self.run_agent_turn(&agent, &workspace, model.as_deref(), &session, &input.text)
                .await?
        };

        self.state
            .add_conversation(ConversationItem {
                id: 0,
                ts: Utc::now(),
                session: session.clone(),
                agent_id: agent.id.clone(),
                role: "assistant".to_string(),
                text: reply.clone(),
                source: input.source,
                user: None,
            })
            .await?;

        Ok(AgentReply {
            run_id,
            agent_id: agent.id,
            session,
            reply,
        })
    }

    fn resolve_session(&self, input: &IncomingMessage) -> String {
        if let Some(session) = input
            .session
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            return session.to_string();
        }
        let user = input
            .user
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("anonymous");
        format!("{}:{}", input.source, user)
    }

    async fn resolve_agent_context(
        &self,
        requested_agent_id: Option<&str>,
    ) -> Result<(AgentConfig, PathBuf, Option<String>)> {
        let cfg = self.config.read().await;
        let requested = requested_agent_id.map(normalize_agent_id);

        let agent = if let Some(agent_id) = requested {
            if agent_id.is_empty() {
                cfg.agents
                    .first()
                    .cloned()
                    .context("at least one agent must be configured")?
            } else {
                cfg.agents
                    .iter()
                    .find(|candidate| candidate.id.eq_ignore_ascii_case(&agent_id))
                    .cloned()
                    .with_context(|| format!("agent '{}' not found. use /agents", agent_id))?
            }
        } else {
            cfg.agents
                .first()
                .cloned()
                .context("at least one agent must be configured")?
        };

        let workspace_str = agent
            .workspace
            .clone()
            .unwrap_or_else(|| cfg.brain.workspace.clone());
        let normalized_workspace =
            normalize_workspace_binding(&workspace_str, &cfg.brain.workspace);
        let workspace = resolve_path(&normalized_workspace);

        Ok((agent, workspace, cfg.brain.model.clone()))
    }

    async fn handle_command(
        &self,
        current_agent: &AgentConfig,
        session: &str,
        raw: &str,
    ) -> Result<String> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("/help") {
            return Ok(self.help_text());
        }

        if trimmed.eq_ignore_ascii_case("/agents") {
            let cfg = self.config.read().await;
            let lines = cfg
                .agents
                .iter()
                .map(|agent| format!("- {}: {}", agent.id, agent.name))
                .collect::<Vec<_>>();
            return Ok(format!("Agents:\n{}", lines.join("\n")));
        }

        if let Some(rest) = trimmed.strip_prefix("/spawn ") {
            return self.spawn_agent(rest).await;
        }

        if trimmed.eq_ignore_ascii_case("/tasks") {
            return Ok(self.render_tasks().await);
        }

        if let Some(rest) = trimmed.strip_prefix("/task add ") {
            let title = rest.trim();
            if title.is_empty() {
                bail!("usage: /task add <title>");
            }
            let task = self.state.add_task(title, &current_agent.id).await?;
            return Ok(format!(
                "task#{} added for {}: {}",
                task.id, task.owner_agent, task.title
            ));
        }

        if let Some(rest) = trimmed.strip_prefix("/task done ") {
            let id = rest
                .trim()
                .parse::<u64>()
                .context("usage: /task done <task_id>")?;
            let updated = self.state.mark_task_done(id).await?;
            if updated {
                return Ok(format!("task#{} marked done", id));
            }
            return Ok(format!("task#{} not found", id));
        }

        if let Some(rest) = trimmed.strip_prefix("/remember ") {
            let note = rest.trim();
            if note.is_empty() {
                bail!("usage: /remember <note>");
            }
            let memory = self
                .state
                .add_memory(format!("{}:{}", current_agent.id, session), note)
                .await?;
            return Ok(format!("saved memory#{}", memory.id));
        }

        if let Some(rest) = trimmed.strip_prefix("/mem ") {
            let query = rest.trim();
            if query.is_empty() {
                bail!("usage: /mem <query>");
            }
            let hits = self.state.search_long_tail(query, 6).await;
            if hits.is_empty() {
                return Ok("no memory hits".to_string());
            }
            return Ok(format!("memory hits:\n{}", hits.join("\n")));
        }

        if let Some(rest) = trimmed.strip_prefix("/delegate ") {
            return self.delegate_command(rest, session).await;
        }

        Ok("unknown command. use /help".to_string())
    }

    fn help_text(&self) -> String {
        [
            "OpenOrchestrator commands:",
            "/help",
            "/agents",
            "/spawn <agent_id> | <soul prompt> | <workspace_path?>",
            "/tasks",
            "/task add <title>",
            "/task done <task_id>",
            "/remember <note>",
            "/mem <query>",
            "/delegate <agent_id> <task>",
            "",
            "Natural delegation:",
            "<agent_id> run <task>",
        ]
        .join("\n")
    }

    async fn spawn_agent(&self, rest: &str) -> Result<String> {
        let parts = rest.split('|').map(str::trim).collect::<Vec<_>>();
        if parts.len() < 2 || parts.len() > 3 {
            bail!("usage: /spawn <agent_id> | <soul prompt> | <workspace_path?>");
        }

        let id = normalize_agent_id(parts[0]);
        let soul = parts[1].trim();
        let workspace = parts
            .get(2)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);

        if id.is_empty() || soul.is_empty() {
            bail!("usage: /spawn <agent_id> | <soul prompt> | <workspace_path?>");
        }
        if !is_valid_agent_id(&id) {
            bail!("agent_id must be lowercase letters, numbers, '-' or '_'");
        }

        self.upsert_agent_config(&id, soul, workspace).await
    }

    async fn upsert_agent_config(
        &self,
        id: &str,
        soul: &str,
        workspace: Option<String>,
    ) -> Result<String> {
        let normalized_id = normalize_agent_id(id);
        if normalized_id.is_empty() || soul.trim().is_empty() {
            bail!("usage: /spawn <agent_id> | <soul prompt> | <workspace_path?>");
        }
        if !is_valid_agent_id(&normalized_id) {
            bail!("agent_id must be lowercase letters, numbers, '-' or '_'");
        }

        let snapshot = {
            let mut cfg = self.config.write().await;
            let normalized_workspace = workspace
                .as_deref()
                .map(|value| normalize_workspace_binding(value, &cfg.brain.workspace));
            if let Some(existing) = cfg
                .agents
                .iter_mut()
                .find(|agent| agent.id.eq_ignore_ascii_case(&normalized_id))
            {
                existing.soul = soul.to_string();
                existing.name = normalized_id.clone();
                if workspace.is_some() {
                    existing.workspace = normalized_workspace.clone();
                }
            } else {
                cfg.agents.push(AgentConfig {
                    id: normalized_id.clone(),
                    name: normalized_id.clone(),
                    soul: soul.to_string(),
                    workspace: normalized_workspace.clone(),
                });
            }
            cfg.clone()
        };

        save_config(&self.config_path, &snapshot).await?;
        let workspace_text = snapshot
            .agents
            .iter()
            .find(|candidate| candidate.id.eq_ignore_ascii_case(&normalized_id))
            .and_then(|candidate| candidate.workspace.as_ref())
            .cloned()
            .unwrap_or_else(|| "inherits brain.workspace".to_string());
        Ok(format!(
            "agent '{}' is ready (workspace: {})",
            normalized_id, workspace_text
        ))
    }

    async fn delegate_command(&self, rest: &str, session: &str) -> Result<String> {
        let mut parts = rest.trim().splitn(2, ' ');
        let agent_id = normalize_agent_id(parts.next().unwrap_or(""));
        let task = parts.next().unwrap_or("").trim();

        if agent_id.is_empty() || task.is_empty() {
            bail!("usage: /delegate <agent_id> <task>");
        }

        let (agent, workspace, model) = self.resolve_agent_context(Some(&agent_id)).await?;
        let response = self
            .run_agent_turn(&agent, &workspace, model.as_deref(), session, task)
            .await?;

        Ok(format!("delegate:{}\n{}", agent.id, response))
    }

    async fn try_inline_delegate(&self, session: &str, text: &str) -> Result<Option<String>> {
        let Some(parsed) = parse_inline_delegate_request(text) else {
            return Ok(None);
        };

        if !self.agent_exists(&parsed.agent_id).await {
            return Ok(None);
        }

        let delegate_input = format!("{} {}", parsed.agent_id, parsed.task);
        let reply = self.delegate_command(&delegate_input, session).await?;
        Ok(Some(reply))
    }

    async fn try_inline_spawn(&self, text: &str) -> Result<Option<String>> {
        let Some(parsed) = parse_inline_spawn_request(text) else {
            return Ok(None);
        };

        let brain_workspace = self.config.read().await.brain.workspace.clone();
        let workspace = normalize_workspace_binding(&parsed.workspace, &brain_workspace);
        let soul = build_workspace_agent_soul(&workspace);
        let ready = self
            .upsert_agent_config(&parsed.agent_id, &soul, Some(workspace))
            .await?;
        let next = format!("next: {} run <task>", parsed.agent_id);
        Ok(Some(format!("{}\n{}", ready, next)))
    }

    async fn agent_exists(&self, requested_agent_id: &str) -> bool {
        let cfg = self.config.read().await;
        cfg.agents
            .iter()
            .any(|agent| agent.id.eq_ignore_ascii_case(requested_agent_id))
    }

    async fn run_agent_turn(
        &self,
        agent: &AgentConfig,
        workspace: &PathBuf,
        model: Option<&str>,
        session: &str,
        user_text: &str,
    ) -> Result<String> {
        let history = self.state.recent_session(session, 12).await;
        let long_tail = self.state.search_long_tail(user_text, 8).await;
        let open_tasks = self.state.list_open_tasks().await;
        let prompt = build_prompt(agent, user_text, &history, &long_tail, &open_tasks);

        info!(agent_id = %agent.id, session = %session, "running codex turn");
        let response = self
            .codex
            .run_prompt(workspace, &prompt, model)
            .await
            .with_context(|| format!("codex run failed for agent '{}'", agent.id));

        match response {
            Ok(answer) => Ok(answer),
            Err(err) => {
                let error_chain = format!("{err:#}");
                error!(
                    agent_id = %agent.id,
                    session = %session,
                    error = %error_chain,
                    "codex run error"
                );
                Ok(format!("[openorchestrator error] {}", error_chain))
            }
        }
    }

    async fn render_tasks(&self) -> String {
        let tasks = self.state.list_tasks().await;
        if tasks.is_empty() {
            return "no tasks".to_string();
        }
        let lines = tasks
            .iter()
            .map(|task| {
                let marker = match task.status {
                    TaskStatus::Open => "[open]",
                    TaskStatus::Done => "[done]",
                };
                format!(
                    "{} task#{} {} ({})",
                    marker, task.id, task.title, task.owner_agent
                )
            })
            .collect::<Vec<_>>();
        lines.join("\n")
    }
}

fn build_prompt(
    agent: &AgentConfig,
    user_text: &str,
    history: &[ConversationItem],
    long_tail: &[String],
    open_tasks: &[TaskItem],
) -> String {
    let user_text = truncate_for_prompt(user_text, MAX_PROMPT_USER_CHARS);
    let history_text = if history.is_empty() {
        "(none)".to_string()
    } else {
        let joined = history
            .iter()
            .map(|item| {
                format!(
                    "{}: {}",
                    item.role,
                    truncate_for_prompt(&item.text, MAX_PROMPT_HISTORY_ITEM_CHARS)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        truncate_for_prompt(&joined, MAX_PROMPT_HISTORY_TOTAL_CHARS)
    };

    let memory_text = if long_tail.is_empty() {
        "(none)".to_string()
    } else {
        let joined = long_tail
            .iter()
            .map(|value| truncate_for_prompt(value, MAX_PROMPT_MEMORY_ITEM_CHARS))
            .collect::<Vec<_>>()
            .join("\n");
        truncate_for_prompt(&joined, MAX_PROMPT_MEMORY_TOTAL_CHARS)
    };

    let tasks_text = if open_tasks.is_empty() {
        "(none)".to_string()
    } else {
        open_tasks
            .iter()
            .take(20)
            .map(|task| {
                format!(
                    "task#{} [{}] {} ({})",
                    task.id,
                    task.owner_agent,
                    task.title,
                    format_status(&task.status)
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "You are OpenOrchestrator agent '{agent_id}'.\n\
Soul:\n{soul}\n\
\nOperating rules:\n\
- Be concise and operational.\n\
- Prefer concrete next actions.\n\
- If user asks for automation, include a clear checklist.\n\
- When stopping processes, avoid broad kill patterns; identify exact target PIDs and exclude your current shell process.\n\
\nLong-tail memory matches:\n{memory}\n\
\nOpen tasks:\n{tasks}\n\
\nRecent session history:\n{history}\n\
\nUser message:\n{message}",
        agent_id = agent.id,
        soul = agent.soul,
        memory = memory_text,
        tasks = tasks_text,
        history = history_text,
        message = user_text,
    )
}

fn truncate_for_prompt(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let mut chars = value.chars();
    let kept = chars.by_ref().take(max_chars).collect::<String>();
    let remaining = chars.count();
    if remaining == 0 {
        value.to_string()
    } else {
        format!("{kept}\n...[truncated {remaining} chars]")
    }
}

fn format_status(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Open => "open",
        TaskStatus::Done => "done",
    }
}

fn is_valid_agent_id(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_')
}

fn normalize_agent_id(value: &str) -> String {
    value
        .trim()
        .trim_matches('`')
        .trim_start_matches('@')
        .trim_end_matches(|ch: char| ch == ':' || ch == ',' || ch == ';')
        .to_ascii_lowercase()
}

fn parse_inline_delegate_request(raw: &str) -> Option<InlineDelegateRequest> {
    let trimmed = raw.trim();
    let first_space = trimmed.find(char::is_whitespace)?;
    let (candidate_agent_id, remaining) = trimmed.split_at(first_space);
    let agent_id = normalize_agent_id(candidate_agent_id);
    if !is_valid_agent_id(&agent_id) {
        return None;
    }

    let remaining = remaining.trim_start();
    let remaining_lower = remaining.to_ascii_lowercase();
    if !remaining_lower.starts_with("run ") {
        return None;
    }

    let task = remaining[3..].trim();
    if task.is_empty() {
        return None;
    }

    Some(InlineDelegateRequest {
        agent_id,
        task: task.to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InlineDelegateRequest {
    agent_id: String,
    task: String,
}

fn parse_inline_spawn_request(raw: &str) -> Option<InlineSpawnRequest> {
    let trimmed = raw.trim();
    let first_space = trimmed.find(char::is_whitespace)?;
    let (candidate_agent_id, remaining) = trimmed.split_at(first_space);
    if !is_explicit_inline_spawn_target(candidate_agent_id) {
        return None;
    }
    let agent_id = normalize_agent_id(candidate_agent_id);
    if !is_valid_agent_id(&agent_id) {
        return None;
    }

    let remaining = remaining.trim_start();
    if remaining.is_empty() {
        return None;
    }

    let remaining_lower = remaining.to_ascii_lowercase();
    let has_spawn_intent = remaining_lower.contains("spawn")
        && (remaining_lower.contains("new agent")
            || remaining_lower.contains("an agent")
            || remaining_lower.contains("sub-agent")
            || remaining_lower.contains("sub agent"));
    if !has_spawn_intent {
        return None;
    }

    let workspace = extract_workspace_path(remaining)?;
    Some(InlineSpawnRequest {
        agent_id,
        workspace,
    })
}

fn is_explicit_inline_spawn_target(raw_candidate: &str) -> bool {
    let trimmed = raw_candidate.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.starts_with('@') {
        return true;
    }

    let normalized = normalize_agent_id(trimmed);
    normalized
        .chars()
        .any(|ch| ch == '_' || ch == '-' || ch.is_ascii_digit())
}

fn extract_workspace_path(raw: &str) -> Option<String> {
    raw.split_whitespace().find_map(|token| {
        let cleaned = token
            .trim_matches(|ch: char| {
                ch == '"'
                    || ch == '\''
                    || ch == '`'
                    || ch == '('
                    || ch == ')'
                    || ch == '['
                    || ch == ']'
            })
            .trim_end_matches(|ch: char| {
                ch == ','
                    || ch == ';'
                    || ch == '.'
                    || ch == ':'
                    || ch == ')'
                    || ch == ']'
                    || ch == '}'
            })
            .trim();

        if cleaned.is_empty() {
            return None;
        }

        let is_path_like = cleaned.starts_with('/')
            || cleaned.starts_with("~/")
            || cleaned.starts_with("./")
            || cleaned.starts_with("../");
        if !is_path_like || cleaned == "/" {
            return None;
        }

        Some(cleaned.to_string())
    })
}

fn build_workspace_agent_soul(workspace: &str) -> String {
    format!(
        "You are a specialized automation sub-agent. Your workspace is {}. Analyze scripts in this workspace, execute delegated runs, and return concise results with concrete next actions.",
        workspace
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InlineSpawnRequest {
    agent_id: String,
    workspace: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, resolve_path};
    use std::sync::Arc;
    use tokio::fs;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    #[test]
    fn parse_inline_delegate_request_accepts_agent_run_format() {
        let parsed =
            parse_inline_delegate_request("OpenClawd_1 run posting-hours with attached file")
                .expect("expected inline delegate request");
        assert_eq!(parsed.agent_id, "openclawd_1");
        assert_eq!(parsed.task, "posting-hours with attached file");
    }

    #[test]
    fn parse_inline_delegate_request_handles_agent_punctuation() {
        let parsed = parse_inline_delegate_request("openclawd_1: RUN posting-hours")
            .expect("expected inline delegate request");
        assert_eq!(parsed.agent_id, "openclawd_1");
        assert_eq!(parsed.task, "posting-hours");
    }

    #[test]
    fn parse_inline_delegate_request_rejects_non_delegate_messages() {
        assert!(parse_inline_delegate_request("openclawd_1 run").is_none());
        assert!(parse_inline_delegate_request("openclawd_1 build posting-hours").is_none());
    }

    #[test]
    fn parse_inline_spawn_request_accepts_spawn_with_workspace() {
        let parsed = parse_inline_spawn_request(
            "@OpenClawd_1 I would like you to spawn up a new agent, which analyzes the scripts in /Users/keszeyd/.openclaw/workspace/automation/secrella",
        )
        .expect("expected inline spawn request");
        assert_eq!(parsed.agent_id, "openclawd_1");
        assert_eq!(
            parsed.workspace,
            "/Users/keszeyd/.openclaw/workspace/automation/secrella"
        );
    }

    #[test]
    fn parse_inline_spawn_request_rejects_missing_workspace() {
        assert!(
            parse_inline_spawn_request("openclawd_1 please spawn a new agent for automation")
                .is_none()
        );
    }

    #[test]
    fn parse_inline_spawn_request_rejects_generic_polite_prompt() {
        assert!(
            parse_inline_spawn_request(
                "please spawn a new agent that analyzes scripts in /Users/keszeyd/work/automation/secrella"
            )
            .is_none()
        );
    }

    #[test]
    fn extract_workspace_path_strips_punctuation() {
        let path = extract_workspace_path(
            "analyzes scripts in (/Users/keszeyd/.openclaw/workspace/automation/secrella).",
        )
        .expect("workspace path should be extracted");
        assert_eq!(
            path,
            "/Users/keszeyd/.openclaw/workspace/automation/secrella"
        );
    }

    #[test]
    fn normalize_agent_id_accepts_uppercase_input() {
        assert_eq!(normalize_agent_id("@OpenClawd_1,"), "openclawd_1");
    }

    #[tokio::test]
    async fn handle_message_inline_spawn_creates_agent_workspace_binding() {
        let base_dir =
            std::env::temp_dir().join(format!("openorchestrator-inline-spawn-{}", Uuid::new_v4()));
        fs::create_dir_all(&base_dir)
            .await
            .expect("failed creating test temp dir");

        let config_path = base_dir.join("openorchestrator.json");
        let state_path = base_dir.join("state.json");
        let cfg = AppConfig::default();
        let expected_workspace = resolve_path(&cfg.brain.workspace)
            .join("automation")
            .join("secrella")
            .to_string_lossy()
            .to_string();
        save_config(&config_path, &cfg)
            .await
            .expect("failed writing test config");

        let state = StateStore::load(&state_path, 200, 200)
            .await
            .expect("failed loading test state");
        let orchestrator = Orchestrator::new(
            config_path.clone(),
            Arc::new(RwLock::new(cfg)),
            state,
            CodexRunner::default(),
        );

        let message = "@OpenClawd_1 I would like you to spawn up a new agent, which analyzes the scripts in /Users/keszeyd/.openclaw/workspace/automation/secrella";
        let reply = orchestrator
            .handle_message(IncomingMessage {
                source: "local-test".to_string(),
                user: Some("tester".to_string()),
                session: Some("session-1".to_string()),
                agent_id: None,
                text: message.to_string(),
            })
            .await
            .expect("inline spawn request should succeed");

        assert!(reply.reply.contains("agent 'openclawd_1' is ready"));
        assert!(reply.reply.contains("next: openclawd_1 run <task>"));

        let persisted = crate::config::load_config(&config_path)
            .await
            .expect("failed loading persisted config");
        let spawned = persisted
            .agents
            .iter()
            .find(|agent| agent.id == "openclawd_1")
            .expect("expected openclawd_1 agent in config");
        assert_eq!(
            spawned.workspace.as_deref(),
            Some(expected_workspace.as_str())
        );
        assert!(spawned.soul.contains(&expected_workspace));

        let _ = fs::remove_dir_all(base_dir).await;
    }
}
