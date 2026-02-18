use crate::codex::CodexRunner;
use crate::config::{AgentConfig, AppConfig, resolve_path, save_config};
use crate::state::{ConversationItem, StateStore, TaskItem, TaskStatus};
use anyhow::{Context, Result, bail};
use chrono::Utc;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

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

        let (agent, workspace, model) = self.resolve_agent_context(input.agent_id.as_deref()).await?;

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
        if let Some(session) = input.session.as_ref().map(|value| value.trim()).filter(|value| !value.is_empty()) {
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

    async fn resolve_agent_context(&self, requested_agent_id: Option<&str>) -> Result<(AgentConfig, PathBuf, Option<String>)> {
        let cfg = self.config.read().await;
        let agent_id = requested_agent_id.unwrap_or("main").trim();

        let agent = cfg
            .agents
            .iter()
            .find(|candidate| candidate.id == agent_id)
            .cloned()
            .or_else(|| cfg.agents.first().cloned())
            .context("at least one agent must be configured")?;

        let workspace_str = agent
            .workspace
            .clone()
            .unwrap_or_else(|| cfg.brain.workspace.clone());
        let workspace = resolve_path(&workspace_str);

        Ok((agent, workspace, cfg.brain.model.clone()))
    }

    async fn handle_command(&self, current_agent: &AgentConfig, session: &str, raw: &str) -> Result<String> {
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
            return Ok(format!("task#{} added for {}: {}", task.id, task.owner_agent, task.title));
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
            "/spawn <agent_id> | <soul prompt>",
            "/tasks",
            "/task add <title>",
            "/task done <task_id>",
            "/remember <note>",
            "/mem <query>",
            "/delegate <agent_id> <task>",
        ]
        .join("\n")
    }

    async fn spawn_agent(&self, rest: &str) -> Result<String> {
        let (left, right) = rest
            .split_once('|')
            .context("usage: /spawn <agent_id> | <soul prompt>")?;
        let id = left.trim();
        let soul = right.trim();

        if id.is_empty() || soul.is_empty() {
            bail!("usage: /spawn <agent_id> | <soul prompt>");
        }
        if !is_valid_agent_id(id) {
            bail!("agent_id must be lowercase letters, numbers, '-' or '_'");
        }

        let snapshot = {
            let mut cfg = self.config.write().await;
            if let Some(existing) = cfg.agents.iter_mut().find(|agent| agent.id == id) {
                existing.soul = soul.to_string();
                existing.name = id.to_string();
            } else {
                cfg.agents.push(AgentConfig {
                    id: id.to_string(),
                    name: id.to_string(),
                    soul: soul.to_string(),
                    workspace: None,
                });
            }
            cfg.clone()
        };

        save_config(&self.config_path, &snapshot).await?;
        Ok(format!("agent '{}' is ready", id))
    }

    async fn delegate_command(&self, rest: &str, session: &str) -> Result<String> {
        let mut parts = rest.trim().splitn(2, ' ');
        let agent_id = parts.next().unwrap_or("").trim();
        let task = parts.next().unwrap_or("").trim();

        if agent_id.is_empty() || task.is_empty() {
            bail!("usage: /delegate <agent_id> <task>");
        }

        let (agent, workspace, model) = self.resolve_agent_context(Some(agent_id)).await?;
        let response = self
            .run_agent_turn(&agent, &workspace, model.as_deref(), session, task)
            .await?;

        Ok(format!("delegate:{}\n{}", agent.id, response))
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
                error!(agent_id = %agent.id, error = %err, "codex run error");
                Ok(format!("[openorchestrator error] {}", err))
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
                format!("{} task#{} {} ({})", marker, task.id, task.title, task.owner_agent)
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
    let history_text = if history.is_empty() {
        "(none)".to_string()
    } else {
        history
            .iter()
            .map(|item| format!("{}: {}", item.role, item.text))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let memory_text = if long_tail.is_empty() {
        "(none)".to_string()
    } else {
        long_tail.join("\n")
    };

    let tasks_text = if open_tasks.is_empty() {
        "(none)".to_string()
    } else {
        open_tasks
            .iter()
            .take(20)
            .map(|task| format!("task#{} [{}] {} ({})", task.id, task.owner_agent, task.title, format_status(&task.status)))
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
