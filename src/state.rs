use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationItem {
    pub id: u64,
    pub ts: DateTime<Utc>,
    pub session: String,
    pub agent_id: String,
    pub role: String,
    pub text: String,
    pub source: String,
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryItem {
    pub id: u64,
    pub ts: DateTime<Utc>,
    pub source: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskItem {
    pub id: u64,
    pub ts: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub owner_agent: String,
    pub title: String,
    pub status: TaskStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Open,
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedState {
    pub next_id: u64,
    pub conversations: Vec<ConversationItem>,
    pub memories: Vec<MemoryItem>,
    pub tasks: Vec<TaskItem>,
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            next_id: 1,
            conversations: Vec::new(),
            memories: Vec::new(),
            tasks: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct StateStore {
    path: PathBuf,
    inner: Arc<RwLock<PersistedState>>,
    max_conversation_items: usize,
    max_memory_items: usize,
}

impl StateStore {
    pub async fn load(
        path: impl AsRef<Path>,
        max_conversation_items: usize,
        max_memory_items: usize,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = if path.exists() {
            let data = fs::read(&path)
                .await
                .with_context(|| format!("failed reading state {}", path.display()))?;
            serde_json::from_slice::<PersistedState>(&data)
                .with_context(|| format!("failed parsing state {}", path.display()))?
        } else {
            PersistedState::default()
        };

        Ok(Self {
            path,
            inner: Arc::new(RwLock::new(state)),
            max_conversation_items,
            max_memory_items,
        })
    }

    pub async fn save(&self) -> Result<()> {
        let snapshot = self.inner.read().await.clone();
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed creating state dir {}", parent.display()))?;
        }
        let data = serde_json::to_vec_pretty(&snapshot)?;
        fs::write(&self.path, data)
            .await
            .with_context(|| format!("failed writing state {}", self.path.display()))?;
        Ok(())
    }

    pub async fn add_conversation(&self, mut item: ConversationItem) -> Result<()> {
        {
            let mut guard = self.inner.write().await;
            item.id = guard.next_id;
            guard.next_id += 1;
            guard.conversations.push(item);
            if guard.conversations.len() > self.max_conversation_items {
                let drop_count = guard
                    .conversations
                    .len()
                    .saturating_sub(self.max_conversation_items);
                guard.conversations.drain(0..drop_count);
            }
        }
        self.save().await
    }

    pub async fn add_memory(
        &self,
        source: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<MemoryItem> {
        let memory = {
            let mut guard = self.inner.write().await;
            let item = MemoryItem {
                id: guard.next_id,
                ts: Utc::now(),
                source: source.into(),
                text: text.into(),
            };
            guard.next_id += 1;
            guard.memories.push(item.clone());
            if guard.memories.len() > self.max_memory_items {
                let drop_count = guard.memories.len().saturating_sub(self.max_memory_items);
                guard.memories.drain(0..drop_count);
            }
            item
        };
        self.save().await?;
        Ok(memory)
    }

    pub async fn recent_session(&self, session: &str, limit: usize) -> Vec<ConversationItem> {
        let guard = self.inner.read().await;
        guard
            .conversations
            .iter()
            .filter(|entry| entry.session == session)
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub async fn search_long_tail(&self, query: &str, limit: usize) -> Vec<String> {
        let q = query.to_lowercase();
        let terms: Vec<&str> = q
            .split_whitespace()
            .filter(|part| !part.is_empty())
            .collect();
        if terms.is_empty() {
            return Vec::new();
        }

        let guard = self.inner.read().await;
        let mut scored: Vec<(usize, String)> = Vec::new();

        for memory in &guard.memories {
            let candidate = memory.text.to_lowercase();
            let score = terms
                .iter()
                .filter(|term| candidate.contains(**term))
                .count();
            if score > 0 {
                scored.push((
                    score,
                    format!("memory#{} [{}] {}", memory.id, memory.source, memory.text),
                ));
            }
        }

        for convo in &guard.conversations {
            let candidate = convo.text.to_lowercase();
            let score = terms
                .iter()
                .filter(|term| candidate.contains(**term))
                .count();
            if score > 0 {
                scored.push((
                    score,
                    format!(
                        "conversation#{} [{}:{}:{}] {}",
                        convo.id, convo.session, convo.agent_id, convo.role, convo.text
                    ),
                ));
            }
        }

        scored.sort_by(|a, b| b.0.cmp(&a.0));
        scored
            .into_iter()
            .take(limit)
            .map(|(_, line)| line)
            .collect()
    }

    pub async fn add_task(
        &self,
        title: impl Into<String>,
        owner_agent: impl Into<String>,
    ) -> Result<TaskItem> {
        let task = {
            let mut guard = self.inner.write().await;
            let now = Utc::now();
            let item = TaskItem {
                id: guard.next_id,
                ts: now,
                updated_at: now,
                owner_agent: owner_agent.into(),
                title: title.into(),
                status: TaskStatus::Open,
            };
            guard.next_id += 1;
            guard.tasks.push(item.clone());
            item
        };
        self.save().await?;
        Ok(task)
    }

    pub async fn mark_task_done(&self, id: u64) -> Result<bool> {
        let mut updated = false;
        {
            let mut guard = self.inner.write().await;
            if let Some(task) = guard.tasks.iter_mut().find(|task| task.id == id) {
                task.status = TaskStatus::Done;
                task.updated_at = Utc::now();
                updated = true;
            }
        }
        if updated {
            self.save().await?;
        }
        Ok(updated)
    }

    pub async fn list_tasks(&self) -> Vec<TaskItem> {
        let guard = self.inner.read().await;
        guard.tasks.clone()
    }

    pub async fn list_open_tasks(&self) -> Vec<TaskItem> {
        let guard = self.inner.read().await;
        guard
            .tasks
            .iter()
            .filter(|task| task.status == TaskStatus::Open)
            .cloned()
            .collect()
    }
}
