# OpenOrchestrator (MVP)

## 1) Background and intent

OpenClaw proves a strong pattern:

- an always-on gateway process
- guided onboarding that writes config
- a terminal interface that feels like direct chat with the assistant
- pluggable channels (Slack included)

OpenOrchestrator recreates that pattern in a minimal Rust-first, Codex-first open-source implementation.

## 2) Vision

A 24/7 Codex-backed personal agent orchestrator that:

- has one primary brain
- supports spawnable sub-agents (who can talk to each other if needed) with distinct souls
- persists long-tail memory and tasks
- is reachable via gateway API, Slack, and terminal UI

## 3) MVP scope (implemented)

### In scope

- Rust CLI app
- Interactive onboarding command
- Config persistence to `~/.openorchestrator/openorchestrator.json`
- Gateway runtime (`openorchestrator gateway run`)
- Codex CLI as the only model backend
- Slack integration (Events API webhook + outbound replies)
- Long-tail memory and task persistence in local state file
- Sub-agent spawning and delegation commands
- Terminal UI (`openorchestrator tui`) attached to gateway API

### Out of scope (deferred)

- Multi-provider model onboarding (Anthropic, OpenAI API keys, etc.)
- Rich full-screen TUI widgets/parsers
- Native Slack Socket Mode runtime
- Clustered distributed orchestration
- NPM package installer + service manager installers

## 4) Target users

- Builders who already use Codex CLI and want always-on orchestration.
- Operators automating recurring work streams (reporting, reminders, task triage, lightweight ops).

## 5) Core user stories

- As an operator, I can run onboarding once and have a working config.
- As an operator, I can run one daemon/gateway 24/7 on a VPS.
- As an operator, I can talk to the brain from Slack or terminal.
- As an operator, I can spawn sub-agents with explicit souls.
- As an operator, I can persist tasks and memory between runs.
- As an operator, I can delegate a task to a specific sub-agent.

## 6) Functional requirements

### FR-1 Onboarding

- Command: `openorchestrator onboard`
- Must check Codex login status (`codex login status`)
- Must gather Slack settings
- Must write/update config file

Acceptance:

- Config file exists and is valid JSON after onboarding.

### FR-2 Config mutability

- Command: `openorchestrator config show`
- Command: `openorchestrator config set <path> <value>`

Acceptance:

- User can modify persisted keys without manual file editing.

### FR-3 Gateway runtime

- Command: `openorchestrator gateway run`
- Exposes health route and chat API
- Must process chat requests and return model responses

Acceptance:

- `GET /health` returns OK
- `POST /api/chat` returns assistant response

### FR-4 Codex brain backend

- Uses `codex exec` under the hood
- Per-turn prompt includes soul + memory + task context

Acceptance:

- A non-command message routes through Codex and returns output.

### FR-5 Long-tail memory + tasks

- Persist conversation records and explicit memory notes
- Persist tasks with status transitions

Acceptance:

- Data survives process restart.

### FR-6 Sub-agent orchestration

- `/spawn <agent_id> | <soul>`
- `/delegate <agent_id> <task>`

Acceptance:

- Spawn updates config and delegation returns sub-agent answer.

### FR-7 Slack integration

- Slack Events API endpoint: `/slack/events`
- Signature verification with signing secret
- Reply via `chat.postMessage`

Acceptance:

- Valid Slack events trigger gateway processing and outbound replies.

### FR-8 Terminal UI

- Command: `openorchestrator tui`
- Must talk to gateway API with session and agent context

Acceptance:

- User can have interactive conversation and switch session/agent.

## 8) Command surface (MVP)

- `openorchestrator onboard`
- `openorchestrator gateway run [--host --port]`
- `openorchestrator tui [--url --session --agent]`
- `openorchestrator config show`
- `openorchestrator config set <path> <value>`
- `openorchestrator config path`

## 9) Agent command protocol (chat-level)

- `/help`
- `/agents`
- `/spawn <agent_id> | <soul prompt>`
- `/tasks`
- `/task add <title>`
- `/task done <id>`
- `/remember <note>`
- `/mem <query>`
- `/delegate <agent_id> <task>`

## 10) Architecture overview

- CLI entrypoint (`clap`) routes commands.
- Gateway (`axum`) handles API + Slack webhook.
- Orchestrator composes prompts, dispatches Codex runs, handles commands.
- State store persists conversations, memories, and tasks.
- TUI talks to gateway API over HTTP.

## 11) Security posture (MVP)

- Slack signing verification supported.
- No implicit remote exposure defaults (gateway binds from config; default loopback).
- Secrets are stored in local config (future: move to keychain/secret store).

## 12) VPS and packaging path

### Phase 2

- Add systemd/launchd service generation.
- Add Docker image and compose template.
- Add optional Socket Mode Slack backend.

### Phase 3

- Publish npm bootstrap wrapper (`npx openorchestrator` installer + binary download).
- Add OAuth onboarding UX wrappers.

## 13) Success metrics

- Time-to-first-reply under 10 minutes for new user onboarding.
- Gateway uptime with stable chat responses over 24h.
- Reliable replay of memory/tasks after restart.
