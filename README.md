# OpenOrchestrator

Codex-first open-source agent orchestrator in Rust.

OpenOrchestrator is a minimal recreation of the OpenClaw style workflow:

- one always-on gateway
- one main brain + spawnable sub-agents (souls)
- long-tail memory + task persistence
- Slack integration
- terminal chat UI

## Why OpenOrchestrator

- Uses your existing Codex CLI authentication (`codex login` OAuth)
- Keeps runtime simple and local-first
- Gives you a practical base for 24/7 assistant automation

## Install

```bash
cd /Users/keszeyd/work/openorchestrator
cargo build
```

## Onboarding

```bash
cargo run -- onboard
```

This writes config to:

- default: `~/.openorchestrator/openorchestrator.json`
- override: `OPENORCHESTRATOR_CONFIG_PATH=/path/to/config.json`

## Run gateway

```bash
cargo run -- gateway run
```

Health check:

```bash
curl http://127.0.0.1:3769/health
```

## Open TUI

```bash
cargo run -- tui
```

Local TUI controls:

- `/session <id>` switch session
- `/agent <id>` switch active agent
- `/who` current context
- `/exit` quit

Everything else is sent to OpenOrchestrator (including agent/task commands).

## Agent orchestration commands

- `/help`
- `/agents`
- `/spawn <agent_id> | <soul prompt>`
- `/tasks`
- `/task add <title>`
- `/task done <id>`
- `/remember <note>`
- `/mem <query>`
- `/delegate <agent_id> <task>`

## Slack integration (Events API)

Gateway route: `POST /slack/events`

Configure Slack app:

- Bot token (`xoxb-...`)
- Choose mode:
  - Events API webhook mode: requires Signing secret + webhook URL
  - Socket Mode: requires App token (`xapp-...`), no webhook signature needed
- Event webhook URL (your gateway URL + `/slack/events`)
- Bot events: at least `app_mention` and message events

OpenOrchestrator verifies Slack signatures in Events API mode and supports Slack Socket Mode for local setups without inbound webhooks.

## Config commands

```bash
cargo run -- config show
cargo run -- config path
cargo run -- config set brain.model '"gpt-5"'
cargo run -- config set gateway.port 4001
```

## Project docs

- PRD: `docs/PRD.md`

## License

MIT
