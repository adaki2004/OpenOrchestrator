# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs` — CLI entrypoint (`onboard`, `gateway`, `tui`, `config`).
- `src/config.rs` — config schema, load/save, path overrides.
- `src/onboarding.rs` — interactive setup flow for Codex + Slack + gateway.
- `src/gateway.rs` — HTTP gateway runtime (`/health`, `/api/chat`, `/api/tasks`, `/slack/events`).
- `src/orchestrator.rs` — agent orchestration, souls, task/memory commands, Codex prompting.
- `src/state.rs` — persisted long-tail state (conversations, memory, tasks).
- `src/codex.rs` — Codex CLI bridge (`codex login status`, `codex exec`).
- `src/tui.rs` — minimal interactive terminal chat client.
- `docs/PRD.md` — product and roadmap specification.
- `README.md` — setup and runtime guide.

## Read This First (mandatory)

- `docs/PRD.md`
- `README.md`

## Agent Context & Persistence

- Your context window may compact as it grows; keep work resumable.
- Do not stop early due to token budget concerns.
- For long tasks, keep progress persisted in files (`docs/`, `PLANS.md`) so a later agent can continue without re-discovery.
- Maintain momentum: complete implementation + validation whenever feasible.

## Long Plans & PLANS.md / ExecPlans

For multi-hour or multi-phase tasks:

- Use an explicit ExecPlan (either a checked-in `PLANS.md` or `update_plan` state).
- Keep the plan current at each meaningful pause/handoff.
- Prefer short, verifiable steps (typically 5–12).
- Split partial completion into completed vs remaining steps.
- Record key decisions and discoveries (including dead ends).

Suggested `PLANS.md` sections:

- Purpose / Big Picture
- Progress
- Surprises & Discoveries
- Decision Log
- Outcomes & Retrospective

## Git Hygiene

- Do not `git push` from agents; keep changes local for human review/push.
- Never run destructive history/file reset commands unless explicitly requested.
- If unexpected unrelated local changes appear in files you are editing, pause and confirm direction before proceeding.

## Build, Test, and Development Commands

- Build: `cargo build`
- Run gateway: `cargo run -- gateway run`
- Run onboarding: `cargo run -- onboard`
- Run TUI: `cargo run -- tui`
- Config inspect: `cargo run -- config show`
- Tests: `timeout 30 cargo test` (use `timeout 120 cargo test` for longer suites)
- Lint: `cargo clippy --all-targets --all-features`
- Format: `cargo fmt`

Long-running tools must always be run with explicit timeouts or in non-interactive batch mode.

## Coding Style & Naming Conventions

- Rust style: rustfmt default (4-space indent).
- Naming:
  - `snake_case` for modules/files/functions
  - `CamelCase` for structs/enums/traits
  - `SCREAMING_SNAKE_CASE` for constants
- Keep patches focused; avoid unrelated refactors in the same change.

## Testing Guidelines

- Prefer table-driven unit tests for command parsing/orchestration logic.
- Add integration tests for API behavior where practical.
- Before handoff, run at least `cargo test` and relevant runtime smoke checks.
- Mark slow/network-dependent tests clearly.

## Commit & Pull Request Guidelines

- Commit subjects: imperative and specific (e.g., `add slack signature validation`).
- PR description should include:
  - summary and rationale
  - behavior change (before/after)
  - validation performed (`cargo build`, `cargo test`, smoke checks)
- Avoid noisy formatting-only diffs unless intentionally formatting.

## Security & Configuration Tips

- Never commit secrets (Slack tokens/signing secrets, API keys).
- Preferred local config path: `~/.openorchestrator/openorchestrator.json` (or `OPENORCHESTRATOR_CONFIG_PATH`).
- Keep machine-specific paths configurable, not hardcoded.
- Treat Slack/webhook payloads as untrusted input; preserve signature checks.
