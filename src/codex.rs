use anyhow::{Context, Result, bail};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::fs;
use tokio::process::Command;
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct CodexRunner {
    timeout: Duration,
}

impl Default for CodexRunner {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(600),
        }
    }
}

impl CodexRunner {
    pub async fn check_login_status(&self) -> Result<bool> {
        let output = Command::new("codex")
            .arg("login")
            .arg("status")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to run `codex login status`")?;

        if !output.status.success() {
            return Ok(false);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.to_lowercase().contains("logged in"))
    }

    pub async fn run_prompt(
        &self,
        workspace: &Path,
        prompt: &str,
        model: Option<&str>,
    ) -> Result<String> {
        if !workspace.exists() {
            bail!("workspace does not exist: {}", workspace.display());
        }

        let output_file =
            std::env::temp_dir().join(format!("openorchestrator-codex-{}.txt", Uuid::new_v4()));

        let mut command = Command::new("codex");
        command.arg("exec").arg("--skip-git-repo-check");

        if bypass_codex_sandbox() {
            command.arg("--dangerously-bypass-approvals-and-sandbox");
        } else {
            command.arg("--sandbox").arg(codex_sandbox_mode());
        }

        command
            .arg("--output-last-message")
            .arg(&output_file)
            .arg("--cd")
            .arg(workspace)
            .arg(prompt)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(model) = model {
            if !model.trim().is_empty() {
                command.arg("--model").arg(model.trim());
            }
        }

        let output = timeout(self.timeout, command.output())
            .await
            .context("timed out waiting for `codex exec`")??;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            bail!(
                "codex exec failed (status={}): {} {}",
                output.status,
                stderr.trim(),
                stdout.trim()
            );
        }

        let response = fs::read_to_string(&output_file).await.with_context(|| {
            format!("failed reading codex output file {}", output_file.display())
        })?;

        let _ = fs::remove_file(&output_file).await;

        let trimmed = response.trim();
        if trimmed.is_empty() {
            bail!("codex returned an empty response");
        }

        Ok(trimmed.to_string())
    }
}

fn codex_sandbox_mode() -> String {
    std::env::var("OPENORCHESTRATOR_CODEX_SANDBOX")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "workspace-write".to_string())
}

fn bypass_codex_sandbox() -> bool {
    std::env::var("OPENORCHESTRATOR_CODEX_BYPASS_SANDBOX")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}
