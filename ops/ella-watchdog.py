#!/usr/bin/env python3
"""Six-hour safety net; no credentials, model calls, or Slack messages."""
import argparse
import fcntl
import json
from pathlib import Path
import subprocess
import sys
import syslog
import time
import urllib.request

SERVICE = "openorchestrator.service"
HEALTH_URL = "http://127.0.0.1:3769/health"
COOLDOWN = 1800


def log(message):
    print(message, flush=True)
    syslog.syslog(syslog.LOG_NOTICE, message)


def command(*args, timeout=15):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)


def problem():
    if command("systemctl", "is-active", "--quiet", SERVICE).returncode:
        return "service is not active"
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=5) as response:
            health = json.load(response)
    except Exception:
        return "gateway health request failed"
    if health.get("status") != "ok":
        return "gateway reports unhealthy"
    if not health.get("slack_enabled"):
        return None
    socket = health.get("slack_socket")
    if "slack_socket" in health and socket is None:
        return None  # Webhook mode has no persistent WebSocket to monitor.
    if isinstance(socket, dict):
        if socket.get("healthy") is True:
            return None
        if health.get("uptime_seconds", 0) < 120:
            return "Slack heartbeat is starting"
        return "Slack heartbeat is unhealthy"
    # Compatibility with the old binary: Slack normally refreshes every few
    # hours. Treat a connection older than six hours as an expired lease,
    # rather than mistaking a quiet channel for a broken connection.
    result = command("journalctl", "-u", SERVICE, "--since", "6 hours ago",
                     "--grep", "Slack socket mode connected", "-n", "1", "-o", "cat", "--no-pager")
    if result.returncode or "Slack socket mode connected" not in result.stdout:
        return "no Slack connection established within the six-hour lease"
    return None


def run(state_dir, check_only=False):
    reason = problem()
    if reason is None:
        log("healthy: gateway and Slack connection; no restart")
        return 0
    if check_only:
        log("unhealthy (check only): " + reason)
        return 1
    if reason == "Slack heartbeat is starting":
        log("startup grace: deferring recovery")
        return 0
    # Require a second failure to filter transient reconnects.
    time.sleep(10)
    reason = problem()
    if reason is None or reason == "Slack heartbeat is starting":
        log("transient failure cleared; no restart")
        return 0
    state_path = state_dir / "last-recovery.json"
    previous = json.loads(state_path.read_text()) if state_path.exists() else {}
    if time.time() - previous.get("attempted_at", 0) < COOLDOWN:
        log("unhealthy: " + reason + "; recovery cooldown active")
        return 1
    temporary = state_path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"attempted_at": time.time(), "reason": reason}))
    temporary.replace(state_path)
    log("recovering: " + reason + "; restarting " + SERVICE)
    restarted = command("systemctl", "restart", SERVICE, timeout=60)
    if restarted.returncode:
        log("recovery failed: systemctl restart returned nonzero")
        return 1
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if problem() is None:
            log("recovered: gateway and Slack connection verified")
            return 0
        time.sleep(2)
    log("recovery failed: service did not regain healthy gateway and Slack connection")
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--state-dir", type=Path, default=Path("/var/lib/openorchestrator/watchdog"))
    args = parser.parse_args()
    syslog.openlog("ella-watchdog")
    args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (args.state_dir / "lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log("another watchdog is already running; skipping")
            return 0
        return run(args.state_dir, args.check_only)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        log("watchdog failed: " + type(error).__name__)
        sys.exit(1)
