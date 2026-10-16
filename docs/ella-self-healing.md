# Ella self-healing operations

Installed on `alpha-1` (`194.35.12.128`) on 2026-10-16. The gateway remains private at `127.0.0.1:3769`.

## Recovery layers

The gateway sends a WebSocket ping immediately after connecting and every 30 seconds thereafter. It requires the matching pong within 15 seconds. A silent connection is dropped and the existing reconnect loop obtains a fresh Slack URL after three seconds. Handshakes have a 20-second deadline; ACK, ping/pong writes, and close have 10-second deadlines. Reconnecting the socket leaves ongoing model work in the gateway running.

Previously, `reader.next()` could wait forever on a silently dead connection while `/health` continued reporting OK. Slack's SDKs also use ping/pong deadlines to detect this condition: [Slack SocketModeOptions](https://docs.slack.dev/tools/node-slack-sdk/reference/socket-mode/interfaces/SocketModeOptions/).

`GET /health` now includes `slack_socket` in Socket Mode:

```json
{"status":"ok","uptime_seconds":360,"slack_enabled":true,"slack_socket":{"healthy":true,"last_pong_age_seconds":12}}
```

Socket health stays false until a matching pong arrives and resets on disconnect. `slack_socket` is null for disabled Slack or webhook mode. Top-level `status` retains its existing HTTP gateway meaning. No user messages are needed to establish socket health.

The independent watchdog checks the systemd service and HTTP/socket health every six hours. Persistent failures trigger one restart of `openorchestrator.service`, followed by verification for up to 60 seconds. It checks twice, ten seconds apart, allows two minutes for startup, locks against concurrent checks, and enforces a 30-minute recovery cooldown. It never runs a model or sends messages. A service restart can interrupt in-flight work; the heartbeat normally avoids needing a full restart.

For an older binary without socket health, the watchdog uses a six-hour connection lease from journal logs as a fallback. This is a bounded refresh policy, not proof of socket failure. New binaries use actual pong health, so healthy quiet channels do not cause restarts.

## Installed files

- `/usr/local/sbin/ella-watchdog` from `ops/ella-watchdog.py`
- `/etc/cron.d/ella-watchdog` from `ops/ella-watchdog.cron`
- `/var/lib/openorchestrator/watchdog/` for its lock and last recovery timestamp
- `/opt/openorchestrator-recovery-20261016/` for the previous binary, source, unit, and existing source diff (root-only)

Cron runs at **00:17, 06:17, 12:17, and 18:17 in the server timezone**. The command has a four-minute outer timeout. Diagnostic logs use the `ella-watchdog` syslog identifier.

```bash
ssh alpha-1 'timeout 20 /usr/local/sbin/ella-watchdog --check-only'
ssh alpha-1 'timeout 240 /usr/local/sbin/ella-watchdog'
ssh alpha-1 'curl -fsS --max-time 5 http://127.0.0.1:3769/health'
ssh alpha-1 'journalctl -t ella-watchdog --since "24 hours ago" --no-pager'
ssh alpha-1 'journalctl -u openorchestrator --since "1 hour ago" --no-pager'
```

To reinstall the watchdog, copy the two `ops/ella-watchdog.*` files to a temporary directory on the host, then use `install -m 755` for the script and `install -m 644` for `/etc/cron.d/ella-watchdog`. The cron daemon automatically reads the file. No credentials are required.

## Rollback

Disable scheduled recovery by renaming `/etc/cron.d/ella-watchdog` to `/etc/cron.d/ella-watchdog.disabled` (cron ignores filenames containing a dot). Restore the old binary by stopping the service, copying `/opt/openorchestrator-recovery-20261016/openorchestrator.before` to `/opt/openorchestrator/target/release/openorchestrator`, and starting the service. The old source is in `src.before`; restore only the files involved after reviewing any newer changes. Configuration, credentials, conversations, and tasks are not part of this deployment.

The existing production timeout, bounded prompts, error reporting, and delegation/spawn fixes were preserved. The delegation parser also now tries the shorter `to` separator when the longer `agent to` separator is absent; its pre-existing test exposed this bug during validation.
