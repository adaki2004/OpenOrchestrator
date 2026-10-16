import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("watchdog", Path(__file__).with_name("ella-watchdog.py"))
watchdog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watchdog)


class WatchdogTests(unittest.TestCase):
    def test_healthy_does_not_restart(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(watchdog, "problem", return_value=None), patch.object(watchdog, "command") as command:
            self.assertEqual(watchdog.run(Path(directory)), 0)
            command.assert_not_called()

    def test_persistent_failure_restarts_and_verifies(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(watchdog, "problem", side_effect=["stale", "stale", None]), patch.object(watchdog.time, "sleep"), patch.object(watchdog, "command", return_value=Mock(returncode=0)) as command:
            self.assertEqual(watchdog.run(Path(directory)), 0)
            command.assert_called_once_with("systemctl", "restart", watchdog.SERVICE, timeout=60)
            self.assertEqual(json.loads((Path(directory) / "last-recovery.json").read_text())["reason"], "stale")

    def test_transient_failure_does_not_restart(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(watchdog, "problem", side_effect=["stale", None]), patch.object(watchdog.time, "sleep"), patch.object(watchdog, "command") as command:
            self.assertEqual(watchdog.run(Path(directory)), 0)
            command.assert_not_called()

    def test_cooldown_prevents_restart_loop(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(watchdog, "problem", return_value="stale"), patch.object(watchdog.time, "sleep"), patch.object(watchdog, "command") as command:
            (Path(directory) / "last-recovery.json").write_text(json.dumps({"attempted_at": watchdog.time.time()}))
            self.assertEqual(watchdog.run(Path(directory)), 1)
            command.assert_not_called()

    def test_check_only_never_restarts(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(watchdog, "problem", return_value="stale"), patch.object(watchdog, "command") as command:
            self.assertEqual(watchdog.run(Path(directory), check_only=True), 1)
            command.assert_not_called()

    def test_restart_failure_is_reported(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(watchdog, "problem", return_value="stale"), patch.object(watchdog.time, "sleep"), patch.object(watchdog, "command", return_value=Mock(returncode=1)):
            self.assertEqual(watchdog.run(Path(directory)), 1)

    def test_live_heartbeat_does_not_depend_on_journal_or_channel_activity(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.read.return_value = json.dumps({"status": "ok", "slack_enabled": True, "slack_socket": {"healthy": True}}).encode()
        with patch.object(watchdog, "command", return_value=Mock(returncode=0)) as command, patch.object(watchdog.urllib.request, "urlopen", return_value=response):
            self.assertIsNone(watchdog.problem())
            self.assertEqual(command.call_count, 1)


if __name__ == "__main__":
    unittest.main()
