"""The CLI operates on the same persisted state as the HTTP API."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from core import Inbox


class WorkerTests(unittest.TestCase):
    def test_seed_process_and_inspect_real_stored_state(self):
        worker = Path(__file__).with_name("worker.py")
        self.assertTrue(worker.exists(), "local worker CLI is missing")
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "inbox.sqlite3"
            fixture = Path(temp) / "events.json"
            fixture.write_text(json.dumps([{"idempotency_key": "cli-demo", "payload": {"synthetic": True}}]))

            def run(*args):
                result = subprocess.run([sys.executable, str(worker), "--db", str(db), *args], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)

            first = run("seed", str(fixture))
            self.assertEqual(first["created"], 1)
            self.assertEqual(run("seed", str(fixture))["created"], 0)
            result = run("run-once")
            self.assertEqual(result["event"]["status"], "succeeded")
            found = run("inspect", result["event"]["id"])
            self.assertEqual([item["action"] for item in found["event"]["history"]], ["accepted", "claimed", "completed"])
            self.assertEqual(run("inspect")["events"][0]["status"], "succeeded")
            self.assertIsNone(run("run-once")["event"])
            Inbox(db).accept("cli-retry", {"synthetic": True})
            self.assertEqual(run("run-once", "--outcome", "retry")["event"]["status"], "pending")

    def test_cli_invalid_fixture_exits_without_traceback_or_insert(self):
        worker = Path(__file__).with_name("worker.py")
        self.assertTrue(worker.exists(), "local worker CLI is missing")
        with tempfile.TemporaryDirectory() as temp:
            fixture = Path(temp) / "bad.json"
            fixture.write_text('[{"unexpected": true}]')
            db = Path(temp) / "inbox.sqlite3"
            result = subprocess.run([sys.executable, str(worker), "--db", str(db), "seed", str(fixture)], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("Traceback", result.stderr)
            self.assertEqual(Inbox(db).list_events(), [])

    def test_cli_unicode_key_fixture_is_structured_error_without_traceback(self):
        worker = Path(__file__).with_name("worker.py")
        with tempfile.TemporaryDirectory() as temp:
            fixture = Path(temp) / "unicode.json"
            fixture.write_text(json.dumps([{"idempotency_key": "demo-\ud800", "payload": {"synthetic": True}}]))
            db = Path(temp) / "inbox.sqlite3"
            result = subprocess.run([sys.executable, str(worker), "--db", str(db), "seed", str(fixture)], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("Traceback", result.stderr)
            self.assertIn("error", json.loads(result.stderr))
            self.assertEqual(Inbox(db).list_events(), [])

    def test_cli_invalid_unicode_retry_argument_does_not_claim(self):
        worker = Path(__file__).with_name("worker.py")
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "inbox.sqlite3"
            inbox = Inbox(db)
            event, _ = inbox.accept("cli-invalid-retry", {"synthetic": True})
            # POSIX undecodable argument bytes arrive through surrogateescape.
            result = subprocess.run([sys.executable, str(worker), "--db", str(db), "run-once", "--outcome", "retry", "--error", "invalid-\udcff"], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("Traceback", result.stderr)
            self.assertIn("error", json.loads(result.stderr))
            current = inbox.get(event["id"])
            self.assertEqual(current["status"], "pending")
            self.assertEqual(current["attempts"], 0)


if __name__ == "__main__":
    unittest.main()
