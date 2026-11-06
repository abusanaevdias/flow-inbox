"""State-machine contracts using a real temporary SQLite database."""

import concurrent.futures
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

import core


class InboxTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "inbox.sqlite3"
        self.now = 1_000.0
        self.inbox = core.Inbox(self.path, clock=lambda: self.now)

    def test_restart_preserves_payload_and_history(self):
        event, created = self.inbox.accept("demo-1", {"kind": "invoice", "amount": 12})
        self.assertTrue(created)
        restarted = core.Inbox(self.path, clock=lambda: self.now)
        found = restarted.get(event["id"])
        self.assertEqual(found["payload"], {"kind": "invoice", "amount": 12})
        self.assertEqual(found["status"], "pending")
        self.assertEqual([h["action"] for h in found["history"]], ["accepted"])

    def test_duplicate_is_canonical_and_conflict_does_not_overwrite(self):
        event, _ = self.inbox.accept("demo-1", {"b": 2, "a": 1})
        duplicate, created = self.inbox.accept("demo-1", {"a": 1, "b": 2})
        self.assertFalse(created)
        self.assertEqual(duplicate["id"], event["id"])
        with self.assertRaises(core.Conflict):
            self.inbox.accept("demo-1", {"a": 99})
        found = self.inbox.get(event["id"])
        self.assertEqual(found["payload"], {"a": 1, "b": 2})
        self.assertEqual(len(found["history"]), 1)
        self.assertEqual(len(self.inbox.list_events()), 1)

    def test_concurrent_claim_has_one_winner(self):
        event, _ = self.inbox.accept("race", {"synthetic": True})
        barrier = threading.Barrier(8)

        def claim(_):
            inbox = core.Inbox(self.path, clock=lambda: self.now)
            barrier.wait(timeout=5)
            return inbox.claim()

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(claim, range(8)))
        winners = [result for result in results if result is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0]["id"], event["id"])
        self.assertEqual(winners[0]["attempts"], 1)
        self.assertTrue(winners[0]["lease_token"])

    def test_expired_token_cannot_complete_or_retry_before_or_after_reclaim(self):
        event, _ = self.inbox.accept("expired", {"synthetic": True})
        old = self.inbox.claim()
        self.now = old["lease_expires_at"]
        with self.assertRaises(core.StaleLease):
            self.inbox.complete(event["id"], old["lease_token"])
        with self.assertRaises(core.StaleLease):
            self.inbox.retry(event["id"], old["lease_token"], "late")
        fresh = self.inbox.claim()
        self.assertNotEqual(fresh["lease_token"], old["lease_token"])
        self.assertEqual(fresh["attempts"], 2)
        with self.assertRaises(core.StaleLease):
            self.inbox.complete(event["id"], old["lease_token"])
        with self.assertRaises(core.StaleLease):
            self.inbox.retry(event["id"], old["lease_token"], "late")
        self.inbox.complete(event["id"], fresh["lease_token"])
        self.assertEqual(self.inbox.get(event["id"])["status"], "succeeded")
        self.assertNotIn("lease_token", self.inbox.get(event["id"]))

    def test_retry_delay_grows_caps_and_attempt_limit_is_terminal(self):
        event, _ = self.inbox.accept("retries", {"synthetic": True})
        for attempt, delay in [(1, 2), (2, 4), (3, 8), (4, 16), (5, 30)]:
            claimed = self.inbox.claim()
            self.assertEqual(claimed["attempts"], attempt)
            retried = self.inbox.retry(event["id"], claimed["lease_token"], "temporary")
            self.assertEqual(retried["available_at"], self.now + delay)
            self.assertIsNone(self.inbox.claim())
            self.now += delay - 0.01
            self.assertIsNone(self.inbox.claim())
            self.now += 0.01
        final = self.inbox.claim()
        self.assertEqual(final["attempts"], 6)
        failed = self.inbox.retry(event["id"], final["lease_token"], "still failing")
        self.assertEqual(failed["status"], "dead")
        self.now += 100_000
        self.assertIsNone(self.inbox.claim())
        restarted = core.Inbox(self.path, clock=lambda: self.now)
        self.assertEqual(restarted.get(event["id"])["last_error"], "still failing")

    def test_lease_expiring_while_waiting_for_writer_lock_cannot_complete(self):
        event, _ = self.inbox.accept("blocked", {"synthetic": True})
        claimed = self.inbox.claim()
        sampled = threading.Event()

        def clock():
            sampled.set()
            return self.now

        self.inbox.clock = clock
        blocker = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(blocker.close)
        blocker.execute("BEGIN IMMEDIATE")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(self.inbox.complete, event["id"], claimed["lease_token"])
            sampled.wait(timeout=0.1)
            self.now = claimed["lease_expires_at"]
            blocker.rollback()
            with self.assertRaises(core.StaleLease):
                result.result(timeout=5)

    def test_last_expired_lease_becomes_dead_without_seventh_attempt(self):
        event, _ = self.inbox.accept("crashing", {"synthetic": True})
        for attempt in range(1, 7):
            claimed = self.inbox.claim()
            self.assertEqual(claimed["attempts"], attempt)
            self.now = claimed["lease_expires_at"]
        self.assertIsNone(self.inbox.claim())
        found = self.inbox.get(event["id"])
        self.assertEqual(found["status"], "dead")
        self.assertEqual(found["history"][-1]["action"], "exhausted")

    def test_invalid_payloads_and_missing_events_are_explicit(self):
        for key, payload in [("", {}), ("bad\nkey", {}), ("x", []), ("x", {"bad": float("nan")}), ("x", {"big": "x" * 70_000})]:
            with self.subTest(key=key, payload_type=type(payload).__name__):
                with self.assertRaises(core.InvalidInput):
                    self.inbox.accept(key, payload)
        with self.assertRaises(core.NotFound):
            self.inbox.get("missing")
        self.assertEqual(self.inbox.list_events(), [])


if __name__ == "__main__":
    unittest.main()
