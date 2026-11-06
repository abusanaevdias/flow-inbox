"""HTTP contracts through a real loopback server and temporary database."""

import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest

from core import Inbox

import server


class APITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.inbox = Inbox(Path(self.tmp.name) / "inbox.sqlite3")
        self.api = server.make_server(self.inbox, port=0)
        self.thread = threading.Thread(target=self.api.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.api.shutdown()
        self.api.server_close()
        self.thread.join(timeout=3)

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection(*self.api.server_address, timeout=3)
        self.addCleanup(connection.close)
        combined = {"Content-Type": "application/json", **(headers or {})}
        if body is not None and not isinstance(body, bytes):
            body = json.dumps(body).encode()
        connection.request(method, path, body=body, headers=combined)
        response = connection.getresponse()
        data = response.read()
        return response.status, json.loads(data)

    def test_http_accept_duplicate_conflict_and_inspection(self):
        status, accepted = self.request("POST", "/api/events", {"synthetic": True}, {"Idempotency-Key": "api-1"})
        self.assertEqual(status, 201)
        event_id = accepted["event"]["id"]
        status, duplicate = self.request("POST", "/api/events", {"synthetic": True}, {"Idempotency-Key": "api-1"})
        self.assertEqual(status, 200)
        self.assertFalse(duplicate["created"])
        self.assertEqual(duplicate["event"]["id"], event_id)
        status, _ = self.request("POST", "/api/events", {"synthetic": False}, {"Idempotency-Key": "api-1"})
        self.assertEqual(status, 409)
        status, listing = self.request("GET", "/api/events?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(len(listing["events"]), 1)
        self.assertNotIn("payload", listing["events"][0])
        status, inspection = self.request("GET", f"/api/events/{event_id}")
        self.assertEqual(status, 200)
        self.assertEqual(inspection["event"]["payload"], {"synthetic": True})
        self.assertEqual(inspection["event"]["history"][0]["action"], "accepted")

    def test_http_claim_complete_and_reject_stale_token(self):
        self.request("POST", "/api/events", {"synthetic": True}, {"Idempotency-Key": "work"})
        status, claimed = self.request("POST", "/api/claim", {})
        self.assertEqual(status, 200)
        event = claimed["event"]
        status, _ = self.request("POST", f"/api/events/{event['id']}/complete", {"lease_token": "wrong"})
        self.assertEqual(status, 409)
        status, completed = self.request("POST", f"/api/events/{event['id']}/complete", {"lease_token": event["lease_token"]})
        self.assertEqual(status, 200)
        self.assertEqual(completed["event"]["status"], "succeeded")
        self.assertNotIn("lease_token", completed["event"])
        status, empty = self.request("POST", "/api/claim", {})
        self.assertEqual(status, 200)
        self.assertIsNone(empty["event"])

    def test_http_retry_is_not_immediately_claimable(self):
        self.request("POST", "/api/events", {"synthetic": True}, {"Idempotency-Key": "retry"})
        _, claimed = self.request("POST", "/api/claim", {})
        event = claimed["event"]
        status, retried = self.request("POST", f"/api/events/{event['id']}/retry", {"lease_token": event["lease_token"], "error": "synthetic failure"})
        self.assertEqual(status, 200)
        self.assertEqual(retried["event"]["status"], "pending")
        _, empty = self.request("POST", "/api/claim", {})
        self.assertIsNone(empty["event"])

    def test_invalid_requests_have_bounded_explicit_errors(self):
        cases = [
            ("POST", "/api/events", b"{", {}, 400),
            ("POST", "/api/events", {}, {}, 400),
            ("POST", "/api/events", {}, {"Content-Type": "text/plain"}, 415),
            ("POST", "/api/events", b"x" * 65_537, {}, 413),
            ("POST", "/api/events", b'{"x":NaN}', {"Idempotency-Key": "nan"}, 400),
            ("POST", "/api/events", b'{"x":1,"x":2}', {"Idempotency-Key": "duplicate"}, 400),
            ("POST", "/api/events", b"[" * 2000, {"Idempotency-Key": "deep"}, 400),
            ("POST", "/api/claim", [], {}, 400),
            ("POST", "/api/claim", {}, {"Transfer-Encoding": "chunked"}, 400),
            ("POST", "/api/claim", {}, {"Content-Length": "9" * 5000}, 400),
            ("GET", "/api/events?limit=0", None, {}, 400),
            ("GET", "/api/events?limit=abc", None, {}, 400),
            ("GET", "/api/events?limit=1&limit=2", None, {}, 400),
            ("GET", "/api/events/missing", None, {}, 404),
            ("POST", "/absent", {}, {}, 404),
            ("DELETE", "/api/events", None, {}, 405),
        ]
        for method, path, body, headers, expected in cases:
            with self.subTest(method=method, path=path, expected=expected):
                status, data = self.request(method, path, body, headers)
                self.assertEqual(status, expected)
                self.assertIn("error", data)
                self.assertLess(len(json.dumps(data)), 1000)

    def test_missing_length_and_unavailable_database_are_explicit(self):
        connection = http.client.HTTPConnection(*self.api.server_address, timeout=3)
        self.addCleanup(connection.close)
        connection.putrequest("POST", "/api/claim")
        connection.putheader("Content-Type", "application/json")
        connection.endheaders()
        response = connection.getresponse()
        self.assertEqual(response.status, 411)
        response.read()
        self.inbox.path = str(Path(self.tmp.name) / "absent-directory" / "db.sqlite3")
        status, data = self.request("GET", "/api/events")
        self.assertEqual(status, 503)
        self.assertNotIn(self.tmp.name, json.dumps(data))

    def test_dashboard_is_served_without_exposing_arbitrary_local_files(self):
        connection = http.client.HTTPConnection(*self.api.server_address, timeout=3)
        self.addCleanup(connection.close)
        connection.request("GET", "/")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.getheader("Content-Type"), "text/html; charset=utf-8")
        self.assertGreater(len(response.read()), 100)
        status, _ = self.request("GET", "/core.py")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
