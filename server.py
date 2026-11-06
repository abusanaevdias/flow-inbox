"""Loopback-only JSON API for the local Flow Inbox demonstrator."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import sqlite3
from urllib.parse import parse_qs, urlsplit

from core import Conflict, Inbox, InvalidInput, MAX_PAYLOAD_BYTES, NotFound, StaleLease


class HTTPProblem(Exception):
    def __init__(self, status, message):
        self.status = status
        self.message = message


def parse_json(raw):
    """Reject ambiguous duplicate keys and non-standard non-finite numbers."""
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate object key")
            result[key] = value
        return result

    def invalid_number(value):
        raise ValueError(f"non-finite number: {value}")

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object, parse_constant=invalid_number)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise InvalidInput("body must be valid UTF-8 JSON with unique keys and finite numbers") from exc


def make_server(inbox, *, port=8765):
    class Handler(BaseHTTPRequestHandler):
        server_version = "FlowInbox/0.1"
        sys_version = ""

        def setup(self):
            super().setup()
            self.connection.settimeout(3)

        def log_message(self, *_):
            # Avoid recording event bodies or lease capabilities in local logs.
            pass

        def _send(self, status, data):
            body = json.dumps(data, ensure_ascii=True, allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def _body(self):
            if self.headers.get("Transfer-Encoding"):
                raise HTTPProblem(400, "transfer encoding is unsupported; send Content-Length")
            lengths = self.headers.get_all("Content-Length", [])
            if not lengths:
                raise HTTPProblem(411, "Content-Length is required")
            if len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():
                raise HTTPProblem(400, "Content-Length must be a non-negative integer")
            try:
                length = int(lengths[0])
            except ValueError as exc:
                raise HTTPProblem(400, "Content-Length is too long to parse") from exc
            if length > MAX_PAYLOAD_BYTES:
                raise HTTPProblem(413, "request body exceeds 65536 bytes")
            if self.headers.get_content_type().lower() != "application/json":
                raise HTTPProblem(415, "Content-Type must be application/json")
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise HTTPProblem(400, "request body is incomplete")
            result = parse_json(raw)
            if not isinstance(result, dict):
                raise InvalidInput("body must be a JSON object")
            return result

        def _route(self, method):
            parsed = urlsplit(self.path)
            parts = parsed.path.strip("/").split("/")
            if method == "GET" and parsed.path == "/":
                return 200, {"service": "flow-inbox", "events": "/api/events", "mode": "local demonstrator"}
            if method == "GET" and parsed.path == "/api/events":
                query = parse_qs(parsed.query, keep_blank_values=True)
                if set(query) - {"limit"} or len(query.get("limit", ["50"])) != 1:
                    raise InvalidInput("supported query: one limit from 1 to 100")
                try:
                    limit = int(query.get("limit", ["50"])[0])
                except ValueError as exc:
                    raise InvalidInput("limit must be an integer from 1 to 100") from exc
                return 200, {"events": inbox.list_events(limit=limit)}
            if parsed.query:
                raise InvalidInput("query parameters are unsupported on this route")
            if method == "GET" and len(parts) == 3 and parts[:2] == ["api", "events"]:
                return 200, {"event": inbox.get(parts[2])}
            if method == "POST" and parsed.path == "/api/events":
                payload = self._body()
                keys = self.headers.get_all("Idempotency-Key", [])
                if len(keys) != 1:
                    raise InvalidInput("one Idempotency-Key header is required")
                event, created = inbox.accept(keys[0], payload)
                return (201 if created else 200), {"created": created, "event": event}
            if method == "POST" and parsed.path == "/api/claim":
                if self._body():
                    raise InvalidInput("claim body must be an empty object")
                return 200, {"event": inbox.claim()}
            if method == "POST" and len(parts) == 4 and parts[:2] == ["api", "events"] and parts[3] in {"complete", "retry"}:
                body = self._body()
                expected = {"lease_token"} if parts[3] == "complete" else {"lease_token", "error"}
                if set(body) != expected:
                    raise InvalidInput("body fields must be: " + ", ".join(sorted(expected)))
                if parts[3] == "complete":
                    event = inbox.complete(parts[2], body["lease_token"])
                else:
                    event = inbox.retry(parts[2], body["lease_token"], body["error"])
                return 200, {"event": event}
            raise HTTPProblem(404, "route not found")

        def _dispatch(self, method):
            try:
                status, data = self._route(method)
            except HTTPProblem as exc:
                status, data = exc.status, {"error": exc.message}
            except InvalidInput as exc:
                status, data = 400, {"error": str(exc)}
            except (Conflict, StaleLease) as exc:
                status, data = 409, {"error": str(exc)}
            except NotFound as exc:
                status, data = 404, {"error": str(exc)}
            except (TimeoutError, socket.timeout):
                status, data = 408, {"error": "request body timed out"}
            except sqlite3.Error:
                status, data = 503, {"error": "inbox storage is unavailable; retry later"}
            except Exception:
                status, data = 500, {"error": "unexpected local server error"}
            try:
                self._send(status, data)
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def _unsupported(self):
            self._send(405, {"error": "supported methods: GET, POST"})

        do_DELETE = do_PUT = do_PATCH = do_OPTIONS = do_HEAD = _unsupported

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="inbox.sqlite3")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    api = make_server(Inbox(args.db), port=args.port)
    print(f"Flow Inbox: http://127.0.0.1:{api.server_port} (local demo)", flush=True)
    try:
        api.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        api.server_close()


if __name__ == "__main__":
    main()
