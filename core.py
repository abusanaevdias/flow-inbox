"""A durable local inbox with fenced leases; no network dispatch."""

from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3
import time
import uuid

MAX_PAYLOAD_BYTES = 65_536
LEASE_SECONDS = 30
MAX_ATTEMPTS = 6
BASE_RETRY_SECONDS = 2
MAX_RETRY_SECONDS = 30


class InvalidInput(ValueError):
    """The request does not meet an inbox contract."""


class Conflict(Exception):
    """An existing idempotency key identifies a different payload."""


class NotFound(Exception):
    """The requested event does not exist."""


class StaleLease(Exception):
    """The caller no longer owns a live processing lease."""


def _require_utf8(value, field):
    if not isinstance(value, str):
        raise InvalidInput(f"{field} must be a string")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise InvalidInput(f"{field} must contain valid Unicode scalar values") from exc


def validate_retry_error(error):
    _require_utf8(error, "error")
    if not 1 <= len(error) <= 1_000:
        raise InvalidInput("error must contain 1–1000 characters")


class Inbox:
    def __init__(self, path, *, clock=time.time):
        self.path = str(Path(path))
        _require_utf8(self.path, "database path")
        self.clock = clock
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    digest TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN
                        ('pending', 'processing', 'succeeded', 'dead')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    available_at REAL NOT NULL,
                    lease_token TEXT,
                    lease_expires_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS events_ready
                    ON events(status, available_at);
                CREATE TABLE IF NOT EXISTS history (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL REFERENCES events(id),
                    action TEXT NOT NULL,
                    at REAL NOT NULL,
                    details TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS history_event ON history(event_id);
            """)

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def _write(self):
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _record(db, event_id, action, now, **details):
        db.execute(
            "INSERT INTO history(event_id, action, at, details) VALUES (?, ?, ?, ?)",
            (event_id, action, now, json.dumps(details, sort_keys=True)),
        )

    @staticmethod
    def _view(row, *, lease=False, summary=False):
        event = dict(row)
        event.pop("lease_token", None)
        if lease:
            event["lease_token"] = row["lease_token"]
        if summary:
            event.pop("payload")
        else:
            event["payload"] = json.loads(event["payload"])
        return event

    def accept(self, key, payload):
        _require_utf8(key, "idempotency key")
        if not isinstance(key, str) or not 1 <= len(key) <= 200 or any(ord(c) < 32 or ord(c) == 127 for c in key):
            raise InvalidInput("idempotency key must be 1–200 characters without controls")
        if not isinstance(payload, dict):
            raise InvalidInput("payload must be a JSON object")
        try:
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
            encoded = canonical.encode("utf-8")
        except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
            raise InvalidInput("payload must contain finite JSON values") from exc
        if len(encoded) > MAX_PAYLOAD_BYTES:
            raise InvalidInput("canonical payload exceeds 65536 bytes")
        digest = hashlib.sha256(encoded).hexdigest()
        with self._write() as db:
            now = self.clock()
            existing = db.execute("SELECT * FROM events WHERE idempotency_key=?", (key,)).fetchone()
            if existing:
                if existing["digest"] != digest:
                    raise Conflict("idempotency key already belongs to a different payload")
                return self._view(existing), False
            event_id = uuid.uuid4().hex
            db.execute(
                """INSERT INTO events(id, idempotency_key, digest, payload, status,
                   available_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)""",
                (event_id, key, digest, canonical, now, now, now),
            )
            self._record(db, event_id, "accepted", now, digest=digest)
            return self._view(db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()), True

    def claim(self):
        with self._write() as db:
            now = self.clock()
            exhausted = db.execute(
                "SELECT id FROM events WHERE status='processing' AND lease_expires_at<=? AND attempts>=?",
                (now, MAX_ATTEMPTS),
            ).fetchall()
            for row in exhausted:
                db.execute(
                    """UPDATE events SET status='dead', lease_token=NULL, lease_expires_at=NULL,
                       updated_at=?, last_error='lease expired after final attempt' WHERE id=?""",
                    (now, row["id"]),
                )
                self._record(db, row["id"], "exhausted", now)
            row = db.execute(
                """SELECT * FROM events WHERE attempts<? AND
                   ((status='pending' AND available_at<=?) OR
                    (status='processing' AND lease_expires_at<=?))
                   ORDER BY available_at, created_at, id LIMIT 1""",
                (MAX_ATTEMPTS, now, now),
            ).fetchone()
            if row is None:
                return None
            if row["status"] == "processing":
                self._record(db, row["id"], "lease_expired", now, attempt=row["attempts"])
            token = secrets.token_urlsafe(24)
            db.execute(
                """UPDATE events SET status='processing', attempts=attempts+1,
                   lease_token=?, lease_expires_at=?, updated_at=? WHERE id=?""",
                (token, now + LEASE_SECONDS, now, row["id"]),
            )
            self._record(db, row["id"], "claimed", now, attempt=row["attempts"] + 1)
            return self._view(db.execute("SELECT * FROM events WHERE id=?", (row["id"],)).fetchone(), lease=True)

    @staticmethod
    def _owned(db, event_id, token, now):
        _require_utf8(event_id, "event_id")
        _require_utf8(token, "lease_token")
        if not isinstance(token, str) or not token:
            raise InvalidInput("lease_token is required")
        row = db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFound("event not found")
        if row["status"] != "processing" or row["lease_token"] != token or row["lease_expires_at"] <= now:
            raise StaleLease("lease expired, changed, or event is no longer processing")
        return row

    def complete(self, event_id, token):
        with self._write() as db:
            now = self.clock()
            self._owned(db, event_id, token, now)
            db.execute(
                """UPDATE events SET status='succeeded', lease_token=NULL,
                   lease_expires_at=NULL, updated_at=?, last_error=NULL WHERE id=?""",
                (now, event_id),
            )
            self._record(db, event_id, "completed", now)
            return self._view(db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone())

    def retry(self, event_id, token, error):
        validate_retry_error(error)
        with self._write() as db:
            now = self.clock()
            row = self._owned(db, event_id, token, now)
            delay = min(MAX_RETRY_SECONDS, BASE_RETRY_SECONDS * 2 ** (row["attempts"] - 1))
            terminal = row["attempts"] >= MAX_ATTEMPTS
            status = "dead" if terminal else "pending"
            db.execute(
                """UPDATE events SET status=?, available_at=?, lease_token=NULL,
                   lease_expires_at=NULL, updated_at=?, last_error=? WHERE id=?""",
                (status, now if terminal else now + delay, now, error, event_id),
            )
            self._record(db, event_id, "exhausted" if terminal else "retry_scheduled", now, error=error, delay=0 if terminal else delay)
            return self._view(db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone())

    def get(self, event_id):
        _require_utf8(event_id, "event_id")
        with closing(self._connect()) as db:
            db.execute("BEGIN")
            row = db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None:
                raise NotFound("event not found")
            event = self._view(row)
            event["history"] = [dict(item) for item in db.execute(
                "SELECT sequence, action, at, details FROM history WHERE event_id=? ORDER BY sequence", (event_id,)
            )]
            for item in event["history"]:
                item["details"] = json.loads(item["details"])
            return event

    def list_events(self, *, limit=50):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise InvalidInput("limit must be an integer from 1 to 100")
        with closing(self._connect()) as db:
            return [self._view(row, summary=True) for row in db.execute(
                "SELECT * FROM events ORDER BY created_at DESC, id DESC LIMIT ?", (limit,)
            )]
