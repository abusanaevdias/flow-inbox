"""Inspect, seed, or simulate one local processing attempt; never dispatch URLs."""

import argparse
import json
from pathlib import Path
import sqlite3
import sys

from core import Conflict, Inbox, InvalidInput, NotFound, StaleLease, validate_retry_error
from server import parse_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="inbox.sqlite3")
    commands = parser.add_subparsers(dest="command", required=True)
    seed = commands.add_parser("seed", help="import a synthetic JSON fixture")
    seed.add_argument("fixture", type=Path)
    inspect = commands.add_parser("inspect", help="show stored events or one event's history")
    inspect.add_argument("event_id", nargs="?")
    run = commands.add_parser("run-once", help="simulate one success or retry without external effects")
    run.add_argument("--outcome", choices=["complete", "retry"], default="complete")
    run.add_argument("--error", default="synthetic transient failure")
    args = parser.parse_args()
    try:
        inbox = Inbox(args.db)
        if args.command == "seed":
            if args.fixture.stat().st_size > 1_048_576:
                raise InvalidInput("fixture exceeds 1 MiB")
            entries = parse_json(args.fixture.read_bytes())
            if not isinstance(entries, list) or len(entries) > 1000:
                raise InvalidInput("fixture must be an array of at most 1000 events")
            if any(not isinstance(entry, dict) or set(entry) != {"idempotency_key", "payload"} for entry in entries):
                raise InvalidInput("each fixture entry needs idempotency_key and payload")
            created = 0
            for entry in entries:
                _, is_new = inbox.accept(entry["idempotency_key"], entry["payload"])
                created += is_new
            result = {"created": created, "duplicates": len(entries) - created, "synthetic_fixture": str(args.fixture)}
        elif args.command == "inspect":
            result = {"event": inbox.get(args.event_id)} if args.event_id else {"events": inbox.list_events()}
        else:
            if args.outcome == "retry":
                validate_retry_error(args.error)
            event = inbox.claim()
            if event is not None:
                if args.outcome == "complete":
                    event = inbox.complete(event["id"], event["lease_token"])
                else:
                    event = inbox.retry(event["id"], event["lease_token"], args.error)
            result = {"event": event, "simulation": True}
        print(json.dumps(result, indent=2, ensure_ascii=True, allow_nan=False))
        return 0
    except (OSError, sqlite3.Error, InvalidInput, Conflict, NotFound, StaleLease) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
