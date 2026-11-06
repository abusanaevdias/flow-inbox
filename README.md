# Flow Inbox

A small, durable webhook inbox you can inspect. Accept JSON once per idempotency key, claim work with a fenced lease, and follow retries through a persisted event history.

An original, AI-assisted portfolio demonstrator for **abusanaevdias**, built with Python 3.11+ and SQLite using only the standard library. Processing outcomes are simulated locally. Bundled events are synthetic.

## Run locally

No packages, provider accounts, or API keys are needed. From this directory:

```sh
python3.11 -m unittest -v
python3.11 worker.py --db inbox.sqlite3 seed sample_events.json
python3.11 server.py --db inbox.sqlite3 --port 8765
```

Open <http://127.0.0.1:8765>. The dashboard reads the actual database, lists the latest 100 events, and shows each selected event's payload and history. **Add synthetic events** is repeatable: matching demo keys are deduplicated. **Simulate success** and **Simulate retry** claim one eligible event and change its local state.

The same state is available from the CLI, including while the server is running:

```sh
python3.11 worker.py --db inbox.sqlite3 inspect
python3.11 worker.py --db inbox.sqlite3 run-once --outcome retry
python3.11 worker.py --db inbox.sqlite3 run-once
python3.11 worker.py --db inbox.sqlite3 inspect EVENT_ID
```

An explicit retry is unavailable for 2 seconds on the first attempt, then 4, 8, 16, and 30 seconds. The sixth failed attempt becomes `dead`. An empty claim is normal when every event is finished, leased, or waiting for a retry. Restarting the server or CLI preserves stored events and history. Use a fresh database filename for a fresh demonstration.

## What the implementation demonstrates

| Concern | Implemented behavior |
| --- | --- |
| Idempotency | A unique key plus SHA-256 digest of canonical JSON; reordered object keys deduplicate |
| Conflicting reuse | Same key with a different canonical payload returns HTTP 409 without overwriting the event |
| Competing workers | One `BEGIN IMMEDIATE` transaction selects and claims eligible work |
| Fenced completion | Only the current, unexpired lease token can complete or retry an event |
| Recovery | Expired leases become claimable again; each claim consumes an attempt |
| Retry budget | Bounded exponential delay and a final `dead` state after six attempts |
| Inspection | Payload, attempt count, timestamps, last error, and ordered transition history |
| HTTP boundary | Loopback-only binding, 64 KiB JSON requests, strict JSON parsing, bounded request reads, explicit error responses |

## API

All POST requests require `Content-Type: application/json`, `Content-Length`, and an object body. The server supports GET and POST; it does not enable cross-origin requests.

| Method | Route | Input / result |
| --- | --- | --- |
| POST | `/api/events` | Payload object and `Idempotency-Key` header; 201 created or 200 matching duplicate |
| GET | `/api/events?limit=50` | Latest event summaries; limit 1–100, payloads excluded |
| GET | `/api/events/EVENT_ID` | Stored event and full history; inspection omits lease tokens |
| POST | `/api/claim` | `{}`; returns one event with its lease token, or `{"event": null}` |
| POST | `/api/events/EVENT_ID/complete` | `{"lease_token":"TOKEN"}` |
| POST | `/api/events/EVENT_ID/retry` | `{"lease_token":"TOKEN","error":"synthetic failure"}` |

Example intake:

```sh
curl -i http://127.0.0.1:8765/api/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-curl-1' \
  --data '{"synthetic":true,"type":"demo.received"}'
```

Errors use `{"error":"..."}`: 400 invalid input, 404 absent route/event, 405 unsupported method, 408 read timeout, 409 conflict/stale lease, 411 missing length, 413 oversized body, 415 unsupported media, 503 unavailable SQLite storage, or 500 unexpected server error. Intake keys are 1–200 characters without control characters. Retry error text is limited to 1,000 characters. Text bound to SQLite must contain valid Unicode scalar values; escaped lone surrogates are rejected as input errors. Duplicate JSON object keys, non-finite values, and non-object payloads are rejected.

## Verification and scope

The unittest suite uses real temporary SQLite databases, eight competing claimant threads, real loopback HTTP requests, and subprocess CLI runs. It covers restart persistence, duplicate/conflict handling, claim exclusivity, stale tokens before and after reclaim, expiry while waiting for a database lock, retry eligibility, capped delays, final-attempt exhaustion, malformed and oversized requests, storage errors, and CLI inspection. See [ARCHITECTURE.md](ARCHITECTURE.md) for invariants and trade-offs.

This is a local demonstrator with no deployment, client usage, load benchmark, or production security assessment. It has no authentication, webhook-signature validation, lease heartbeat, external dispatch, retention policy, or schema migration system. It binds only to `127.0.0.1`; any process able to reach that local port can change its state. Keep its synthetic database on a local filesystem. A `succeeded` state records a local acknowledgement; it does not establish an external business result.

MIT licensed. See [LICENSE](LICENSE).
