# api-idempotency

SQLite-backed `Idempotency-Key` support for FastAPI / Starlette apps.

```python
from fastapi import FastAPI
from api_idempotency import IdempotencyMiddleware

app = FastAPI()
app.add_middleware(IdempotencyMiddleware, db_path="/data/idempotency.db")
```

Behaviour for requests (POST/PATCH by default) carrying an `Idempotency-Key` header:

| Situation | Result |
|---|---|
| First request | Key is locked to this instance, handler runs, response (<500) is stored |
| Same key + same request, completed | Stored response replayed with `Idempotent-Replayed: true` |
| Same key, still running elsewhere | `409` (or wait up to `wait_timeout` seconds, then replay) |
| Same key, different method/path/query/body | `422` |
| Handler raised / returned 5xx | Lock released, client may retry |
| Owner crashed | Lease (`lease_seconds`, renewed by heartbeat) expires and another instance takes over |

Options: `db_path`, `header_name`, `methods`, `lease_seconds`, `ttl_seconds`,
`wait_timeout`, `poll_interval`. Call `IdempotencyStore.cleanup()` periodically to purge expired rows.

The lock is a database row claimed in a short `BEGIN IMMEDIATE` transaction; no transaction is
held while your handler runs.

## Caveats

- All instances/containers must share the **same SQLite file on a local disk volume of a single
  host**. SQLite file locking is unreliable on NFS and other network filesystems, so this does
  not provide cross-host guarantees.
- Responses are buffered in memory (streaming responses are not streamed) and requests are
  at-most-once only within the lease: a handler stalled past its lease with no heartbeat could be taken over.

Run tests with `uv run pytest`.

Keys must be unique per client. To stop two clients that happen to reuse a key from seeing each
other's responses, pass `scope_func=lambda scope: <e.g. hash of the Authorization header>`.
