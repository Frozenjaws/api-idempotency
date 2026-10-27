"""SQLite-backed idempotency store.

The database row is the lock. A short ``BEGIN IMMEDIATE`` transaction claims
a key; no transaction is held while the request handler runs, so unrelated
writes are never blocked. Processes sharing the same database file (on a local
filesystem) therefore coordinate through SQLite's own file locking.
"""

from __future__ import annotations

import enum
import json
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field

_SCHEMA = """
CREATE TABLE IF NOT EXISTS idempotency_records (
    key              TEXT NOT NULL,
    scope            TEXT NOT NULL,
    fingerprint      TEXT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('in_progress', 'completed')),
    owner            TEXT NOT NULL,
    lease_expires_at REAL NOT NULL,
    status_code      INTEGER,
    headers_json     TEXT,
    body             BLOB,
    created_at       REAL NOT NULL,
    expires_at       REAL NOT NULL,
    PRIMARY KEY (key, scope)
);
CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_records (expires_at);
"""


class Outcome(enum.Enum):
    ACQUIRED = "acquired"  # caller owns the lock and must execute the request
    REPLAY = "replay"  # a stored response is available
    IN_PROGRESS = "in_progress"  # another owner holds a live lease
    MISMATCH = "mismatch"  # key reused with a different request


@dataclass
class StoredResponse:
    status_code: int
    headers: list[tuple[str, str]]
    body: bytes


@dataclass
class Acquisition:
    outcome: Outcome
    response: StoredResponse | None = None


@dataclass
class IdempotencyStore:
    path: str
    lease_seconds: float = 30.0
    ttl_seconds: float = 24 * 3600.0
    busy_timeout_ms: int = 10_000
    owner: str = field(default_factory=lambda: f"{os.getpid()}-{uuid.uuid4().hex}")
    _local: threading.local = field(default_factory=threading.local, repr=False)

    def __post_init__(self) -> None:
        conn = self._conn()
        conn.executescript(_SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        """One connection per thread (and per process after a fork)."""
        pid = os.getpid()
        conn = getattr(self._local, "conn", None)
        if conn is None or getattr(self._local, "pid", None) != pid:
            conn = sqlite3.connect(
                self.path, timeout=self.busy_timeout_ms / 1000, isolation_level=None
            )
            conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            self._local.conn = conn
            self._local.pid = pid
        return conn

    def acquire(self, key: str, scope: str, fingerprint: str) -> Acquisition:
        conn = self._conn()
        now = time.time()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT fingerprint, state, owner, lease_expires_at, status_code,"
                " headers_json, body, expires_at"
                " FROM idempotency_records WHERE key = ? AND scope = ?",
                (key, scope),
            ).fetchone()

            if row is not None and row[7] <= now:
                # Record expired: forget it and treat the key as new.
                conn.execute(
                    "DELETE FROM idempotency_records WHERE key = ? AND scope = ?",
                    (key, scope),
                )
                row = None

            if row is None:
                conn.execute(
                    "INSERT INTO idempotency_records (key, scope, fingerprint, state,"
                    " owner, lease_expires_at, created_at, expires_at)"
                    " VALUES (?, ?, ?, 'in_progress', ?, ?, ?, ?)",
                    (key, scope, fingerprint, self.owner, now + self.lease_seconds,
                     now, now + self.ttl_seconds),
                )
                result = Acquisition(Outcome.ACQUIRED)
            else:
                fp, state, owner, lease, status, headers_json, body, _ = row
                if fp != fingerprint:
                    result = Acquisition(Outcome.MISMATCH)
                elif state == "completed":
                    result = Acquisition(
                        Outcome.REPLAY,
                        StoredResponse(
                            status, [tuple(h) for h in json.loads(headers_json)], bytes(body)
                        ),
                    )
                elif lease < now:
                    # Previous owner died or stalled past its lease: take over.
                    conn.execute(
                        "UPDATE idempotency_records SET owner = ?, lease_expires_at = ?"
                        " WHERE key = ? AND scope = ? AND state = 'in_progress'",
                        (self.owner, now + self.lease_seconds, key, scope),
                    )
                    result = Acquisition(Outcome.ACQUIRED)
                else:
                    result = Acquisition(Outcome.IN_PROGRESS)
            conn.execute("COMMIT")
            return result
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def complete(self, key: str, scope: str, response: StoredResponse) -> bool:
        """Store the response. Returns False if we no longer own the lock."""
        cur = self._conn().execute(
            "UPDATE idempotency_records SET state = 'completed', status_code = ?,"
            " headers_json = ?, body = ? WHERE key = ? AND scope = ? AND owner = ?"
            " AND state = 'in_progress'",
            (response.status_code, json.dumps(response.headers), response.body,
             key, scope, self.owner),
        )
        return cur.rowcount == 1

    def release(self, key: str, scope: str) -> None:
        """Drop our in-progress lock so the client can retry."""
        self._conn().execute(
            "DELETE FROM idempotency_records WHERE key = ? AND scope = ?"
            " AND owner = ? AND state = 'in_progress'",
            (key, scope, self.owner),
        )

    def heartbeat(self, key: str, scope: str) -> bool:
        """Extend our lease; False means the lock was lost."""
        cur = self._conn().execute(
            "UPDATE idempotency_records SET lease_expires_at = ?"
            " WHERE key = ? AND scope = ? AND owner = ? AND state = 'in_progress'",
            (time.time() + self.lease_seconds, key, scope, self.owner),
        )
        return cur.rowcount == 1

    def cleanup(self) -> int:
        """Delete expired records; returns the number removed."""
        cur = self._conn().execute(
            "DELETE FROM idempotency_records WHERE expires_at <= ?", (time.time(),)
        )
        return cur.rowcount
