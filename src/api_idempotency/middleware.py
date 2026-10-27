"""Pure ASGI middleware that makes requests idempotent."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable

import sys

import anyio
from anyio import to_thread

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

from .store import IdempotencyStore, Outcome, StoredResponse

REPLAY_HEADER = b"idempotent-replayed"


class IdempotencyMiddleware:
    """Guarantee at-most-once execution per ``Idempotency-Key``.

    Requests without the header (or with a method not in ``methods``) pass
    straight through. Otherwise the key is claimed in SQLite, the handler runs,
    and the response (status < 500) is stored and replayed for later requests
    carrying the same key. 5xx responses and exceptions release the key.
    """

    def __init__(
        self,
        app,
        db_path: str = "idempotency.db",
        header_name: str = "Idempotency-Key",
        methods: Iterable[str] = ("POST", "PATCH"),
        lease_seconds: float = 30.0,
        ttl_seconds: float = 24 * 3600.0,
        wait_timeout: float = 0.0,
        poll_interval: float = 0.05,
        store: IdempotencyStore | None = None,
        scope_func: Callable[[dict], str] | None = None,
        max_threads: int = 16,
    ) -> None:
        self.app = app
        self.header = header_name.lower().encode("latin-1")
        self.methods = {m.upper() for m in methods}
        self.wait_timeout = wait_timeout
        self.poll_interval = poll_interval
        self.scope_func = scope_func
        self._limiter = None
        self._max_threads = max_threads
        self.store = store or IdempotencyStore(
            db_path, lease_seconds=lease_seconds, ttl_seconds=ttl_seconds
        )

    async def _db(self, func, *args):
        """Run a blocking store call on a dedicated thread limiter."""
        if self._limiter is None:
            self._limiter = anyio.CapacityLimiter(self._max_threads)
        return await to_thread.run_sync(func, *args, limiter=self._limiter)

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or scope["method"] not in self.methods:
            return await self.app(scope, receive, send)
        key = None
        for name, value in scope["headers"]:
            if name == self.header:
                key = value.decode("latin-1").strip()
                break
        if not key:
            return await self.app(scope, receive, send)

        body = b""
        more = True
        while more:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body += message.get("body", b"")
            more = message.get("more_body", False)

        lock_scope = f"{scope['method']} {scope['path']}"
        if self.scope_func is not None:
            lock_scope = f"{self.scope_func(scope)}|{lock_scope}"
        fingerprint = hashlib.sha256(
            b"\0".join([
                scope["method"].encode(), scope["path"].encode(),
                scope.get("query_string", b""), body,
            ])
        ).hexdigest()

        deadline = time.monotonic() + self.wait_timeout
        while True:
            acq = await self._db(self.store.acquire, key, lock_scope, fingerprint)
            if acq.outcome is Outcome.IN_PROGRESS and time.monotonic() < deadline:
                await anyio.sleep(self.poll_interval)
                continue
            break

        if acq.outcome is Outcome.REPLAY:
            return await self._send_stored(send, acq.response, extra=[(REPLAY_HEADER, b"true")])
        if acq.outcome is Outcome.MISMATCH:
            return await self._send_json(
                send, 422, "Idempotency-Key reused with a different request"
            )
        if acq.outcome is Outcome.IN_PROGRESS:
            return await self._send_json(
                send, 409, "A request with this Idempotency-Key is still in progress",
                extra=[(b"retry-after", b"1")],
            )

        await self._execute(scope, send, body, key, lock_scope)

    async def _execute(self, scope, send, body, key, lock_scope) -> None:
        sent_body = False

        async def replay_receive():
            nonlocal sent_body
            if not sent_body:
                sent_body = True
                return {"type": "http.request", "body": body, "more_body": False}
            await anyio.sleep_forever()  # nothing more to read; wait for cancellation

        start = None
        chunks: list[bytes] = []

        async def capture_send(message) -> None:
            nonlocal start
            if message["type"] == "http.response.start":
                start = message
            elif message["type"] == "http.response.body":
                chunks.append(message.get("body", b""))

        async def keep_alive() -> None:
            interval = max(self.store.lease_seconds / 3, 0.01)
            while True:
                await anyio.sleep(interval)
                await self._db(self.store.heartbeat, key, lock_scope)

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(keep_alive)
                try:
                    await self.app(scope, replay_receive, capture_send)
                finally:
                    tg.cancel_scope.cancel()
        except BaseException as exc:
            with anyio.CancelScope(shield=True):
                await self._db(self.store.release, key, lock_scope)
            # The task group wraps app errors in an exception group; hand the
            # original exception back so host error handlers see its real type.
            while isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
                exc = exc.exceptions[0]
            raise exc

        if start is None:
            await self._db(self.store.release, key, lock_scope)
            return
        status = start["status"]
        headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in start["headers"]]
        response = StoredResponse(status, headers, b"".join(chunks))
        with anyio.CancelScope(shield=True):
            if status >= 500:
                await self._db(self.store.release, key, lock_scope)
            else:
                await self._db(self.store.complete, key, lock_scope, response)
        await self._send_stored(send, response)

    @staticmethod
    async def _send_stored(send, response: StoredResponse, extra=()) -> None:
        headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in response.headers]
        await send({
            "type": "http.response.start",
            "status": response.status_code,
            "headers": headers + list(extra),
        })
        await send({"type": "http.response.body", "body": response.body})

    @staticmethod
    async def _send_json(send, status: int, detail: str, extra=()) -> None:
        payload = json.dumps({"detail": detail}).encode()
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode()),
                *extra,
            ],
        })
        await send({"type": "http.response.body", "body": payload})
