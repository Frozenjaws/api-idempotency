import os
import time

from fastapi import FastAPI, HTTPException

from api_idempotency import IdempotencyMiddleware


def make_app(db_path, counter_dir, lease=30.0, wait=0.0, delay=0.3):
    """Each handler execution drops a marker file, so counts work across processes."""
    app = FastAPI()
    app.add_middleware(
        IdempotencyMiddleware, db_path=db_path, lease_seconds=lease, wait_timeout=wait
    )

    def mark():
        open(os.path.join(counter_dir, f"{os.getpid()}-{time.time_ns()}"), "w").close()

    @app.post("/charge")
    async def charge(payload: dict):
        import anyio

        mark()
        await anyio.sleep(delay)
        return {"charged": payload["amount"], "pid": os.getpid()}

    @app.post("/fail")
    async def fail():
        mark()
        if len(os.listdir(counter_dir)) < 2:
            raise HTTPException(500, "boom")
        return {"ok": True}

    @app.post("/crash")
    async def crash():
        mark()
        if len(os.listdir(counter_dir)) < 2:
            raise RuntimeError("x")
        return {"ok": True}

    return app
