import multiprocessing
import os
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from api_idempotency import IdempotencyStore, Outcome, StoredResponse
from helpers import make_app

H = {"Idempotency-Key": "k1"}


@pytest.fixture
def env(tmp_path):
    counter = tmp_path / "calls"
    counter.mkdir()
    return str(tmp_path / "idem.db"), str(counter)


def calls(env):
    return len(os.listdir(env[1]))


def test_replay(env):
    client = TestClient(make_app(*env, delay=0))
    r1 = client.post("/charge", json={"amount": 5}, headers=H)
    r2 = client.post("/charge", json={"amount": 5}, headers=H)
    assert r1.json() == r2.json() and r2.status_code == 200
    assert r2.headers["idempotent-replayed"] == "true"
    assert "idempotent-replayed" not in r1.headers
    assert calls(env) == 1


def test_no_key_passes_through(env):
    client = TestClient(make_app(*env, delay=0))
    client.post("/charge", json={"amount": 5})
    client.post("/charge", json={"amount": 5})
    assert calls(env) == 2


def test_fingerprint_mismatch(env):
    client = TestClient(make_app(*env, delay=0))
    client.post("/charge", json={"amount": 5}, headers=H)
    assert client.post("/charge", json={"amount": 6}, headers=H).status_code == 422
    assert calls(env) == 1


def test_5xx_releases_lock(env):
    client = TestClient(make_app(*env, delay=0), raise_server_exceptions=False)
    assert client.post("/fail", headers=H).status_code == 500
    r = client.post("/fail", headers=H)
    assert r.status_code == 200 and calls(env) == 2


def test_thread_race_executes_once(env):
    app = make_app(*env, delay=0.5)
    results = []

    def go():
        with TestClient(app) as c:
            results.append(c.post("/charge", json={"amount": 1}, headers=H).status_code)

    threads = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert calls(env) == 1
    assert results.count(200) >= 1 and set(results) <= {200, 409}


def test_waiting_requests_get_the_same_response(env):
    app = make_app(*env, delay=0.5, wait=5.0)
    results = []

    def go():
        with TestClient(app) as c:
            results.append(c.post("/charge", json={"amount": 1}, headers=H))

    threads = [threading.Thread(target=go) for _ in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert calls(env) == 1
    assert [r.status_code for r in results] == [200] * 6
    assert len({r.text for r in results}) == 1


def test_expired_lease_is_taken_over(env):
    db = env[0]
    dead = IdempotencyStore(db, lease_seconds=0.05)
    assert dead.acquire("k", "POST /x", "fp").outcome is Outcome.ACQUIRED
    time.sleep(0.1)
    live = IdempotencyStore(db, lease_seconds=30)
    assert live.acquire("k", "POST /x", "fp").outcome is Outcome.ACQUIRED
    assert dead.complete("k", "POST /x", StoredResponse(200, [], b"")) is False  # lost the lock
    assert IdempotencyStore(db).acquire("k", "POST /x", "fp").outcome is Outcome.IN_PROGRESS


def test_cleanup(env):
    s = IdempotencyStore(env[0], ttl_seconds=0.01)
    s.acquire("k", "POST /x", "fp")
    time.sleep(0.05)
    assert s.cleanup() == 1


def _serve(db, counter, port):
    uvicorn.run(make_app(db, counter, wait=10.0), port=port, log_level="error")


def _wait_up(port):
    for _ in range(100):
        try:
            httpx.get(f"http://127.0.0.1:{port}/openapi.json")
            return
        except httpx.HTTPError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def test_two_server_processes_share_one_lock(env):
    ctx = multiprocessing.get_context("spawn")
    ports = [18931, 18932]
    procs = [ctx.Process(target=_serve, args=(*env, p), daemon=True) for p in ports]
    [p.start() for p in procs]
    try:
        for p in ports:
            _wait_up(p)
        out = []

        def go(port):
            out.append(httpx.post(f"http://127.0.0.1:{port}/charge",
                                  json={"amount": 9}, headers=H, timeout=20))

        threads = [threading.Thread(target=go, args=(ports[i % 2],)) for i in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        assert calls(env) == 1
        assert [r.status_code for r in out] == [200] * 8
        assert len({r.text for r in out}) == 1  # identical body, same pid
    finally:
        [p.terminate() for p in procs]


def test_unhandled_exception_releases_lock(env):
    client = TestClient(make_app(*env, delay=0), raise_server_exceptions=False)
    assert client.post("/crash", headers=H).status_code == 500
    assert client.post("/crash", headers=H).status_code == 200
    assert calls(env) == 2


def test_unhandled_exception_type_is_preserved(env):
    client = TestClient(make_app(*env, delay=0))
    with pytest.raises(RuntimeError):
        client.post("/crash", headers=H)
