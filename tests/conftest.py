"""Shared fixtures: a real PostgreSQL (pgserver) and a live uvicorn
server running in a background thread."""
from __future__ import annotations

import os
import socket
import threading
import time

import httpx
import pytest


@pytest.fixture(scope="session")
def base_url(tmp_path_factory):
    pgserver = pytest.importorskip("pgserver")
    srv = pgserver.get_server(str(tmp_path_factory.mktemp("pgdata")))
    os.environ["DATABASE_URL"] = srv.get_uri()

    import uvicorn
    from app.main import app

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    url = f"http://127.0.0.1:{port}"
    for _ in range(200):
        try:
            if httpx.get(url + "/healthz", timeout=1).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)
    else:
        raise RuntimeError("server did not start")
    yield url
    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture()
def client(base_url):
    with httpx.Client(base_url=base_url, timeout=30) as c:
        yield c
