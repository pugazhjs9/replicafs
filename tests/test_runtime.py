"""Startup and shutdown checks without opening network ports.

The metadata HTTP service and SQLite database are real. A waiting monitor worker
keeps lifecycle tests independent of repair timing and external storage nodes.
"""

import os
import subprocess
import sys
import threading

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from replicafs.metadata import app as metadata
from replicafs.metadata.db import Base


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'runtime.db'}", connect_args={"check_same_thread": False})
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    initialized = []
    workers = []
    started = threading.Event()
    outgoing = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(503)))

    def initialize():
        Base.metadata.create_all(engine)
        initialized.append(True)

    def monitor(stop):
        workers.append((threading.current_thread(), stop))
        started.set()
        stop.wait()

    monkeypatch.setattr(metadata, "init_db", initialize)
    monkeypatch.setattr(metadata, "SessionLocal", sessions)
    monkeypatch.setattr(metadata, "monitor_loop", monitor)
    monkeypatch.setattr(metadata, "client", outgoing)
    try:
        yield {"initialized": initialized, "workers": workers, "outgoing": outgoing, "started": started}
    finally:
        metadata.client.close()
        outgoing.close()
        engine.dispose()


def test_metadata_lifespan_initializes_database_and_stops_worker(runtime):
    with TestClient(metadata.app) as client:
        assert client.get("/health").status_code == 200
        assert runtime["started"].wait(timeout=2)
        assert len(runtime["initialized"]) == 1
        assert len(runtime["workers"]) == 1
        worker, stop = runtime["workers"][0]
        assert worker.is_alive()
        assert not stop.is_set()
        assert not runtime["outgoing"].is_closed
    assert stop.is_set()
    assert not worker.is_alive()
    assert runtime["outgoing"].is_closed


def test_repeated_lifespans_reopen_client_and_leave_no_worker_running(runtime):
    clients = []
    for _ in range(3):
        with TestClient(metadata.app) as client:
            assert client.get("/health").status_code == 200
            current = metadata.client
            assert not current.is_closed
            assert all(current is not previous for previous in clients)
            clients.append(current)
        assert current.is_closed
    assert len(runtime["initialized"]) == 3
    assert len(runtime["workers"]) == 3
    assert all(not worker.is_alive() and stop.is_set() for worker, stop in runtime["workers"])


def test_database_startup_failure_does_not_launch_monitor(runtime, monkeypatch):
    def fail_initialize():
        raise SQLAlchemyError("database migration failed")

    monkeypatch.setattr(metadata, "init_db", fail_initialize)
    with pytest.raises(SQLAlchemyError, match="database migration failed"), TestClient(metadata.app):
        pytest.fail("service must not start after database initialization failed")
    assert runtime["workers"] == []


def test_invalid_write_threshold_fails_before_metadata_startup():
    result = subprocess.run(
        [sys.executable, "-c", "import replicafs.metadata.app"],
        env={**os.environ, "REPLICATION_FACTOR": "2", "MIN_WRITE_REPLICAS": "3"},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert "MIN_WRITE_REPLICAS must not exceed REPLICATION_FACTOR" in result.stderr
