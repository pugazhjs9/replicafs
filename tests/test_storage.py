"""Storage node tests use a temporary disk and an in-process HTTP client.

No server ports or real cloud accounts are required. Source downloads in repair
requests are mocked; the filesystem and atomic-write code are real.
"""
import errno
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from replicafs.common import sha256_hex
from replicafs.storage import app as storage

HEARTBEAT_LOOP = storage.heartbeat_loop


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "DATA_DIR", tmp_path)
    monkeypatch.setattr(storage, "MAX_CHUNK_BYTES", 1024)
    monkeypatch.delenv("CLUSTER_TOKEN", raising=False)
    with TestClient(storage.app, raise_server_exceptions=True) as test_client:
        # Avoid contacting a running cluster from a unit test.
        yield test_client


@pytest.fixture(autouse=True)
def no_heartbeat(monkeypatch):
    monkeypatch.setattr(storage, "heartbeat_loop", lambda stop: stop.wait())


def source_response(monkeypatch, data=b"replica", status=200):
    calls = []

    @contextmanager
    def fake_stream(method, url, **kwargs):
        calls.append((method, url, kwargs))
        response = httpx.Response(status, content=data, request=httpx.Request(method, url))
        try:
            yield response
        finally:
            response.close()

    monkeypatch.setattr(storage.httpx, "stream", fake_stream)
    return calls


def replicate_payload(data=b"replica", **overrides):
    return {"chunk_id": "chunk1", "source_url": "http://node1:8001", "checksum": sha256_hex(data), **overrides}


@pytest.mark.parametrize("data", [b"", b"hello", bytes(range(256)) * 4])
def test_put_get_and_idempotent_delete(client, tmp_path, data):
    response = client.put("/chunks/chunk1", content=data, headers={"X-Checksum": sha256_hex(data)})
    assert response.status_code == 200
    assert response.json() == {"chunk_id": "chunk1", "checksum": sha256_hex(data), "size": len(data)}
    assert (tmp_path / "chunk1").read_bytes() == data
    response = client.get("/chunks/chunk1")
    assert response.status_code == 200
    assert response.content == data
    assert response.headers["x-checksum"] == sha256_hex(data)
    assert client.delete("/chunks/chunk1").status_code == 200
    assert client.delete("/chunks/chunk1").status_code == 200
    assert client.get("/chunks/chunk1").status_code == 404


def test_mismatched_checksum_preserves_previous_chunk(client, tmp_path):
    (tmp_path / "chunk1").write_bytes(b"previous")
    response = client.put("/chunks/chunk1", content=b"bad", headers={"X-Checksum": sha256_hex(b"good")})
    assert response.status_code == 422
    assert (tmp_path / "chunk1").read_bytes() == b"previous"
    assert list(tmp_path.glob("*.tmp")) == []


@pytest.mark.parametrize("checksum", ["", "bad", "A" * 64, "x" * 64, "0" * 65])
def test_malformed_checksum_is_rejected(client, checksum):
    assert client.put("/chunks/chunk1", content=b"hello", headers={"X-Checksum": checksum}).status_code == 422


@pytest.mark.parametrize("chunk_id", ["", ".", "..", "a/b", "a.b", "a\\b", "a-b", "é", "a" * 129, "a\n"])
def test_chunk_path_rejects_unsafe_and_non_ascii_ids(chunk_id):
    with pytest.raises(HTTPException) as error:
        storage.chunk_path(chunk_id)
    assert error.value.status_code == 400


def test_symlinks_cannot_escape_storage_directory(client, tmp_path):
    target = tmp_path / "outside.txt"
    target.write_bytes(b"private")
    (tmp_path / "chunk1").symlink_to(target)
    for response in (
        client.get("/chunks/chunk1"),
        client.put("/chunks/chunk1", content=b"replace"),
        client.delete("/chunks/chunk1"),
    ):
        assert response.status_code == 400
    assert target.read_bytes() == b"private"


def test_oversized_upload_is_rejected_without_writing(client, tmp_path):
    response = client.put("/chunks/chunk1", content=b"x" * 1025)
    assert response.status_code == 413
    assert not (tmp_path / "chunk1").exists()


def test_chunked_upload_cannot_bypass_size_limit(client, tmp_path):
    response = client.put("/chunks/chunk1", content=iter([b"x" * 800, b"y" * 800]))
    assert response.status_code == 413
    assert not (tmp_path / "chunk1").exists()


@pytest.mark.parametrize("length", ["invalid", "-1"])
def test_invalid_content_length(client, length):
    assert client.put("/chunks/chunk1", content=b"x", headers={"Content-Length": length}).status_code == 400


def test_oversized_disk_chunk_is_bounded_on_read(client, tmp_path):
    (tmp_path / "chunk1").write_bytes(b"x" * 1025)
    assert client.get("/chunks/chunk1").status_code == 413


def test_disk_write_error_is_reported_as_insufficient_storage(client, monkeypatch):
    def fail_write(*args):
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr(storage, "write_chunk_atomically", fail_write)
    assert client.put("/chunks/chunk1", content=b"hello").status_code == 507


def test_non_file_read_and_delete_errors_are_controlled(client, tmp_path):
    (tmp_path / "chunk1").mkdir()
    assert client.get("/chunks/chunk1").status_code == 507
    assert client.delete("/chunks/chunk1").status_code == 507


def test_health_counts_only_real_chunks(client, tmp_path):
    (tmp_path / "chunk1").write_bytes(b"x")
    (tmp_path / "chunk2").mkdir()
    (tmp_path / "chunk3").symlink_to(tmp_path / "chunk1")
    (tmp_path / ".chunk1.random.tmp").write_bytes(b"unfinished")
    (tmp_path / "legacy.tmp").write_bytes(b"unfinished")
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["chunks"] == 1


def test_health_reports_unavailable_disk(client, tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "DATA_DIR", tmp_path / "doesnotexist")
    assert client.get("/health").status_code == 503


def test_concurrent_atomic_writes_leave_one_complete_chunk_and_no_temps(tmp_path):
    destination = tmp_path / "chunk1"
    values = [bytes([i]) * (1024 + i) for i in range(16)]
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda value: storage.write_chunk_atomically(destination, value), values))
    assert destination.read_bytes() in values
    assert list(tmp_path.iterdir()) == [destination]


def test_failed_replace_preserves_old_data_and_cleans_temp(tmp_path, monkeypatch):
    destination = tmp_path / "chunk1"
    destination.write_bytes(b"previous")

    def fail_replace(*args):
        raise OSError(errno.EIO, "rename failed")

    monkeypatch.setattr(storage.os, "replace", fail_replace)
    with pytest.raises(OSError):
        storage.write_chunk_atomically(destination, b"new")
    assert destination.read_bytes() == b"previous"
    assert list(tmp_path.iterdir()) == [destination]


def test_failed_flush_cleans_temp_before_file_becomes_visible(tmp_path, monkeypatch):
    def fail_sync(*args):
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr(storage.os, "fsync", fail_sync)
    with pytest.raises(OSError):
        storage.write_chunk_atomically(tmp_path / "chunk1", b"new")
    assert list(tmp_path.iterdir()) == []


def test_replicate_verifies_content_and_reports_receipt(client, tmp_path, monkeypatch):
    calls = source_response(monkeypatch)
    response = client.post("/replicate", json=replicate_payload(source_url="http://node1:8001/"))
    assert response.status_code == 200
    assert response.json() == {
        "chunk_id": "chunk1", "checksum": sha256_hex(b"replica"), "size": 7,
        "replicated_from": "http://node1:8001",
    }
    assert (tmp_path / "chunk1").read_bytes() == b"replica"
    assert calls[0][0:2] == ("GET", "http://node1:8001/chunks/chunk1")


@pytest.mark.parametrize("source", [
    "file:///etc/passwd", "ftp://host", "http://user:secret@host", "http://host/path",
    "http://host?query", "http://host#fragment", "http://", "http://host:bad", "http://host /",
    "\x00http://host", "http://host\\other",
])
def test_replicate_rejects_invalid_source_before_network(client, monkeypatch, source):
    calls = source_response(monkeypatch)
    assert client.post("/replicate", json=replicate_payload(source_url=source)).status_code == 422
    assert calls == []


@pytest.mark.parametrize("overrides", [{"chunk_id": "../escape"}, {"checksum": "bad"}, {"chunk_id": "é"}])
def test_replicate_validates_id_and_digest_before_network(client, monkeypatch, overrides):
    calls = source_response(monkeypatch)
    assert client.post("/replicate", json=replicate_payload(**overrides)).status_code == 422
    assert calls == []


@pytest.mark.parametrize("status", [404, 500, 302])
def test_replicate_source_failure_is_controlled(client, monkeypatch, tmp_path, status):
    source_response(monkeypatch, status=status)
    assert client.post("/replicate", json=replicate_payload()).status_code == 502
    assert not (tmp_path / "chunk1").exists()


def test_replicate_transport_error_is_controlled(client, monkeypatch):
    def fail_stream(*args, **kwargs):
        raise httpx.ConnectError("source offline")

    monkeypatch.setattr(storage.httpx, "stream", fail_stream)
    assert client.post("/replicate", json=replicate_payload()).status_code == 502


def test_replicate_rejects_corrupt_source_and_preserves_old_copy(client, tmp_path, monkeypatch):
    (tmp_path / "chunk1").write_bytes(b"previous")
    source_response(monkeypatch, data=b"corrupt")
    assert client.post("/replicate", json=replicate_payload()).status_code == 422
    assert (tmp_path / "chunk1").read_bytes() == b"previous"


def test_replicate_enforces_size_limit(client, tmp_path, monkeypatch):
    source_response(monkeypatch, data=b"x" * 1025)
    assert client.post("/replicate", json=replicate_payload()).status_code == 413
    assert not (tmp_path / "chunk1").exists()


def test_replicate_disk_failure_is_controlled(client, monkeypatch):
    source_response(monkeypatch)

    def fail_write(*args):
        raise OSError(errno.ENOSPC, "disk full")

    monkeypatch.setattr(storage, "write_chunk_atomically", fail_write)
    assert client.post("/replicate", json=replicate_payload()).status_code == 507


@pytest.mark.parametrize("method,path,kwargs", [
    ("put", "/chunks/chunk1", {"content": b"bytes"}),
    ("get", "/chunks/chunk1", {}),
    ("delete", "/chunks/chunk1", {}),
    ("post", "/replicate", {"json": replicate_payload()}),
])
def test_all_internal_endpoints_require_cluster_token(client, monkeypatch, method, path, kwargs):
    monkeypatch.setenv("CLUSTER_TOKEN", "cluster-secret")
    request = getattr(client, method)
    assert request(path, **kwargs).status_code == 401
    assert request(path, headers={"Authorization": "Bearer wrong"}, **kwargs).status_code == 401
    assert client.get("/health").status_code == 200


def test_valid_cluster_token_is_forwarded_to_replication_source(client, monkeypatch):
    monkeypatch.setenv("CLUSTER_TOKEN", "cluster-secret")
    calls = source_response(monkeypatch)
    response = client.post("/replicate", json=replicate_payload(), headers={"Authorization": "Bearer cluster-secret"})
    assert response.status_code == 200
    assert calls[0][2]["headers"] == {"Authorization": "Bearer cluster-secret"}


def test_valid_cluster_token_allows_storage_roundtrip(client, monkeypatch):
    monkeypatch.setenv("CLUSTER_TOKEN", "cluster-secret")
    headers = {"Authorization": "Bearer cluster-secret"}
    assert client.put("/chunks/chunk1", content=b"bytes", headers=headers).status_code == 200
    assert client.get("/chunks/chunk1", headers=headers).content == b"bytes"
    assert client.delete("/chunks/chunk1", headers=headers).status_code == 200


@pytest.mark.parametrize("status", [200, 500])
def test_heartbeat_forwards_token_and_checks_http_status(tmp_path, monkeypatch, caplog, status):
    monkeypatch.setattr(storage, "DATA_DIR", tmp_path)
    monkeypatch.setenv("CLUSTER_TOKEN", "cluster-secret")
    stop = threading.Event()
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        stop.set()  # One iteration is enough to inspect the heartbeat.
        return httpx.Response(status, request=httpx.Request("POST", url))

    monkeypatch.setattr(storage.httpx, "post", post)
    HEARTBEAT_LOOP(stop)
    assert len(calls) == 1
    assert calls[0][1]["headers"] == {"Authorization": "Bearer cluster-secret"}
    assert calls[0][1]["json"]["chunk_count"] == 0
    assert ("heartbeat failed" in caplog.text) is (status == 500)


def test_lifespan_stops_heartbeat_worker(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "DATA_DIR", tmp_path / "newdir")
    finished = threading.Event()

    def worker(stop):
        stop.wait()
        finished.set()

    monkeypatch.setattr(storage, "heartbeat_loop", worker)
    with TestClient(storage.app) as test_client:
        assert test_client.get("/health").status_code == 200
        assert not finished.is_set()
    assert finished.is_set()
