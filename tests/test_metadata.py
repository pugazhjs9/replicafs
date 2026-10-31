"""Metadata API behavior against real SQLite and simulated storage HTTP calls.

The simulator replaces only the network boundary. Transactions, file chunking,
version assignment, authentication, and checksum validation run unchanged.
"""

import errno
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import quote

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from replicafs.common import sha256_hex
from replicafs.metadata import app as metadata
from replicafs.metadata.db import Base, Chunk, File, Node, Replica, utcnow


class StorageSimulator:
    def __init__(self):
        self.data = {}
        self.requests = []
        self.failure = None

    def handle(self, request):
        self.requests.append(request)
        if self.failure is not None:
            response = self.failure(request)
            if response is not None:
                return response
        node = request.url.host
        chunk = request.url.path.rsplit("/", 1)[-1]
        key = (node, chunk)
        if request.method == "PUT":
            data = request.read()
            assert request.headers["x-checksum"] == sha256_hex(data)
            self.data[key] = data
            return httpx.Response(
                200, json={"chunk_id": chunk, "checksum": sha256_hex(data), "size": len(data)}
            )
        if request.method == "GET":
            if key not in self.data:
                return httpx.Response(404)
            data = self.data[key]
            return httpx.Response(200, content=data, headers={"X-Checksum": sha256_hex(data)})
        if request.method == "DELETE":
            self.data.pop(key, None)
            return httpx.Response(200, json={"deleted": chunk})
        raise AssertionError(f"unexpected storage request: {request.method} {request.url}")


@dataclass
class API:
    client: TestClient
    sessions: sessionmaker
    storage: StorageSimulator

    def heartbeat(self, node="node1", free_bytes=10000, **overrides):
        return self.client.post(
            "/nodes/heartbeat",
            json={
                "node_id": node,
                "url": f"http://{node}:8001",
                "chunk_count": 0,
                "free_bytes": free_bytes,
                **overrides,
            },
        )

    def nodes(self, count=3):
        for number in range(1, count + 1):
            response = self.heartbeat(f"node{number}")
            assert response.status_code == 200, response.text

    def upload(self, data=b"abcdefghij", name="sample.bin"):
        return self.client.post("/files", files={"file": (name, data, "application/octet-stream")})

    def count(self, model):
        with self.sessions() as db:
            return db.query(model).count()


@pytest.fixture
def api(tmp_path, monkeypatch):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'metadata.db'}", connect_args={"check_same_thread": False, "timeout": 30}
    )

    @event.listens_for(engine, "connect")
    def sqlite_foreign_keys(connection, _record):
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")

    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    simulator = StorageSimulator()
    outgoing = httpx.Client(transport=httpx.MockTransport(simulator.handle))
    monkeypatch.setattr(metadata, "SessionLocal", sessions)
    monkeypatch.setattr(metadata, "client", outgoing)
    monkeypatch.setattr(metadata, "CHUNK_SIZE", 4)
    monkeypatch.setattr(metadata, "REPLICATION_FACTOR", 3)
    monkeypatch.setattr(metadata, "MIN_WRITE_REPLICAS", 1, raising=False)
    monkeypatch.setattr(metadata, "MAX_FILE_BYTES", 64, raising=False)
    monkeypatch.delenv("API_TOKEN", raising=False)
    monkeypatch.delenv("CLUSTER_TOKEN", raising=False)
    # Deliberately avoid entering TestClient: unit tests do not start monitor threads.
    incoming = TestClient(metadata.app)
    try:
        yield API(incoming, sessions, simulator)
    finally:
        incoming.close()
        outgoing.close()
        engine.dispose()


def test_upload_chunk_boundaries_replica_placement_and_download(api):
    api.nodes()
    data = b"abcdefghij"
    response = api.upload(data)
    assert response.status_code == 201, response.text
    info = response.json()
    assert info["name"] == "sample.bin"
    assert info["size"] == 10
    assert info["chunks"] == 3
    assert info["version"] == 1
    assert info["checksum"] == sha256_hex(data)
    assert info["replication_factor"] == 3
    assert info["min_replicas"] == 3
    assert info["degraded"] is False
    assert api.count(File) == 1
    assert api.count(Chunk) == 3
    assert api.count(Replica) == 9
    assert sorted(len(content) for content in api.storage.data.values()) == [2, 2, 2, 4, 4, 4, 4, 4, 4]
    response = api.client.get("/files/sample.bin")
    assert response.status_code == 200
    assert response.content == data
    assert response.headers["content-length"] == str(len(data))
    assert response.headers["content-type"] == "application/octet-stream"


@pytest.mark.parametrize("data, chunks", [(b"", 0), (b"x", 1), (b"abcd", 1), (b"abcde", 2), (b"x" * 64, 16)])
def test_empty_exact_tail_and_maximum_upload_boundaries(api, data, chunks):
    api.nodes()
    response = api.upload(data)
    assert response.status_code == 201
    assert response.json()["chunks"] == chunks
    assert response.json()["checksum"] == sha256_hex(data)
    assert api.client.get("/files/sample.bin").content == data


def test_upload_without_nodes_returns_service_unavailable(api):
    assert api.upload().status_code == 503
    assert api.count(File) == 0
    assert not api.storage.data


def test_upload_without_free_space_returns_service_unavailable(api):
    assert api.heartbeat(free_bytes=0).status_code == 200
    assert api.upload().status_code == 503
    assert api.count(File) == 0
    assert not api.storage.data


def test_unknown_free_space_is_distinct_from_a_known_full_disk(api):
    assert api.heartbeat(free_bytes=-1).status_code == 200
    assert api.upload(b"abcdefgh").status_code == 201
    assert api.client.get("/files/sample.bin").content == b"abcdefgh"


def test_capacity_depleted_within_upload_cannot_be_reused_as_unknown(api):
    assert api.heartbeat(free_bytes=4).status_code == 200
    # The first 4-byte chunk fills this node. The next chunk cannot be placed.
    assert api.upload(b"abcdefgh").status_code == 503
    assert api.count(File) == api.count(Chunk) == api.count(Replica) == 0
    assert not api.storage.data


def test_capacity_less_than_chunk_size_is_not_selected(api):
    assert api.heartbeat(free_bytes=3).status_code == 200
    assert api.upload(b"abcd").status_code == 503
    assert not api.storage.requests


def test_degraded_upload_reports_actual_replication(api):
    api.nodes(2)
    response = api.upload()
    assert response.status_code == 201
    assert response.json()["degraded"] is True
    assert response.json()["min_replicas"] == 2
    assert api.client.get("/files/sample.bin").content == b"abcdefghij"


def test_minimum_write_replicas_is_enforced_and_partial_data_is_cleaned(api, monkeypatch):
    api.nodes(2)
    monkeypatch.setattr(metadata, "MIN_WRITE_REPLICAS", 3)
    assert api.upload().status_code == 503
    assert api.count(File) == api.count(Chunk) == api.count(Replica) == 0
    assert not api.storage.data


@pytest.mark.parametrize("failure_mode", ["http", "transport"])
def test_upload_tries_healthy_alternate_after_node_failure(api, failure_mode):
    api.nodes(4)

    def fail_node1(request):
        if request.method == "PUT" and request.url.host == "node1":
            if failure_mode == "transport":
                raise httpx.ConnectError("node unavailable", request=request)
            return httpx.Response(500)

    api.storage.failure = fail_node1
    response = api.upload(b"abcd")
    assert response.status_code == 201
    assert response.json()["min_replicas"] == 3
    assert response.json()["degraded"] is False
    assert {node for node, _ in api.storage.data} == {"node2", "node3", "node4"}


def test_later_chunk_failure_rolls_back_file_and_cleans_earlier_chunks(api):
    api.nodes()

    def fail_second_chunk(request):
        if request.method == "PUT" and request.read() == b"efgh":
            return httpx.Response(507)

    api.storage.failure = fail_second_chunk
    assert api.upload(b"abcdefgh").status_code == 503
    assert api.client.get("/files").json() == []
    assert api.count(File) == api.count(Chunk) == api.count(Replica) == 0
    assert not api.storage.data
    assert len([request for request in api.storage.requests if request.method == "DELETE"]) >= 3


def test_database_commit_failure_cleans_uncommitted_storage(api, monkeypatch):
    api.nodes()

    class FailingSession(Session):
        def commit(self):
            if any(isinstance(record, File) for record in self.new):
                raise SQLAlchemyError("simulated metadata commit failure")
            return super().commit()

    monkeypatch.setattr(
        metadata,
        "SessionLocal",
        sessionmaker(
            bind=api.sessions.kw["bind"],
            class_=FailingSession,
            expire_on_commit=False,
        ),
    )
    response = api.upload(b"abcd")
    assert response.status_code in {500, 503}
    assert api.count(File) == api.count(Chunk) == api.count(Replica) == 0
    assert not api.storage.data


def test_exceeding_file_limit_leaves_no_visible_or_orphaned_data(api):
    api.nodes()
    assert api.upload(b"x" * 65).status_code == 413
    assert api.count(File) == api.count(Chunk) == api.count(Replica) == 0
    assert not api.storage.data


@pytest.mark.parametrize("name", ["..", "../escape", "bad\\name", "bad\nname", "bad\rname", ".", "x" * 256])
def test_invalid_filenames_are_rejected(api, name):
    api.nodes()
    if "\n" in name or "\r" in name:
        # httpx's normal multipart encoder escapes these controls to literal
        # %0A/%0D text. Send the raw header to exercise actual control rejection.
        body = (
            f'--boundary\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\n'
            "Content-Type: application/octet-stream\r\n\r\nx\r\n--boundary--\r\n"
        ).encode()
        response = api.client.post(
            "/files", content=body, headers={"Content-Type": "multipart/form-data; boundary=boundary"}
        )
    else:
        response = api.upload(b"x", name=name)
    assert response.status_code == 400, response.text
    assert api.count(File) == 0


def test_missing_multipart_file_is_validation_error(api):
    assert api.client.post("/files").status_code == 422
    assert api.client.post("/files", data={"file": "text instead of an upload"}).status_code == 422


def test_unicode_filename_roundtrips_with_rfc5987_content_disposition(api):
    api.nodes()
    name = "தமிழ் résumé.txt"
    assert api.upload(b"unicode", name=name).status_code == 201
    response = api.client.get(f"/files/{quote(name, safe='')}")
    assert response.status_code == 200
    assert response.content == b"unicode"
    assert (
        f"filename*=UTF-8''{quote(name, safe='')}".lower() in response.headers["content-disposition"].lower()
    )


def test_file_versions_listing_soft_delete_and_restore(api):
    api.nodes()
    assert api.upload(b"one").json()["version"] == 1
    assert api.upload(b"two").json()["version"] == 2
    assert api.client.get("/files/sample.bin").content == b"two"
    assert api.client.get("/files/sample.bin?version=1").content == b"one"
    files = api.client.get("/files").json()
    assert len(files) == 1 and files[0]["version"] == 2
    assert [row["version"] for row in api.client.get("/files/sample.bin/versions").json()] == [1, 2]
    assert api.client.delete("/files/sample.bin").status_code == 200
    assert api.client.get("/files/sample.bin").status_code == 404
    assert api.client.get("/files/sample.bin?version=1").status_code == 404
    assert api.client.get("/files").json() == []
    assert all(row["deleted_at"] for row in api.client.get("/files/sample.bin/versions").json())
    assert api.client.post("/files/sample.bin/restore").status_code == 200
    assert api.client.get("/files/sample.bin").content == b"two"
    assert api.client.get("/files/sample.bin?version=1").content == b"one"
    assert all(row["deleted_at"] is None for row in api.client.get("/files/sample.bin/versions").json())


def test_reupload_after_delete_does_not_reuse_version_number(api):
    api.nodes()
    assert api.upload(b"one").json()["version"] == 1
    assert api.client.delete("/files/sample.bin").status_code == 200
    assert api.upload(b"two").json()["version"] == 2
    assert api.client.get("/files/sample.bin").content == b"two"
    assert api.client.get("/files/sample.bin?version=1").status_code == 404
    assert api.client.post("/files/sample.bin/restore").status_code == 200
    assert api.client.get("/files/sample.bin?version=1").content == b"one"
    assert api.client.get("/files/sample.bin").content == b"two"


def test_concurrent_same_filename_uploads_get_unique_consecutive_versions(api):
    api.nodes()
    values = [f"value-{number}".encode() for number in range(8)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(api.upload, values))
    assert all(response.status_code == 201 for response in responses), [
        response.text for response in responses
    ]
    assert sorted(response.json()["version"] for response in responses) == list(range(1, 9))
    for response, data in zip(responses, values):
        assert api.client.get(f"/files/sample.bin?version={response.json()['version']}").content == data


@pytest.mark.parametrize(
    "path,method",
    [
        ("/files/absent", "get"),
        ("/files/absent/versions", "get"),
        ("/files/absent", "delete"),
        ("/files/absent/restore", "post"),
    ],
)
def test_missing_files_return_not_found(api, path, method):
    assert getattr(api.client, method)(path).status_code == 404


@pytest.mark.parametrize("version", ["0", "-1", "abc"])
def test_invalid_download_version_is_validation_error(api, version):
    assert api.client.get(f"/files/sample.bin?version={version}").status_code == 422


def test_repeated_delete_and_restore_report_missing_action(api):
    api.nodes()
    api.upload()
    assert api.client.post("/files/sample.bin/restore").status_code == 404
    assert api.client.delete("/files/sample.bin").status_code == 200
    assert api.client.delete("/files/sample.bin").status_code == 404
    assert api.client.post("/files/sample.bin/restore").status_code == 200
    assert api.client.post("/files/sample.bin/restore").status_code == 404


@pytest.mark.parametrize("corruption", ["missing", "wrong", "oversize"])
def test_download_bypasses_invalid_replica_and_forgets_its_metadata(api, corruption):
    api.nodes()
    api.upload(b"abcd")
    key = next(iter(api.storage.data))
    if corruption == "missing":
        del api.storage.data[key]
    elif corruption == "oversize":
        api.storage.data[key] = b"x" * 10
    else:
        api.storage.data[key] = b"bad!"
    response = api.client.get("/files/sample.bin")
    assert response.status_code == 200
    assert response.content == b"abcd"
    assert api.count(Replica) == 2


@pytest.mark.parametrize("failure", ["missing", "corrupt", "offline"])
def test_unavailable_chunk_returns_503_before_success_response(api, failure):
    api.nodes()
    api.upload(b"abcdefgh")
    if failure == "missing":
        api.storage.data.clear()
    elif failure == "corrupt":
        for key in api.storage.data:
            api.storage.data[key] = b"bad!"
    else:

        def offline(request):
            if request.method == "GET":
                raise httpx.ConnectError("offline", request=request)

        api.storage.failure = offline
    response = api.client.get("/files/sample.bin")
    assert response.status_code == 503
    assert response.headers["content-type"].startswith("application/json")


def test_missing_later_chunk_does_not_return_a_partial_success_response(api):
    api.nodes()
    api.upload(b"abcdefgh")
    for key in list(api.storage.data):
        if api.storage.data[key] == b"efgh":
            del api.storage.data[key]
    response = api.client.get("/files/sample.bin")
    assert response.status_code == 503
    assert response.headers["content-type"].startswith("application/json")
    assert "detail" in response.json()


def test_transient_read_failure_keeps_replica_metadata_for_retry(api):
    api.nodes()
    api.upload(b"abcd")

    def temporarily_unavailable(request):
        if request.method == "GET" and request.url.host == "node1":
            return httpx.Response(503)

    api.storage.failure = temporarily_unavailable
    assert api.client.get("/files/sample.bin").content == b"abcd"
    assert api.count(Replica) == 3
    api.storage.failure = None
    assert api.client.get("/files/sample.bin").content == b"abcd"


@pytest.mark.parametrize(
    "receipt",
    [
        {},
        [],
        {"chunk_id": "wrong", "checksum": "0" * 64, "size": 4},
        "invalid-json",
    ],
)
def test_upload_does_not_trust_bad_write_receipts(api, receipt):
    api.nodes(4)

    def invalid_receipt(request):
        if request.method == "PUT" and request.url.host == "node1":
            if receipt == "invalid-json":
                return httpx.Response(200, content=b"this is not JSON")
            return httpx.Response(200, json=receipt)

    api.storage.failure = invalid_receipt
    response = api.upload(b"abcd")
    assert response.status_code == 201
    assert response.json()["min_replicas"] == 3
    with api.sessions() as db:
        assert {replica.node_id for replica in db.query(Replica)} == {"node2", "node3", "node4"}


def test_download_temporary_disk_failure_is_controlled(api, monkeypatch):
    api.nodes()
    api.upload(b"abcd")

    def disk_full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "no temporary disk space")

    monkeypatch.setattr(metadata.tempfile, "TemporaryFile", disk_full)
    assert api.client.get("/files/sample.bin").status_code == 507


@pytest.mark.parametrize("field,value", [("checksum", "0" * 64), ("size", 999)])
def test_download_verifies_whole_file_manifest(api, field, value):
    api.nodes()
    api.upload(b"abcdefgh")
    with api.sessions() as db:
        record = db.query(File).one()
        setattr(record, field, value)
        db.commit()
    assert api.client.get("/files/sample.bin").status_code == 503


def test_stale_nodes_are_excluded_before_background_monitor_runs(api):
    api.nodes()
    api.upload(b"abcd")
    with api.sessions() as db:
        for node in db.query(Node):
            node.last_heartbeat = utcnow() - timedelta(days=1)
        db.commit()
    assert api.upload(b"new", name="new.bin").status_code == 503
    assert api.client.get("/files/sample.bin").status_code == 503
    assert api.client.get("/ready").status_code == 503


def test_recovering_node_is_excluded_from_reads_and_uploads(api):
    api.nodes(1)
    api.upload(b"abcd")
    with api.sessions() as db:
        db.query(Node).one().status = "RECOVERING"
        db.commit()
    assert api.upload(b"new", name="new.bin").status_code == 503
    assert api.client.get("/files/sample.bin").status_code == 503


def test_new_heartbeat_refreshes_telemetry(api):
    assert api.heartbeat().status_code == 200
    assert api.heartbeat(chunk_count=7, free_bytes=9000).status_code == 200
    nodes = api.client.get("/nodes").json()
    assert len(nodes) == 1
    assert nodes[0]["status"] == "ALIVE"
    assert nodes[0]["chunk_count"] == 7
    assert nodes[0]["free_bytes"] == 9000


def test_heartbeat_address_cannot_change_for_existing_node(api):
    assert api.heartbeat().status_code == 200
    assert api.heartbeat(url="http://other:8001").status_code == 409
    assert api.client.get("/nodes").json()[0]["url"] == "http://node1:8001"


@pytest.mark.parametrize(
    "first,second,canonical",
    [
        ("http://node1:8001", "http://node1:8001", "http://node1:8001"),
        ("http://HOST:80/", "http://host", "http://host"),
        ("https://HOST:443/", "https://host", "https://host"),
    ],
)
def test_distinct_node_ids_cannot_register_the_same_storage_endpoint(api, first, second, canonical):
    assert api.heartbeat("node1", url=first).status_code == 200
    assert api.heartbeat("node2", url=second).status_code == 409
    nodes = api.client.get("/nodes").json()
    assert len(nodes) == 1
    assert nodes[0]["id"] == "node1"
    assert nodes[0]["url"] == canonical


def test_same_node_can_heartbeat_with_equivalent_origin_spelling(api):
    assert api.heartbeat(url="http://HOST:80/").status_code == 200
    assert api.heartbeat(url="http://host").status_code == 200
    nodes = api.client.get("/nodes").json()
    assert len(nodes) == 1
    assert nodes[0]["url"] == "http://host"


def test_restarted_live_node_is_quarantined_before_its_existing_claims_are_trusted(api):
    api.nodes(1)
    assert api.upload(b"abcd").status_code == 201
    assert api.count(Replica) == 1
    assert api.heartbeat(restarted=True).status_code == 200
    assert api.client.get("/nodes").json()[0]["status"] == "RECOVERING"
    assert api.count(Replica) == 1
    assert api.client.get("/files/sample.bin").status_code == 503
    assert api.heartbeat(restarted=False).status_code == 200
    assert api.client.get("/nodes").json()[0]["status"] == "RECOVERING"


def test_restarted_first_registration_can_join_alive_without_prior_claims(api):
    assert api.heartbeat(restarted=True).status_code == 200
    assert api.client.get("/nodes").json()[0]["status"] == "ALIVE"
    assert api.count(Replica) == 0


def test_concurrent_node_registrations_cannot_inflate_replication_with_one_endpoint(api):
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(
            pool.map(
                lambda number: api.heartbeat(f"node{number}", url="http://one-server:8001"),
                range(8),
            )
        )
    assert [response.status_code for response in responses].count(200) == 1
    assert [response.status_code for response in responses].count(409) == 7
    assert api.count(Node) == 1


@pytest.mark.parametrize("status", ["DEAD", "RECOVERING"])
def test_returning_heartbeat_cannot_skip_inventory_verification(api, status):
    assert api.heartbeat().status_code == 200
    with api.sessions() as db:
        node = db.get(Node, "node1")
        node.status = status
        db.commit()
    assert api.heartbeat().status_code == 200
    assert api.client.get("/nodes").json()[0]["status"] == "RECOVERING"


def test_heartbeat_marks_stale_node_recovering_even_before_monitor(api):
    assert api.heartbeat().status_code == 200
    with api.sessions() as db:
        db.get(Node, "node1").last_heartbeat = utcnow() - timedelta(days=1)
        db.commit()
    assert api.heartbeat().status_code == 200
    assert api.client.get("/nodes").json()[0]["status"] == "RECOVERING"


@pytest.mark.parametrize(
    "overrides",
    [
        {"node_id": "../bad"},
        {"node_id": "é"},
        {"node_id": ""},
        {"node_id": "x" * 65},
        {"url": "file:///etc/passwd"},
        {"url": "http://user:secret@node1"},
        {"url": "http://node1/path"},
        {"url": "http://node1?query"},
        {"url": "http://node1#fragment"},
        {"chunk_count": -1},
        {"free_bytes": -2},
    ],
)
def test_invalid_heartbeat_is_rejected(api, overrides):
    assert api.heartbeat(**overrides).status_code == 422
    assert api.count(Node) == 0


def test_health_and_readiness_have_distinct_meaning(api, monkeypatch):
    assert api.client.get("/health").status_code == 200
    assert api.client.get("/ready").status_code == 503
    api.nodes(1)
    assert api.client.get("/ready").status_code == 200
    monkeypatch.setattr(metadata, "MIN_WRITE_REPLICAS", 2)
    assert api.client.get("/ready").status_code == 503
    api.heartbeat("node2")
    assert api.client.get("/ready").status_code == 200


def test_health_and_readiness_report_database_failure(api, monkeypatch):
    class UnavailableSession(Session):
        def execute(self, *args, **kwargs):
            raise SQLAlchemyError("simulated unavailable database")

    monkeypatch.setattr(
        metadata,
        "SessionLocal",
        sessionmaker(
            bind=api.sessions.kw["bind"],
            class_=UnavailableSession,
            expire_on_commit=False,
        ),
    )
    assert api.client.get("/health").status_code == 503
    assert api.client.get("/ready").status_code == 503


def test_cluster_counts_logical_files_separately_from_versions(api):
    api.nodes()
    api.upload(b"one")
    api.upload(b"two")
    api.upload(b"other", name="other.bin")
    status = api.client.get("/cluster").json()
    assert status["files"] == 2
    assert status["versions"] == 3
    assert status["nodes_alive"] == 3
    assert status["under_replicated_chunks"] == 0
    assert status["unavailable_chunks"] == 0
    api.client.delete("/files/sample.bin")
    status = api.client.get("/cluster").json()
    assert status["files"] == 1
    assert status["versions"] == 3


def test_cluster_reports_chunks_with_zero_live_copies(api):
    api.nodes()
    api.upload(b"abcd")
    with api.sessions() as db:
        for node in db.query(Node):
            node.status = "DEAD"
        db.commit()
    status = api.client.get("/cluster").json()
    assert status["unavailable_chunks"] == 1
    assert status["under_replicated_chunks"] == 1
    assert status["nodes_dead"] == 3


def test_metrics_exposes_prometheus_text(api):
    response = api.client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "replicafs_" in response.text


@pytest.mark.parametrize(
    "path,method,kwargs",
    [
        ("/files", "get", {}),
        ("/files", "post", {"files": {"file": ("x", b"x")}}),
        ("/files/absent", "get", {}),
        ("/files/absent/versions", "get", {}),
        ("/files/absent", "delete", {}),
        ("/files/absent/restore", "post", {}),
        ("/nodes", "get", {}),
        ("/cluster", "get", {}),
        ("/metrics", "get", {}),
    ],
)
def test_api_routes_require_client_token(api, monkeypatch, path, method, kwargs):
    monkeypatch.setenv("API_TOKEN", "client-secret")
    response = getattr(api.client, method)(path, **kwargs)
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert (
        getattr(api.client, method)(path, headers={"Authorization": "Bearer wrong"}, **kwargs).status_code
        == 401
    )
    assert api.client.get("/health").status_code == 200


def test_api_and_cluster_tokens_have_separate_roles(api, monkeypatch):
    monkeypatch.setenv("API_TOKEN", "client-secret")
    monkeypatch.setenv("CLUSTER_TOKEN", "cluster-secret")
    heartbeat = {"node_id": "node1", "url": "http://node1:8001", "free_bytes": 10000}
    assert api.client.post("/nodes/heartbeat", json=heartbeat).status_code == 401
    assert (
        api.client.post(
            "/nodes/heartbeat", json=heartbeat, headers={"Authorization": "Bearer client-secret"}
        ).status_code
        == 401
    )
    assert (
        api.client.post(
            "/nodes/heartbeat", json=heartbeat, headers={"Authorization": "Bearer cluster-secret"}
        ).status_code
        == 200
    )
    headers = {"Authorization": "Bearer client-secret"}
    response = api.client.post("/files", files={"file": ("x", b"abcd")}, headers=headers)
    assert response.status_code == 201
    response = api.client.get("/files/x", headers=headers)
    assert response.status_code == 200
    assert response.content == b"abcd"
    assert api.client.get("/files", headers={"Authorization": "Bearer cluster-secret"}).status_code == 401
    assert all(
        request.headers["authorization"] == "Bearer cluster-secret" for request in api.storage.requests
    )
