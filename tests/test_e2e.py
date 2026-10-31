"""Real-process acceptance tests for authenticated local ReplicaFS clusters.

Every test owns its database, four disks, reserved ports, processes, and logs.
No Docker or cloud account is required. Polling waits for actual state changes;
checks inspect both metadata and bytes, so a healthy counter alone cannot make a
corruption or recovery test pass.
"""

import hashlib
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
CHUNK_SIZE = 256 * 1024
MAX_FILE_BYTES = 2 * 1024 * 1024
API_TOKEN = "acceptance-client-token"
CLUSTER_TOKEN = "acceptance-internal-token"
CHUNK_ID = re.compile(r"[a-f0-9]{32}\Z")


def wait_for(predicate, timeout: float, what: str, log_dir: Path | None = None):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (httpx.HTTPError, sqlite3.OperationalError, FileNotFoundError) as exc:
            last_error = exc
        time.sleep(0.1)
    detail = f"; last error: {last_error}" if last_error else ""
    logs = f"; service logs: {log_dir}" if log_dir else ""
    raise TimeoutError(f"timed out waiting for {what}{detail}{logs}")


class Cluster:
    def __init__(self, tmp_path: Path, nodes: int = 4, **metadata_overrides):
        self.tmp = tmp_path
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.logs = self.tmp / "logs"
        self.logs.mkdir()
        self.procs: dict[str, subprocess.Popen] = {}
        self.node_env: dict[str, dict] = {}
        self.reserved: dict[str, socket.socket] = {}
        self.base = self.reserve_url()
        self.database = tmp_path / "meta.db"
        self.metadata_env = {
            "DATABASE_URL": f"sqlite:///{self.database}",
            "CHUNK_SIZE": str(CHUNK_SIZE),
            "REPLICATION_FACTOR": "3",
            "MIN_WRITE_REPLICAS": "1",
            "MAX_FILE_BYTES": str(MAX_FILE_BYTES),
            "DEAD_AFTER_SECONDS": "3",
            "MONITOR_INTERVAL": "0.5",
            "SCRUB_INTERVAL": "1",
            "SCRUB_BATCH_SIZE": "100",
            "API_TOKEN": API_TOKEN,
            "CLUSTER_TOKEN": CLUSTER_TOKEN,
            **metadata_overrides,
        }
        for index in range(1, nodes + 1):
            name = f"node{index}"
            self.node_env[name] = {
                "NODE_ID": name,
                "NODE_URL": self.reserve_url(),
                "METADATA_URL": self.base,
                "DATA_DIR": str(tmp_path / name),
                "HEARTBEAT_INTERVAL": "0.5",
                "MAX_CHUNK_BYTES": str(CHUNK_SIZE),
                "CLUSTER_TOKEN": CLUSTER_TOKEN,
            }
        self.api = httpx.Client(
            base_url=self.base,
            headers={"Authorization": f"Bearer {API_TOKEN}"},
            timeout=30,
            trust_env=False,
        )
        try:
            self.start_metadata()
            for name in self.node_env:
                self.start_node(name)
            self.wait(lambda: self.alive() == nodes, 20, "all nodes ALIVE")
        except BaseException:
            self.shutdown()
            raise

    def reserve_url(self):
        # Hold each port until its process is started. This prevents duplicate
        # choices within a cluster and keeps the release-to-bind race very short.
        reservation = socket.socket()
        reservation.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{reservation.getsockname()[1]}"
        self.reserved[url] = reservation
        return url

    def _spawn(self, name: str, module: str, url: str, env: dict):
        if name in self.procs:
            raise RuntimeError(f"{name} already has a process")
        reservation = self.reserved.pop(url, None)
        if reservation is not None:
            reservation.close()
        with (self.logs / f"{name}.log").open("ab") as log:
            self.procs[name] = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    module,
                    "--host",
                    "127.0.0.1",
                    "--port",
                    url.rsplit(":", 1)[1],
                    "--workers",
                    "1",
                    "--log-level",
                    "warning",
                ],
                cwd=ROOT,
                env={**os.environ, **env},
                stdout=log,
                stderr=subprocess.STDOUT,
            )

    def wait(self, predicate, timeout: float, what: str):
        def checked():
            for name, process in self.procs.items():
                if process.poll() is not None:
                    raise RuntimeError(
                        f"{name} exited with {process.returncode}; see {self.logs / (name + '.log')}"
                    )
            return predicate()

        wait_for(checked, timeout, what, self.logs)

    def start_metadata(self):
        self._spawn("metadata", "replicafs.metadata.app:app", self.base, self.metadata_env)
        self.wait(
            lambda: self.request("GET", "/health", timeout=2).status_code == 200, 20, "metadata readiness"
        )

    def restart_metadata(self, **overrides):
        self.kill_service("metadata")
        self.metadata_env.update(overrides)
        self.start_metadata()
        expected = sum(name in self.procs for name in self.node_env)
        self.wait(lambda: self.alive() == expected, 20, "nodes after metadata restart")

    def start_node(self, name: str):
        env = self.node_env[name]
        self._spawn(name, "replicafs.storage.app:app", env["NODE_URL"], env)

    def kill_service(self, name: str):
        process = self.procs.pop(name)
        process.kill()
        process.wait(timeout=10)

    def kill_node(self, name: str):
        self.kill_service(name)

    def node_data_dir(self, name: str) -> Path:
        return Path(self.node_env[name]["DATA_DIR"])

    def local_chunks(self, name: str):
        return sorted(
            path
            for path in self.node_data_dir(name).iterdir()
            if path.is_file() and CHUNK_ID.fullmatch(path.name)
        )

    def request(self, method: str, path: str, **kwargs):
        return self.api.request(method, path, **kwargs)

    def nodes(self) -> dict[str, str]:
        response = self.request("GET", "/nodes", timeout=5)
        response.raise_for_status()
        return {node["id"]: node["status"] for node in response.json()}

    def alive(self) -> int:
        return sum(status == "ALIVE" for status in self.nodes().values())

    def cluster(self) -> dict:
        response = self.request("GET", "/cluster", timeout=5)
        response.raise_for_status()
        return response.json()

    def upload(self, name: str, data: bytes) -> dict:
        response = self.request("POST", "/files", files={"file": (name, data)}, timeout=60)
        assert response.status_code == 201, f"{response.text}; logs: {self.logs}"
        return response.json()

    def download(self, name: str, **params) -> httpx.Response:
        return self.request("GET", f"/files/{quote(name, safe='')}", params=params, timeout=60)

    def replicas(self, name: str):
        """Read committed placements directly, without downloading or repairing."""
        with closing(sqlite3.connect(self.database, timeout=5)) as connection:
            return connection.execute(
                "SELECT c.id, c.checksum, c.size, r.node_id, n.status "
                "FROM chunks c JOIN files f ON f.id = c.file_id "
                "JOIN replicas r ON r.chunk_id = c.id JOIN nodes n ON n.id = r.node_id "
                'WHERE f.name = ? ORDER BY c."index", r.node_id',
                (name,),
            ).fetchall()

    def replicas_valid(self, name: str, minimum: int = 3):
        """Require actual healthy bytes for every indexed live placement."""
        with closing(sqlite3.connect(self.database, timeout=5)) as connection:
            expected_chunks = connection.execute(
                "SELECT COUNT(*) FROM chunks c JOIN files f ON f.id = c.file_id WHERE f.name = ?",
                (name,),
            ).fetchone()[0]
        counts = {}
        for chunk_id, checksum, size, node, status in self.replicas(name):
            if status != "ALIVE":
                continue
            path = self.node_data_dir(node) / chunk_id
            if not path.exists():
                return False
            data = path.read_bytes()
            if len(data) != size or hashlib.sha256(data).hexdigest() != checksum:
                return False
            counts[chunk_id] = counts.get(chunk_id, 0) + 1
        return len(counts) == expected_chunks and all(count >= minimum for count in counts.values())

    def corrupt_first_replica(self, name: str):
        chunk_id, _, _, node, _ = self.replicas(name)[0]
        path = self.node_data_dir(node) / chunk_id
        original = path.read_bytes()
        path.write_bytes(bytes([original[0] ^ 0xFF]) + original[1:])
        return path

    def shutdown(self):
        for reservation in self.reserved.values():
            reservation.close()
        self.reserved.clear()
        # Signal all children first, then collect them. Cleanup also runs after
        # startup errors; failed tests never leave background clusters behind.
        for process in self.procs.values():
            if process.poll() is None:
                process.terminate()
        for process in self.procs.values():
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self.procs.clear()
        self.api.close()


@pytest.fixture
def cluster(tmp_path):
    # Pytest displays this captured line on failure, making child logs discoverable.
    print(f"Cluster service logs: {tmp_path / 'logs'}")
    instance = Cluster(tmp_path)
    try:
        yield instance
    finally:
        instance.shutdown()


def test_upload_is_chunked_and_fully_replicated(cluster):
    data = os.urandom(CHUNK_SIZE * 3 + 777)
    info = cluster.upload("big.bin", data)
    assert info["chunks"] == 4
    assert info["size"] == len(data)
    assert info["checksum"] == hashlib.sha256(data).hexdigest()
    status = cluster.cluster()
    assert status["chunks"] == 4
    assert status["replicas"] == 12
    assert status["under_replicated_chunks"] == 0
    assert cluster.replicas_valid("big.bin")
    assert cluster.download("big.bin").content == data


def test_download_survives_node_failure_and_cluster_self_heals(cluster):
    data = os.urandom(CHUNK_SIZE * 3 + 1)
    cluster.upload("big.bin", data)
    victim = cluster.replicas("big.bin")[0][3]
    cluster.kill_node(victim)
    cluster.wait(lambda: cluster.nodes()[victim] == "DEAD", 10, "victim marked DEAD")
    assert cluster.download("big.bin").content == data
    cluster.wait(lambda: cluster.replicas_valid("big.bin"), 15, "verified repair onto spare")
    assert cluster.cluster()["under_replicated_chunks"] == 0
    cluster.start_node(victim)
    cluster.wait(lambda: cluster.nodes()[victim] == "ALIVE", 15, "returning node revalidated")
    assert cluster.replicas_valid("big.bin")
    assert cluster.download("big.bin").content == data


def test_corrupted_replica_is_detected_and_bypassed(cluster):
    cluster.restart_metadata(SCRUB_INTERVAL="3600")
    data = os.urandom(CHUNK_SIZE + 5)
    cluster.upload("doc.bin", data)
    cluster.corrupt_first_replica("doc.bin")
    assert cluster.download("doc.bin").content == data
    cluster.wait(lambda: cluster.replicas_valid("doc.bin"), 15, "actual valid repaired replicas")
    assert cluster.cluster()["under_replicated_chunks"] == 0


def test_versioning_soft_delete_and_restore(cluster):
    cluster.upload("notes.txt", b"version one")
    cluster.upload("notes.txt", b"version two")
    assert cluster.download("notes.txt").content == b"version two"
    assert cluster.download("notes.txt", version=1).content == b"version one"
    assert [version["version"] for version in cluster.request("GET", "/files/notes.txt/versions").json()] == [
        1,
        2,
    ]
    assert cluster.request("DELETE", "/files/notes.txt").status_code == 200
    assert cluster.download("notes.txt").status_code == 404
    assert cluster.request("GET", "/files").json() == []
    assert cluster.request("POST", "/files/notes.txt/restore").status_code == 200
    assert cluster.download("notes.txt").content == b"version two"
    assert cluster.download("notes.txt", version=1).content == b"version one"


def test_empty_exact_boundary_and_binary_payloads(cluster):
    cases = [
        ("empty.bin", b"", 0),
        ("exact.bin", os.urandom(CHUNK_SIZE * 2), 2),
        ("binary.bin", bytes(range(256)) * 1031, 2),
    ]
    for name, data, expected_chunks in cases:
        info = cluster.upload(name, data)
        assert info["chunks"] == expected_chunks
        assert info["size"] == len(data)
        assert info["checksum"] == hashlib.sha256(data).hexdigest()
        response = cluster.download(name)
        assert response.status_code == 200
        assert response.content == data
        assert response.headers["content-length"] == str(len(data))


def test_unicode_and_spaces_in_filename(cluster):
    name = "தமிழ் notes ☃.bin"
    cluster.upload(name, b"unicode names, unchanged content")
    response = cluster.download(name)
    assert response.status_code == 200
    assert response.content == b"unicode names, unchanged content"
    assert "filename*=utf-8''" in response.headers["content-disposition"].lower()
    assert cluster.request("DELETE", f"/files/{quote(name, safe='')}").status_code == 200
    assert cluster.download(name).status_code == 404
    assert cluster.request("POST", f"/files/{quote(name, safe='')}/restore").status_code == 200
    assert cluster.download(name).status_code == 200


def test_missing_invalid_version_and_oversized_requests(cluster):
    for method, path in [
        ("GET", "/files/missing"),
        ("GET", "/files/missing/versions"),
        ("DELETE", "/files/missing"),
        ("POST", "/files/missing/restore"),
    ]:
        assert cluster.request(method, path).status_code == 404
    cluster.upload("versioned.bin", b"data")
    for value in (0, -1, "not-a-number"):
        assert cluster.download("versioned.bin", version=value).status_code == 422
    assert cluster.download("versioned.bin", version=999).status_code == 404
    response = cluster.request(
        "POST", "/files", files={"file": ("oversized.bin", b"x" * (MAX_FILE_BYTES + 1))}
    )
    assert response.status_code == 413
    assert cluster.download("oversized.bin").status_code == 404
    assert [entry["name"] for entry in cluster.request("GET", "/files").json()] == ["versioned.bin"]


def test_tokens_protect_client_and_internal_routes(cluster):
    with httpx.Client(trust_env=False) as outsider:
        assert outsider.get(f"{cluster.base}/health").status_code == 200
        assert outsider.get(f"{cluster.base}/files").status_code == 401
        assert outsider.get(f"{cluster.base}/cluster").status_code == 401
        assert (
            outsider.post(
                f"{cluster.base}/nodes/heartbeat", json={"node_id": "fake", "url": "http://fake:8001"}
            ).status_code
            == 401
        )
        node = cluster.node_env["node1"]["NODE_URL"]
        assert outsider.get(f"{node}/health").status_code == 200
        chunk = "a" * 32
        assert outsider.get(f"{node}/chunks/{chunk}").status_code == 401
        assert (
            outsider.get(
                f"{node}/chunks/{chunk}", headers={"Authorization": f"Bearer {API_TOKEN}"}
            ).status_code
            == 401
        )
        assert (
            outsider.get(
                f"{node}/chunks/{chunk}", headers={"Authorization": f"Bearer {CLUSTER_TOKEN}"}
            ).status_code
            == 404
        )
    assert len(cluster.nodes()) == 4


def test_concurrent_same_name_uploads_have_unique_consecutive_versions(cluster):
    payloads = [f"writer {index}".encode() * 200 for index in range(6)]
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda data: cluster.upload("race.bin", data), payloads))
    assert sorted(info["version"] for info in results) == list(range(1, 7))
    for info, data in zip(results, payloads):
        assert cluster.download("race.bin", version=info["version"]).content == data
    versions = cluster.request("GET", "/files/race.bin/versions").json()
    assert [entry["version"] for entry in versions] == list(range(1, 7))


def test_metadata_restart_preserves_files_versions_and_deletion(cluster):
    cluster.upload("persist.bin", b"first")
    cluster.upload("persist.bin", b"second")
    cluster.upload("deleted.bin", b"recover later")
    assert cluster.request("DELETE", "/files/deleted.bin").status_code == 200
    cluster.restart_metadata()
    assert cluster.download("persist.bin").content == b"second"
    assert cluster.download("persist.bin", version=1).content == b"first"
    assert cluster.download("deleted.bin").status_code == 404
    assert cluster.request("POST", "/files/deleted.bin/restore").status_code == 200
    assert cluster.download("deleted.bin").content == b"recover later"


def test_returning_node_with_lost_disk_never_resurrects_false_claims(cluster):
    data = os.urandom(CHUNK_SIZE + 1)
    cluster.upload("lost-disk.bin", data)
    victim = cluster.replicas("lost-disk.bin")[0][3]
    cluster.kill_node(victim)
    cluster.wait(lambda: cluster.nodes()[victim] == "DEAD", 10, "disk-loss victim DEAD")
    shutil.rmtree(cluster.node_data_dir(victim))
    cluster.start_node(victim)
    cluster.wait(lambda: cluster.nodes()[victim] == "ALIVE", 15, "empty returning disk revalidated")
    cluster.wait(lambda: cluster.replicas_valid("lost-disk.bin"), 15, "retained claims verified or removed")
    assert cluster.download("lost-disk.bin").content == data
    assert cluster.cluster()["under_replicated_chunks"] == 0


def test_complete_cluster_outage_recovers_preserved_disks(cluster):
    data = os.urandom(CHUNK_SIZE + 1)
    cluster.upload("outage.bin", data)
    for name in cluster.node_env:
        cluster.kill_node(name)
    cluster.wait(
        lambda: all(state == "DEAD" for state in cluster.nodes().values()), 10, "all storage nodes DEAD"
    )
    assert cluster.cluster()["unavailable_chunks"] == 2
    assert cluster.download("outage.bin").status_code == 503
    assert (
        cluster.request(
            "POST", "/files", files={"file": ("unavailable.bin", b"no writable nodes")}
        ).status_code
        == 503
    )
    cluster.kill_service("metadata")
    cluster.start_metadata()
    for name in cluster.node_env:
        cluster.start_node(name)
    cluster.wait(lambda: cluster.alive() == 4, 20, "all returning disks revalidated")
    cluster.wait(lambda: cluster.replicas_valid("outage.bin"), 15, "full replication after complete restart")
    assert cluster.download("outage.bin").content == data
    assert cluster.cluster()["unavailable_chunks"] == 0


def test_periodic_scrub_repairs_corruption_without_a_download(cluster):
    data = os.urandom(CHUNK_SIZE)
    cluster.upload("scrub.bin", data)
    cluster.corrupt_first_replica("scrub.bin")
    # This poll reads disk/SQLite directly. No download triggers corruption
    # detection; only the monitor's checksum scrub can make this become true.
    cluster.wait(lambda: cluster.replicas_valid("scrub.bin"), 15, "background scrub and actual-byte repair")
    assert cluster.cluster()["under_replicated_chunks"] == 0
    assert cluster.download("scrub.bin").content == data


def test_all_copies_corrupt_returns_json_503_before_streaming(cluster):
    cluster.restart_metadata(SCRUB_INTERVAL="3600")
    cluster.upload("unrecoverable.bin", b"one chunk with three copies")
    for chunk_id, _, _, node, _ in cluster.replicas("unrecoverable.bin"):
        (cluster.node_data_dir(node) / chunk_id).write_bytes(b"corrupt")
    response = cluster.download("unrecoverable.bin")
    assert response.status_code == 503
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["detail"]
    assert cluster.cluster()["unavailable_chunks"] == 1
    assert cluster.cluster()["under_replicated_chunks"] == 1


def test_read_falls_back_before_failed_node_heartbeat_expires(cluster):
    data = os.urandom(CHUNK_SIZE)
    cluster.upload("fast-fallback.bin", data)
    victim = cluster.replicas("fast-fallback.bin")[0][3]
    cluster.kill_node(victim)
    assert cluster.nodes()[victim] == "ALIVE"
    assert cluster.download("fast-fallback.bin").content == data


def test_no_spare_node_reports_under_replication_and_accepts_degraded_writes(cluster):
    data = b"still available with two surviving copies"
    cluster.upload("degraded.bin", data)
    # Placement initially selects node1..node3. Kill the third copy and idle
    # spare, leaving exactly two machines: no repair can manufacture a third.
    for name in ("node3", "node4"):
        cluster.kill_node(name)
    cluster.wait(lambda: cluster.alive() == 2, 10, "only two live storage nodes")
    assert cluster.download("degraded.bin").content == data
    status = cluster.cluster()
    assert status["under_replicated_chunks"] == 1
    assert status["unavailable_chunks"] == 0
    info = cluster.upload("degraded-write.bin", b"default minimum is one copy")
    assert info["version"] == 1
    assert cluster.download("degraded-write.bin").content == b"default minimum is one copy"
    assert cluster.cluster()["under_replicated_chunks"] == 2


def test_strict_minimum_write_policy_rejects_insufficient_copies(cluster):
    for name in ("node3", "node4"):
        cluster.kill_node(name)
    cluster.wait(lambda: cluster.alive() == 2, 10, "only two live nodes before strict upload")
    cluster.restart_metadata(MIN_WRITE_REPLICAS="3")
    response = cluster.request("POST", "/files", files={"file": ("strict.bin", b"must have three copies")})
    assert response.status_code == 503
    assert cluster.download("strict.bin").status_code == 404
    assert cluster.request("GET", "/files").json() == []
    assert cluster.cluster()["chunks"] == 0
    assert cluster.cluster()["replicas"] == 0
    assert all(not cluster.local_chunks(name) for name in ("node1", "node2"))


def test_fast_restart_with_lost_disk_revalidates_before_failure_timeout(cluster):
    # Disable the regular scrub and keep the failure deadline far away. No read
    # is made until the placements contain real bytes again, so only the node's
    # first restarted=True heartbeat can trigger the required revalidation.
    cluster.restart_metadata(DEAD_AFTER_SECONDS="30", SCRUB_INTERVAL="3600")
    data = os.urandom(CHUNK_SIZE + 1)
    cluster.upload("fast-restart.bin", data)
    victim = cluster.replicas("fast-restart.bin")[0][3]
    started = time.monotonic()
    cluster.kill_node(victim)
    shutil.rmtree(cluster.node_data_dir(victim))
    assert cluster.nodes()[victim] == "ALIVE"
    cluster.start_node(victim)
    cluster.wait(
        lambda: cluster.nodes()[victim] == "ALIVE" and cluster.replicas_valid("fast-restart.bin"),
        15,
        "startup heartbeat revalidation and valid placements before timeout",
    )
    assert time.monotonic() - started < 30
    assert cluster.download("fast-restart.bin").content == data
    assert cluster.cluster()["under_replicated_chunks"] == 0
