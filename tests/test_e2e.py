"""End-to-end tests against a real local cluster (1 metadata server + 4 storage
nodes, each a separate uvicorn process). No Docker needed.

Timeouts are tuned down (heartbeat 1s, dead after 3s) so the whole file runs
in well under a minute.
"""
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
CHUNK_SIZE = 256 * 1024


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(predicate, timeout: float, what: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for {what}")


class Cluster:
    def __init__(self, tmp_path: Path, nodes: int = 4):
        self.tmp = tmp_path
        self.procs: dict[str, subprocess.Popen] = {}
        self.node_env: dict[str, dict] = {}
        self.base = f"http://127.0.0.1:{free_port()}"

        self.procs["metadata"] = self._spawn(
            "replicafs.metadata.app:app",
            self.base,
            {
                "DATABASE_URL": f"sqlite:///{tmp_path}/meta.db",
                "CHUNK_SIZE": str(CHUNK_SIZE),
                "REPLICATION_FACTOR": "3",
                "DEAD_AFTER_SECONDS": "3",
                "MONITOR_INTERVAL": "1",
            },
        )
        wait_for(lambda: httpx.get(f"{self.base}/nodes", timeout=1).status_code == 200, 20, "metadata server")

        for i in range(1, nodes + 1):
            name = f"node{i}"
            url = f"http://127.0.0.1:{free_port()}"
            self.node_env[name] = {
                "NODE_ID": name,
                "NODE_URL": url,
                "METADATA_URL": self.base,
                "DATA_DIR": str(tmp_path / name),
                "HEARTBEAT_INTERVAL": "1",
            }
            self.start_node(name)
        wait_for(lambda: self.alive() == nodes, 20, "all nodes alive")

    def _spawn(self, module: str, url: str, env: dict) -> subprocess.Popen:
        port = url.rsplit(":", 1)[1]
        return subprocess.Popen(
            [sys.executable, "-m", "uvicorn", module, "--host", "127.0.0.1", "--port", port, "--log-level", "warning"],
            cwd=ROOT,
            env={**os.environ, **env},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def start_node(self, name: str):
        env = self.node_env[name]
        self.procs[name] = self._spawn("replicafs.storage.app:app", env["NODE_URL"], env)

    def kill_node(self, name: str):
        p = self.procs.pop(name)
        p.kill()
        p.wait()

    def node_data_dir(self, name: str) -> Path:
        return Path(self.node_env[name]["DATA_DIR"])

    # --- queries
    def nodes(self) -> dict[str, str]:
        return {n["id"]: n["status"] for n in httpx.get(f"{self.base}/nodes", timeout=2).json()}

    def alive(self) -> int:
        return sum(1 for s in self.nodes().values() if s == "ALIVE")

    def cluster(self) -> dict:
        return httpx.get(f"{self.base}/cluster", timeout=2).json()

    def upload(self, name: str, data: bytes) -> dict:
        r = httpx.post(f"{self.base}/files", files={"file": (name, data)}, timeout=60)
        assert r.status_code == 201, r.text
        return r.json()

    def download(self, name: str, **params) -> httpx.Response:
        return httpx.get(f"{self.base}/files/{name}", params=params, timeout=60)

    def shutdown(self):
        for p in self.procs.values():
            p.kill()
        for p in self.procs.values():
            p.wait()


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path)
    try:
        yield c
    finally:
        c.shutdown()


def test_upload_is_chunked_and_fully_replicated(cluster):
    data = os.urandom(CHUNK_SIZE * 3 + 777)  # 4 chunks, last one partial
    info = cluster.upload("big.bin", data)
    assert info["chunks"] == 4
    assert info["size"] == len(data)

    status = cluster.cluster()
    assert status["chunks"] == 4
    assert status["replicas"] == 12  # 4 chunks x rf 3
    assert status["under_replicated_chunks"] == 0

    assert cluster.download("big.bin").content == data


def test_download_survives_node_failure_and_cluster_self_heals(cluster):
    data = os.urandom(CHUNK_SIZE * 3 + 1)
    cluster.upload("big.bin", data)

    # Find a node that actually holds chunks (with 4 nodes and rf=3, one is idle).
    victim = next(n for n in cluster.node_env if any(cluster.node_data_dir(n).iterdir()))
    cluster.kill_node(victim)
    wait_for(lambda: cluster.nodes()[victim] == "DEAD", 10, f"{victim} marked DEAD")

    # Reads keep working from the surviving replicas.
    assert cluster.download("big.bin").content == data

    # The re-replicator copies the lost chunks onto the spare node.
    wait_for(lambda: cluster.cluster()["under_replicated_chunks"] == 0, 15, "re-replication")
    assert cluster.download("big.bin").content == data

    # Victim comes back: it's ALIVE again and its old replicas simply count as extras.
    cluster.start_node(victim)
    wait_for(lambda: cluster.nodes()[victim] == "ALIVE", 10, f"{victim} back ALIVE")


def test_corrupted_replica_is_detected_and_bypassed(cluster):
    data = os.urandom(CHUNK_SIZE + 5)  # 2 chunks
    cluster.upload("doc.bin", data)

    # Flip bytes in one on-disk chunk on one node.
    chunk_file = next(p for n in cluster.node_env for p in cluster.node_data_dir(n).iterdir())
    original = chunk_file.read_bytes()
    chunk_file.write_bytes(b"\x00" * 16 + original[16:])

    # Download still returns correct bytes (served from a good replica)...
    assert cluster.download("doc.bin").content == data
    # ...and the bad replica is dropped from the index, then re-made by the re-replicator.
    wait_for(lambda: cluster.cluster()["under_replicated_chunks"] == 0, 15, "re-replication after corruption")
    assert cluster.cluster()["replicas"] == 6


def test_versioning_soft_delete_and_restore(cluster):
    cluster.upload("notes.txt", b"version one")
    cluster.upload("notes.txt", b"version two")

    assert cluster.download("notes.txt").content == b"version two"
    assert cluster.download("notes.txt", version=1).content == b"version one"
    assert [v["version"] for v in httpx.get(f"{cluster.base}/files/notes.txt/versions").json()] == [1, 2]

    r = httpx.delete(f"{cluster.base}/files/notes.txt")
    assert r.status_code == 200
    assert cluster.download("notes.txt").status_code == 404
    assert httpx.get(f"{cluster.base}/files").json() == []

    r = httpx.post(f"{cluster.base}/files/notes.txt/restore")
    assert r.status_code == 200
    assert cluster.download("notes.txt").content == b"version two"
