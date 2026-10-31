#!/usr/bin/env python3
"""Run the learning cluster without Docker; Ctrl+C stops only its child servers."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=4, help="storage nodes (default: 4)")
    parser.add_argument(
        "--base-port", type=int, default=8000, help="metadata port; nodes use the following ports"
    )
    parser.add_argument(
        "--data-dir", type=Path, default=ROOT / "data" / "dev", help="persistent cluster data and logs"
    )
    args = parser.parse_args()
    if args.nodes < 1 or not 1024 <= args.base_port <= 65535 - args.nodes:
        parser.error("use at least one node and ports between 1024 and 65535")
    data = args.data_dir.resolve()
    data.mkdir(parents=True, exist_ok=True)
    logs = data / "logs"
    logs.mkdir(exist_ok=True)
    base = f"http://127.0.0.1:{args.base_port}"
    children: list[subprocess.Popen] = []
    handles = []
    stop = False

    def request_stop(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    def start(name, module, port, overrides):
        logfile = (logs / f"{name}.log").open("a")
        handles.append(logfile)
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                module,
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--workers",
                "1",
            ],
            cwd=ROOT,
            env={**os.environ, **overrides},
            stdout=logfile,
            stderr=subprocess.STDOUT,
        )
        children.append(process)

    def get(path):
        token = os.environ.get("API_TOKEN", "")
        headers = {"Authorization": "Bearer " + token} if token else {}
        with urllib.request.urlopen(
            urllib.request.Request(base + path, headers=headers), timeout=2
        ) as response:
            return json.load(response)

    try:
        start(
            "metadata",
            "replicafs.metadata.app:app",
            args.base_port,
            {
                "DATABASE_URL": "sqlite:///" + str(data / "metadata.db"),
                "CHUNK_SIZE": os.environ.get("CHUNK_SIZE", "1048576"),
            },
        )
        for i in range(1, args.nodes + 1):
            port = args.base_port + i
            start(
                f"node{i}",
                "replicafs.storage.app:app",
                port,
                {
                    "NODE_ID": f"node{i}",
                    "NODE_URL": f"http://127.0.0.1:{port}",
                    "METADATA_URL": base,
                    "DATA_DIR": str(data / f"node{i}"),
                },
            )
        deadline = time.monotonic() + 40
        while not stop:
            if any(p.poll() is not None for p in children):
                raise RuntimeError(f"A server exited. Read its log in {logs}; check for occupied ports.")
            try:
                nodes = get("/nodes")
                if sum(n["status"] == "ALIVE" for n in nodes) >= args.nodes:
                    break
            except (OSError, ValueError, urllib.error.URLError):
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Cluster did not become ready. Read logs in {logs}.")
            time.sleep(0.2)
        if not stop:
            print(f"ReplicaFS ready: {base}/docs", flush=True)
            print(f"Data and logs: {data}\nPress Ctrl+C to stop; data stays on disk.", flush=True)
        while not stop:
            if any(p.poll() is not None for p in children):
                raise RuntimeError(f"A server exited. Read logs in {logs}.")
            time.sleep(0.3)
        return 0
    except (OSError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        for process in children:
            if process.poll() is None:
                process.terminate()
        for process in children:
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
