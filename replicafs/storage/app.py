"""A disk-backed chunk server with bounded requests and atomic chunk writes.

Storage nodes know chunk ids and bytes, not filenames or versions. The metadata
server decides placement and supplies the expected SHA-256 checksum.
"""
import logging
import os
import re
import shutil
import tempfile
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, field_validator

from replicafs.common import sha256_hex
from replicafs.security import cluster_headers, require_cluster_token

NODE_ID = os.environ.get("NODE_ID", "node1")
NODE_URL = os.environ.get("NODE_URL", "http://localhost:8001")
METADATA_URL = os.environ.get("METADATA_URL", "http://localhost:8000")
DATA_DIR = Path(os.environ.get("DATA_DIR", f"./data/{NODE_ID}"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "3"))
MAX_CHUNK_BYTES = int(os.environ.get("MAX_CHUNK_BYTES", str(8 * 1024 * 1024)))
if HEARTBEAT_INTERVAL <= 0 or MAX_CHUNK_BYTES <= 0:
    raise ValueError("HEARTBEAT_INTERVAL and MAX_CHUNK_BYTES must be positive")

log = logging.getLogger(f"storage.{NODE_ID}")
CHUNK_ID = re.compile(r"[A-Za-z0-9]{1,128}\Z", re.ASCII)
CHECKSUM = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
INTERNAL = [Depends(require_cluster_token)]


def chunk_path(chunk_id: str) -> Path:
    """Accept plain ASCII ids only; never follow an existing symbolic link."""
    if not CHUNK_ID.fullmatch(chunk_id):
        raise HTTPException(status_code=400, detail="invalid chunk id")
    path = DATA_DIR / chunk_id
    if path.is_symlink():
        raise HTTPException(status_code=400, detail="chunk path must not be a symbolic link")
    return path


def write_chunk_atomically(path: Path, data: bytes) -> None:
    """Flush a unique temporary file, then replace the visible chunk in one step.

    A separate temporary filename for every request also makes concurrent repairs
    of the same chunk safe: a reader sees one complete copy, never half a copy.
    """
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        # Persist the rename as well as the file contents where supported.
        if hasattr(os, "O_DIRECTORY"):
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        tmp.unlink(missing_ok=True)


def local_chunk_count() -> int:
    return sum(
        1 for path in DATA_DIR.iterdir()
        if CHUNK_ID.fullmatch(path.name) and path.is_file() and not path.is_symlink()
    )


def heartbeat_loop(stop: threading.Event) -> None:
    """Keep retrying metadata outages and stop promptly during server shutdown."""
    while not stop.is_set():
        try:
            response = httpx.post(
                f"{METADATA_URL}/nodes/heartbeat",
                headers=cluster_headers(),
                json={
                    "node_id": NODE_ID,
                    "url": NODE_URL,
                    "chunk_count": local_chunk_count(),
                    "free_bytes": shutil.disk_usage(DATA_DIR).free,
                },
                timeout=2,
            )
            response.raise_for_status()
        except (httpx.HTTPError, OSError) as exc:
            log.warning("heartbeat failed: %s", exc)
        stop.wait(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    worker = threading.Thread(target=heartbeat_loop, args=(stop,), daemon=True)
    worker.start()
    log.info("storage node %s serving %s, reporting to %s", NODE_ID, DATA_DIR, METADATA_URL)
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=3)


app = FastAPI(title=f"ReplicaFS storage node {NODE_ID}", lifespan=lifespan)


@app.get("/health")
def health():
    try:
        return {"node_id": NODE_ID, "chunks": local_chunk_count()}
    except OSError as exc:
        log.warning("storage health check failed: %s", exc)
        raise HTTPException(status_code=503, detail="storage directory unavailable") from exc


@app.put("/chunks/{chunk_id}", dependencies=INTERNAL)
async def put_chunk(chunk_id: str, request: Request):
    path = chunk_path(chunk_id)
    expected = request.headers.get("x-checksum")
    if expected is not None and not CHECKSUM.fullmatch(expected):
        raise HTTPException(status_code=422, detail="x-checksum must be a lowercase SHA-256 digest")
    if "content-length" in request.headers:
        try:
            length = int(request.headers["content-length"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid content-length") from exc
        if length < 0:
            raise HTTPException(status_code=400, detail="invalid content-length")
        if length > MAX_CHUNK_BYTES:
            raise HTTPException(status_code=413, detail="chunk exceeds MAX_CHUNK_BYTES")
    data = bytearray()
    async for part in request.stream():
        if len(data) + len(part) > MAX_CHUNK_BYTES:
            raise HTTPException(status_code=413, detail="chunk exceeds MAX_CHUNK_BYTES")
        data.extend(part)
    actual = sha256_hex(data)
    if expected is not None and expected != actual:
        raise HTTPException(status_code=422, detail="checksum mismatch: chunk corrupted in transit")
    try:
        write_chunk_atomically(path, bytes(data))
    except OSError as exc:
        log.warning("could not write chunk %s: %s", chunk_id, exc)
        raise HTTPException(status_code=507, detail="could not write chunk to disk") from exc
    return {"chunk_id": chunk_id, "checksum": actual, "size": len(data)}


@app.get("/chunks/{chunk_id}", dependencies=INTERNAL)
def get_chunk(chunk_id: str):
    path = chunk_path(chunk_id)
    try:
        # Never read an unexpectedly large on-disk file into memory.
        with path.open("rb") as handle:
            data = handle.read(MAX_CHUNK_BYTES + 1)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="chunk not on this node") from exc
    except OSError as exc:
        raise HTTPException(status_code=507, detail="could not read chunk from disk") from exc
    if len(data) > MAX_CHUNK_BYTES:
        raise HTTPException(status_code=413, detail="stored chunk exceeds MAX_CHUNK_BYTES")
    return Response(content=data, media_type="application/octet-stream", headers={"X-Checksum": sha256_hex(data)})


@app.delete("/chunks/{chunk_id}", dependencies=INTERNAL)
def delete_chunk(chunk_id: str):
    path = chunk_path(chunk_id)
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        raise HTTPException(status_code=507, detail="could not delete chunk from disk") from exc
    return {"deleted": chunk_id}


class ReplicateRequest(BaseModel):
    chunk_id: str
    source_url: str
    checksum: str

    @field_validator("chunk_id")
    @classmethod
    def valid_chunk_id(cls, value: str) -> str:
        if not CHUNK_ID.fullmatch(value):
            raise ValueError("invalid chunk id")
        return value

    @field_validator("checksum")
    @classmethod
    def valid_checksum(cls, value: str) -> str:
        if not CHECKSUM.fullmatch(value):
            raise ValueError("checksum must be a lowercase SHA-256 digest")
        return value

    @field_validator("source_url")
    @classmethod
    def valid_source_url(cls, value: str) -> str:
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme in {"http", "https"} and parsed.hostname
                and parsed.username is None and parsed.password is None
                and not parsed.query and not parsed.fragment
                and parsed.path in {"", "/"} and "\\" not in value
                and not any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)
            )
            parsed.port  # Validate a malformed/non-numeric port too.
        except ValueError as exc:
            raise ValueError("source_url must be an HTTP(S) origin") from exc
        if not valid:
            raise ValueError("source_url must be an HTTP(S) origin without credentials, path, query or fragment")
        return value.rstrip("/")


@app.post("/replicate", dependencies=INTERNAL)
def replicate(req: ReplicateRequest):
    """Pull and verify a bounded chunk from a source selected by metadata."""
    path = chunk_path(req.chunk_id)
    data = bytearray()
    try:
        with httpx.stream(
            "GET", f"{req.source_url}/chunks/{req.chunk_id}",
            headers=cluster_headers(), timeout=30,
        ) as response:
            response.raise_for_status()
            for part in response.iter_bytes(chunk_size=64 * 1024):
                if len(data) + len(part) > MAX_CHUNK_BYTES:
                    raise HTTPException(status_code=413, detail="source chunk exceeds MAX_CHUNK_BYTES")
                data.extend(part)
    except httpx.HTTPError as exc:
        log.warning("replication source unavailable: %s", exc)
        raise HTTPException(status_code=502, detail="could not fetch chunk from source") from exc
    actual = sha256_hex(data)
    if actual != req.checksum:
        raise HTTPException(status_code=422, detail="source replica is corrupt")
    try:
        write_chunk_atomically(path, bytes(data))
    except OSError as exc:
        raise HTTPException(status_code=507, detail="could not write replica to disk") from exc
    log.info("replicated chunk %s from %s", req.chunk_id, req.source_url)
    return {"chunk_id": req.chunk_id, "replicated_from": req.source_url, "checksum": actual, "size": len(data)}
