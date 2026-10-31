"""The single-process metadata coordinator: files, placement, and recovery."""

import hashlib
import logging
import tempfile
import threading
import time
from contextlib import asynccontextmanager
from typing import Annotated
from urllib.parse import quote
from uuid import uuid4

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, UploadFile
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask

from replicafs.common import (
    iter_chunks,
    normalize_origin,
    pick_nodes,
    positive_float,
    positive_int,
    sha256_hex,
    validate_filename,
)
from replicafs.limits import UploadBodyLimit
from replicafs.metadata import replicator
from replicafs.metadata.db import Chunk, File, Node, Replica, SessionLocal, init_db, utcnow
from replicafs.security import cluster_headers, require_api_token, require_cluster_token

CHUNK_SIZE = positive_int("CHUNK_SIZE", 4 * 1024 * 1024)
REPLICATION_FACTOR = positive_int("REPLICATION_FACTOR", 3)
MIN_WRITE_REPLICAS = positive_int("MIN_WRITE_REPLICAS", 1)
MAX_FILE_BYTES = positive_int("MAX_FILE_BYTES", 1024 * 1024 * 1024)
DEAD_AFTER_SECONDS = positive_float("DEAD_AFTER_SECONDS", 10)
MONITOR_INTERVAL = positive_float("MONITOR_INTERVAL", 5)
SCRUB_INTERVAL = positive_float("SCRUB_INTERVAL", 30)
SCRUB_BATCH_SIZE = positive_int("SCRUB_BATCH_SIZE", 100)
if MIN_WRITE_REPLICAS > REPLICATION_FACTOR:
    raise ValueError("MIN_WRITE_REPLICAS must not exceed REPLICATION_FACTOR")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("metadata")
client = httpx.Client(trust_env=False)
# Only the final file-version assignment is serialized, never network transfers.
# A single uvicorn worker is required; the DB unique constraint is the last guard.
file_mutation_lock = threading.RLock()
API = [Depends(require_api_token)]


def get_db():
    with SessionLocal() as db:
        yield db


def monitor_loop(stop: threading.Event) -> None:
    cursor = None
    next_scrub = 0.0
    while not stop.is_set():
        try:
            with SessionLocal() as db:
                replicator.mark_dead_nodes(db, DEAD_AFTER_SECONDS)
                replicator.validate_recovering_nodes(db, client)
                if time.monotonic() >= next_scrub:
                    result = replicator.verify_replicas(db, client, SCRUB_BATCH_SIZE, cursor)
                    cursor = result["next_cursor"]
                    next_scrub = time.monotonic() + SCRUB_INTERVAL
                replicator.re_replicate(db, client, REPLICATION_FACTOR)
        except Exception:
            log.exception("monitor pass failed; retrying next interval")
        stop.wait(MONITOR_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    init_db()
    if client.is_closed:
        client = httpx.Client(trust_env=False)
    stop = threading.Event()
    worker = threading.Thread(target=monitor_loop, args=(stop,), daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=5)
        if not worker.is_alive():
            client.close()


app = FastAPI(title="ReplicaFS metadata server", version="1.0.0", lifespan=lifespan)
app.add_middleware(UploadBodyLimit, limit=lambda: MAX_FILE_BYTES + 64 * 1024)


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="metadata database unavailable") from exc
    return {"status": "ok"}


def expire_nodes(db: Session) -> None:
    # Requests must not trust stale ALIVE rows between monitor passes.
    replicator.mark_dead_nodes(db, DEAD_AFTER_SECONDS)


def alive_nodes(db: Session) -> list[Node]:
    return db.query(Node).filter(Node.status == "ALIVE").order_by(Node.id).all()


@app.get("/ready")
def ready(db: Session = Depends(get_db)):
    health(db)
    expire_nodes(db)
    count = len(alive_nodes(db))
    if count < MIN_WRITE_REPLICAS:
        raise HTTPException(status_code=503, detail="not enough live storage nodes for writes")
    return {"status": "ready", "nodes_alive": count, "min_write_replicas": MIN_WRITE_REPLICAS}


class Heartbeat(BaseModel):
    node_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    url: str
    chunk_count: int = Field(default=0, ge=0)
    free_bytes: int = Field(default=-1, ge=-1)
    restarted: bool = False

    @field_validator("url")
    @classmethod
    def validate_url(cls, value):
        return normalize_origin(value)


@app.post("/nodes/heartbeat", dependencies=[Depends(require_cluster_token)])
def heartbeat(hb: Heartbeat, db: Session = Depends(get_db)):
    expire_nodes(db)
    node = db.get(Node, hb.node_id)
    if node is None:
        existing = db.query(Node).filter(Node.url == hb.url).first()
        if existing is not None:
            raise HTTPException(status_code=409, detail="URL already registered under a different node id")
        node = Node(id=hb.node_id, url=hb.url, status="ALIVE", last_heartbeat=utcnow())
        db.add(node)
    elif node.url != hb.url:
        # Stable identity avoids racing a recovery read against an address change.
        raise HTTPException(
            status_code=409, detail="node id already registered at a different URL; use a new node id"
        )
    elif node.status == "DEAD" or hb.restarted:
        node.status = "RECOVERING"
    node.last_heartbeat = utcnow()
    node.chunk_count = hb.chunk_count
    node.free_bytes = hb.free_bytes
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="node registration changed; retry heartbeat") from exc
    return {"ok": True, "status": node.status}


@app.get("/nodes", dependencies=API)
def list_nodes(db: Session = Depends(get_db)):
    expire_nodes(db)
    return [
        {
            "id": n.id,
            "url": n.url,
            "status": n.status,
            "last_heartbeat": n.last_heartbeat.isoformat(),
            "chunk_count": n.chunk_count,
            "free_bytes": n.free_bytes,
        }
        for n in db.query(Node).order_by(Node.id)
    ]


@app.get("/cluster", dependencies=API)
def cluster_status(db: Session = Depends(get_db)):
    expire_nodes(db)
    nodes = db.query(Node).all()
    return {
        "replication_factor": REPLICATION_FACTOR,
        "min_write_replicas": MIN_WRITE_REPLICAS,
        "nodes_alive": sum(n.status == "ALIVE" for n in nodes),
        "nodes_dead": sum(n.status == "DEAD" for n in nodes),
        "nodes_recovering": sum(n.status == "RECOVERING" for n in nodes),
        "files": db.query(File.name).filter(File.deleted_at.is_(None)).distinct().count(),
        "versions": db.query(File).count(),
        "chunks": db.query(Chunk).count(),
        "replicas": db.query(Replica).count(),
        **replicator.replication_health(db, REPLICATION_FACTOR),
    }


@app.get("/metrics", response_class=PlainTextResponse, dependencies=API)
def metrics(db: Session = Depends(get_db)):
    values = cluster_status(db)
    lines = []
    for name, value in values.items():
        lines.extend([f"# TYPE replicafs_{name} gauge", f"replicafs_{name} {value}"])
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


def checked_name(name: str) -> str:
    try:
        return validate_filename(name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def latest_file(db: Session, name: str, include_deleted: bool = False) -> File | None:
    query = db.query(File).filter(File.name == name)
    if not include_deleted:
        query = query.filter(File.deleted_at.is_(None))
    return query.order_by(File.version.desc()).first()


def file_summary(record: File) -> dict:
    return {
        "name": record.name,
        "version": record.version,
        "size": record.size,
        "checksum": record.checksum,
        "chunks": len(record.chunks),
        "created_at": record.created_at.isoformat(),
        "deleted_at": record.deleted_at.isoformat() if record.deleted_at else None,
    }


def cleanup_upload(written: list[tuple[str, str]]) -> None:
    for url, chunk_id in written:
        try:
            client.delete(f"{url}/chunks/{chunk_id}", headers=cluster_headers(), timeout=5)
        except httpx.HTTPError:
            log.warning("orphan chunk %s on %s needs manual cleanup", chunk_id, url)


class PlacementNode:
    def __init__(self, node_id, url, chunk_count, free_bytes):
        self.id, self.url, self.chunk_count, self.free_bytes = node_id, url, chunk_count, free_bytes


@app.post("/files", status_code=201, dependencies=API)
def upload(file: UploadFile, db: Session = Depends(get_db)):
    name = checked_name(file.filename or "")
    if file.size is not None and file.size > MAX_FILE_BYTES:
        raise HTTPException(status_code=413, detail="file exceeds MAX_FILE_BYTES")
    expire_nodes(db)
    # Plain descriptors avoid holding a DB transaction across HTTP I/O.
    nodes = [PlacementNode(n.id, n.url, n.chunk_count, n.free_bytes) for n in alive_nodes(db)]
    db.rollback()
    if len(nodes) < MIN_WRITE_REPLICAS:
        raise HTTPException(status_code=503, detail="not enough live storage nodes for writes")
    record = File(id=uuid4().hex, name=name, version=0, size=0, checksum="", created_at=utcnow())
    digest = hashlib.sha256()
    attempted: list[tuple[str, str]] = []
    minimum = REPLICATION_FACTOR
    try:
        for index, data in iter_chunks(file.file, CHUNK_SIZE):
            if record.size + len(data) > MAX_FILE_BYTES:
                raise HTTPException(status_code=413, detail="file exceeds MAX_FILE_BYTES")
            chunk = Chunk(
                id=uuid4().hex, file_id=record.id, index=index, size=len(data), checksum=sha256_hex(data)
            )
            digest.update(data)
            eligible = [n for n in nodes if n.free_bytes == -1 or n.free_bytes >= len(data)]
            for node in pick_nodes(eligible, len(eligible)):
                attempted.append((node.url, chunk.id))
                try:
                    response = client.put(
                        f"{node.url}/chunks/{chunk.id}",
                        content=data,
                        headers={**cluster_headers(), "X-Checksum": chunk.checksum},
                        timeout=30,
                    )
                    response.raise_for_status()
                    receipt = response.json()
                    if (
                        not isinstance(receipt, dict)
                        or receipt.get("chunk_id") != chunk.id
                        or receipt.get("checksum") != chunk.checksum
                        or receipt.get("size") != len(data)
                    ):
                        log.warning("invalid chunk write receipt from %s", node.id)
                        continue
                except (httpx.HTTPError, ValueError) as exc:
                    log.warning("chunk write to %s failed: %s", node.id, exc)
                    continue
                chunk.replicas.append(Replica(chunk_id=chunk.id, node_id=node.id))
                node.chunk_count += 1
                if node.free_bytes >= 0:
                    node.free_bytes = max(0, node.free_bytes - len(data))
                if len(chunk.replicas) >= REPLICATION_FACTOR:
                    break
            copies = len(chunk.replicas)
            if copies < MIN_WRITE_REPLICAS:
                raise HTTPException(
                    status_code=503, detail=f"chunk {index} could not reach MIN_WRITE_REPLICAS"
                )
            minimum = min(minimum, copies)
            record.chunks.append(chunk)
            record.size += len(data)
        record.checksum = digest.hexdigest()
        with file_mutation_lock:
            db.rollback()
            version = db.query(func.max(File.version)).filter(File.name == name).scalar() or 0
            record.version = version + 1
            db.add(record)
            db.commit()
            result = file_summary(record)
    except Exception as exc:
        db.rollback()
        cleanup_upload(attempted)
        if isinstance(exc, IntegrityError):
            raise HTTPException(status_code=409, detail="version changed concurrently; retry upload") from exc
        if isinstance(exc, SQLAlchemyError):
            raise HTTPException(status_code=503, detail="could not commit file metadata") from exc
        raise
    return {
        **result,
        "replication_factor": REPLICATION_FACTOR,
        "min_replicas": minimum if record.chunks else 0,
        "degraded": bool(record.chunks) and minimum < REPLICATION_FACTOR,
    }


@app.get("/files", dependencies=API)
def list_files(db: Session = Depends(get_db)):
    latest = {}
    for record in db.query(File).filter(File.deleted_at.is_(None)).order_by(File.name, File.version):
        latest[record.name] = record
    return [file_summary(record) for record in latest.values()]


@app.get("/files/{name}/versions", dependencies=API)
def versions(name: str, db: Session = Depends(get_db)):
    rows = db.query(File).filter(File.name == checked_name(name)).order_by(File.version).all()
    if not rows:
        raise HTTPException(status_code=404, detail="file not found")
    return [file_summary(record) for record in rows]


@app.get("/files/{name}", dependencies=API)
def download(name: str, version: Annotated[int | None, Query(ge=1)] = None, db: Session = Depends(get_db)):
    checked_name(name)
    expire_nodes(db)
    record = (
        latest_file(db, name)
        if version is None
        else db.query(File)
        .filter(File.name == name, File.version == version, File.deleted_at.is_(None))
        .first()
    )
    if record is None:
        raise HTTPException(status_code=404, detail="file not found")
    alive = {node.id: node.url for node in alive_nodes(db)}
    plan = [
        (c.id, c.checksum, c.size, [alive[r.node_id] for r in c.replicas if r.node_id in alive])
        for c in record.chunks
    ]
    size, checksum = record.size, record.checksum
    db.rollback()
    spool = None
    try:
        # Verify before HTTP 200. RAM holds a chunk; disk holds one download.
        spool = tempfile.TemporaryFile(mode="w+b")
        digest = hashlib.sha256()
        actual_size = 0
        for chunk_id, expected, chunk_size, urls in plan:
            data = read_chunk(chunk_id, expected, urls, chunk_size)
            digest.update(data)
            actual_size += len(data)
            spool.write(data)
        if actual_size != size or digest.hexdigest() != checksum:
            raise HTTPException(status_code=503, detail="file metadata integrity check failed")
        spool.seek(0)
    except Exception as exc:
        if spool is not None:
            spool.close()
        if isinstance(exc, OSError):
            raise HTTPException(
                status_code=507, detail="not enough temporary disk space for download"
            ) from exc
        raise

    def stream():
        try:
            while data := spool.read(64 * 1024):
                yield data
        finally:
            spool.close()

    return StreamingResponse(
        stream(),
        media_type="application/octet-stream",
        background=BackgroundTask(spool.close),
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(name, safe='')}",
            "Content-Length": str(size),
            "X-Checksum": checksum,
            "ETag": f'"{checksum}"',
        },
    )


def read_chunk(chunk_id: str, checksum: str, urls: list[str], size: int) -> bytes:
    for url in urls:
        try:
            with client.stream(
                "GET", f"{url}/chunks/{chunk_id}", headers=cluster_headers(), timeout=30
            ) as response:
                if response.status_code in {404, 413}:
                    forget_replica(chunk_id, url)
                    continue
                response.raise_for_status()
                data = bytearray()
                for part in response.iter_bytes(chunk_size=64 * 1024):
                    data.extend(part)
                    if len(data) > size:
                        break
        except httpx.HTTPError:
            continue
        if len(data) != size or sha256_hex(data) != checksum:
            forget_replica(chunk_id, url)
            continue
        return bytes(data)
    raise HTTPException(status_code=503, detail=f"no healthy replica for chunk {chunk_id[:8]}")


def forget_replica(chunk_id: str, node_url: str) -> None:
    with SessionLocal() as db:
        node = db.query(Node).filter(Node.url == node_url).first()
        if node is not None:
            removed = (
                db.query(Replica).filter(Replica.chunk_id == chunk_id, Replica.node_id == node.id).delete()
            )
            if removed:
                node.chunk_count = max(0, node.chunk_count - 1)
            db.commit()


@app.delete("/files/{name}", dependencies=API)
def soft_delete(name: str, db: Session = Depends(get_db)):
    checked_name(name)
    with file_mutation_lock:
        rows = (
            db.query(File).filter(File.name == name, File.deleted_at.is_(None)).order_by(File.version).all()
        )
        if not rows:
            raise HTTPException(status_code=404, detail="file not found")
        now = utcnow()
        for record in rows:
            record.deleted_at = now
        db.commit()
    return {"deleted": name, "versions": [f.version for f in rows], "restorable_until": "forever (no GC yet)"}


@app.post("/files/{name}/restore", dependencies=API)
def restore(name: str, db: Session = Depends(get_db)):
    checked_name(name)
    with file_mutation_lock:
        rows = (
            db.query(File)
            .filter(File.name == name, File.deleted_at.is_not(None))
            .order_by(File.version)
            .all()
        )
        if not rows:
            raise HTTPException(status_code=404, detail="nothing to restore")
        for record in rows:
            record.deleted_at = None
        db.commit()
    return {"restored": name, "versions": [f.version for f in rows]}
