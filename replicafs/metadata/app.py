"""Metadata server — the brain of ReplicaFS.

Responsibilities:
  * accept uploads, split them into chunks, place REPLICATION_FACTOR copies of
    each chunk on different storage nodes
  * serve downloads by reading chunks back from any live replica, verifying checksums
  * track node health via heartbeats and re-replicate when a node dies
  * soft-delete + versioning so files can be recovered
"""
import hashlib
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from replicafs.common import iter_chunks, pick_nodes, sha256_hex
from replicafs.metadata import replicator
from replicafs.metadata.db import Chunk, File, Node, Replica, SessionLocal, init_db, utcnow

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 4 * 1024 * 1024))
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", 3))
DEAD_AFTER_SECONDS = float(os.environ.get("DEAD_AFTER_SECONDS", 10))
MONITOR_INTERVAL = float(os.environ.get("MONITOR_INTERVAL", 5))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("metadata")

client = httpx.Client()  # thread-safe; shared by request handlers and the monitor thread


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def monitor_loop() -> None:
    while True:
        try:
            with SessionLocal() as db:
                replicator.mark_dead_nodes(db, DEAD_AFTER_SECONDS)
                replicator.re_replicate(db, client, REPLICATION_FACTOR)
        except Exception:
            log.exception("monitor loop error")
        time.sleep(MONITOR_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    threading.Thread(target=monitor_loop, daemon=True).start()
    log.info("metadata server up: chunk=%dB rf=%d dead_after=%.0fs", CHUNK_SIZE, REPLICATION_FACTOR, DEAD_AFTER_SECONDS)
    yield


app = FastAPI(title="ReplicaFS metadata server", lifespan=lifespan)


# --------------------------------------------------------------------------- nodes

class Heartbeat(BaseModel):
    node_id: str
    url: str
    chunk_count: int = 0
    free_bytes: int = 0


@app.post("/nodes/heartbeat")
def heartbeat(hb: Heartbeat, db: Session = Depends(get_db)):
    node = db.get(Node, hb.node_id)
    if node is None:
        node = Node(id=hb.node_id, url=hb.url, last_heartbeat=utcnow())
        db.add(node)
        log.info("new node joined: %s at %s", hb.node_id, hb.url)
    elif node.status == "DEAD":
        log.info("node %s is back ALIVE", node.id)
    node.url = hb.url
    node.status = "ALIVE"
    node.last_heartbeat = utcnow()
    node.chunk_count = hb.chunk_count
    node.free_bytes = hb.free_bytes
    db.commit()
    return {"ok": True}


@app.get("/nodes")
def list_nodes(db: Session = Depends(get_db)):
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


@app.get("/cluster")
def cluster_status(db: Session = Depends(get_db)):
    """One-shot view of cluster health, handy for demos."""
    nodes = db.query(Node).all()
    under = sum(1 for _ in replicator.under_replicated_chunks(db, REPLICATION_FACTOR))
    return {
        "replication_factor": REPLICATION_FACTOR,
        "nodes_alive": sum(1 for n in nodes if n.status == "ALIVE"),
        "nodes_dead": sum(1 for n in nodes if n.status == "DEAD"),
        "files": db.query(File).filter(File.deleted_at.is_(None)).count(),
        "chunks": db.query(Chunk).count(),
        "replicas": db.query(Replica).count(),
        "under_replicated_chunks": under,
    }


def alive_nodes(db: Session) -> list[Node]:
    return db.query(Node).filter(Node.status == "ALIVE").all()


# --------------------------------------------------------------------------- files

def latest_file(db: Session, name: str, include_deleted: bool = False) -> File | None:
    q = db.query(File).filter(File.name == name)
    if not include_deleted:
        q = q.filter(File.deleted_at.is_(None))
    return q.order_by(File.version.desc()).first()


def file_summary(f: File) -> dict:
    return {
        "name": f.name,
        "version": f.version,
        "size": f.size,
        "checksum": f.checksum,
        "chunks": len(f.chunks),
        "created_at": f.created_at.isoformat(),
        "deleted_at": f.deleted_at.isoformat() if f.deleted_at else None,
    }


@app.post("/files", status_code=201)
def upload(file: UploadFile, db: Session = Depends(get_db)):
    nodes = alive_nodes(db)
    if not nodes:
        raise HTTPException(status_code=503, detail="no storage nodes alive")

    prev = latest_file(db, file.filename, include_deleted=True)
    record = File(
        id=uuid4().hex,
        name=file.filename,
        version=(prev.version + 1) if prev else 1,
        size=0,
        checksum="",
        created_at=utcnow(),
    )
    file_hash = hashlib.sha256()
    written: list[tuple[str, str]] = []  # (node_url, chunk_id) — for cleanup if we abort

    try:
        for index, data in iter_chunks(file.file, CHUNK_SIZE):
            chunk = Chunk(id=uuid4().hex, file_id=record.id, index=index, size=len(data), checksum=sha256_hex(data))
            file_hash.update(data)
            stored = 0
            for node in pick_nodes(nodes, REPLICATION_FACTOR):
                try:
                    r = client.put(f"{node.url}/chunks/{chunk.id}", content=data, headers={"X-Checksum": chunk.checksum}, timeout=30)
                    r.raise_for_status()
                except httpx.HTTPError as e:
                    log.warning("chunk %d -> %s failed: %s", index, node.id, e)
                    continue
                chunk.replicas.append(Replica(chunk_id=chunk.id, node_id=node.id))
                node.chunk_count += 1  # keeps pick_nodes spreading chunks within this upload
                written.append((node.url, chunk.id))
                stored += 1
            if stored == 0:
                raise HTTPException(status_code=503, detail=f"could not store chunk {index} on any node")
            if stored < REPLICATION_FACTOR:
                log.warning("chunk %d stored on %d/%d nodes; re-replicator will top it up", index, stored, REPLICATION_FACTOR)
            record.chunks.append(chunk)
            record.size += len(data)
    except Exception:
        # Nothing has been committed yet, so the file simply doesn't exist from
        # the user's point of view. Best-effort removal of orphaned chunks.
        for url, chunk_id in written:
            try:
                client.delete(f"{url}/chunks/{chunk_id}", timeout=5)
            except httpx.HTTPError:
                pass
        raise

    record.checksum = file_hash.hexdigest()
    # Commit point: the file becomes visible only once every chunk is safely stored.
    db.add(record)
    db.commit()
    log.info("stored %s v%d (%d bytes, %d chunks)", record.name, record.version, record.size, len(record.chunks))
    return file_summary(record)


@app.get("/files")
def list_files(db: Session = Depends(get_db)):
    latest: dict[str, File] = {}
    for f in db.query(File).filter(File.deleted_at.is_(None)).order_by(File.version):
        latest[f.name] = f
    return [file_summary(f) for f in latest.values()]


@app.get("/files/{name}/versions")
def versions(name: str, db: Session = Depends(get_db)):
    rows = db.query(File).filter(File.name == name).order_by(File.version).all()
    if not rows:
        raise HTTPException(status_code=404, detail="file not found")
    return [file_summary(f) for f in rows]


@app.get("/files/{name}")
def download(name: str, version: int | None = None, db: Session = Depends(get_db)):
    if version is None:
        record = latest_file(db, name)
    else:
        record = db.query(File).filter(File.name == name, File.version == version, File.deleted_at.is_(None)).first()
    if record is None:
        raise HTTPException(status_code=404, detail="file not found")

    alive = {n.id: n.url for n in alive_nodes(db)}
    # Build a plain-Python read plan now, so the streaming generator below
    # doesn't depend on the DB session (which closes when this function returns).
    plan = [
        (c.id, c.checksum, [alive[r.node_id] for r in c.replicas if r.node_id in alive])
        for c in record.chunks
    ]
    for chunk_id, _, urls in plan:
        if not urls:
            raise HTTPException(status_code=503, detail=f"chunk {chunk_id[:8]} has no live replica")

    def stream():
        for chunk_id, checksum, urls in plan:
            yield read_chunk(chunk_id, checksum, urls)

    return StreamingResponse(
        stream(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{name}"', "Content-Length": str(record.size)},
    )


def read_chunk(chunk_id: str, checksum: str, urls: list[str]) -> bytes:
    """Try each live replica in turn until one returns bytes with the right checksum."""
    for url in urls:
        try:
            r = client.get(f"{url}/chunks/{chunk_id}", timeout=30)
            if r.status_code == 404:
                log.warning("replica of %s missing on %s", chunk_id[:8], url)
                continue
            r.raise_for_status()
        except httpx.HTTPError as e:
            log.warning("read %s from %s failed: %s", chunk_id[:8], url, e)
            continue
        if sha256_hex(r.content) != checksum:
            log.error("CORRUPT replica of %s on %s — dropping it, trying next", chunk_id[:8], url)
            forget_replica(chunk_id, url)
            continue
        return r.content
    raise RuntimeError(f"no healthy replica for chunk {chunk_id}")


def forget_replica(chunk_id: str, node_url: str) -> None:
    """Remove a replica we found to be corrupt; the re-replicator will make a fresh copy."""
    with SessionLocal() as db:
        node = db.query(Node).filter(Node.url == node_url).first()
        if node:
            db.query(Replica).filter(Replica.chunk_id == chunk_id, Replica.node_id == node.id).delete()
            db.commit()


@app.delete("/files/{name}")
def soft_delete(name: str, db: Session = Depends(get_db)):
    rows = db.query(File).filter(File.name == name, File.deleted_at.is_(None)).all()
    if not rows:
        raise HTTPException(status_code=404, detail="file not found")
    now = utcnow()
    for f in rows:
        f.deleted_at = now
    db.commit()
    return {"deleted": name, "versions": [f.version for f in rows], "restorable_until": "forever (no GC yet)"}


@app.post("/files/{name}/restore")
def restore(name: str, db: Session = Depends(get_db)):
    rows = db.query(File).filter(File.name == name, File.deleted_at.is_not(None)).all()
    if not rows:
        raise HTTPException(status_code=404, detail="nothing to restore")
    for f in rows:
        f.deleted_at = None
    db.commit()
    return {"restored": name, "versions": [f.version for f in rows]}
