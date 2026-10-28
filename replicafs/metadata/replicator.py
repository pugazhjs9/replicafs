"""Background health monitor + re-replicator.

Runs in a thread next to the API. Every MONITOR_INTERVAL seconds it:
  1. marks nodes DEAD if they've been silent longer than DEAD_AFTER_SECONDS
  2. finds chunks with fewer live replicas than REPLICATION_FACTOR and copies
     them from a surviving replica onto a healthy node

This is the "self-healing" part of the system. Without it a second node
failure could lose data; with it the cluster returns to full redundancy
on its own.
"""
import logging
from datetime import timedelta

import httpx
from sqlalchemy.orm import Session

from replicafs.metadata.db import Chunk, Node, Replica, utcnow

log = logging.getLogger("metadata.replicator")


def mark_dead_nodes(db: Session, dead_after_seconds: float) -> int:
    cutoff = utcnow() - timedelta(seconds=dead_after_seconds)
    newly_dead = db.query(Node).filter(Node.status == "ALIVE", Node.last_heartbeat < cutoff).all()
    for node in newly_dead:
        node.status = "DEAD"
        log.warning("node %s missed heartbeats for %.0fs -> DEAD", node.id, dead_after_seconds)
    db.commit()
    return len(newly_dead)


def under_replicated_chunks(db: Session, rf: int):
    """Yield (chunk, live_replicas) for chunks with 0 < live copies < rf."""
    alive_ids = {n.id for n in db.query(Node).filter(Node.status == "ALIVE")}
    # Full scan is fine at this scale; a real system would index by node.
    for chunk in db.query(Chunk).all():
        live = [r for r in chunk.replicas if r.node_id in alive_ids]
        # 0 live replicas means the data is unreachable right now — nothing to copy from.
        if 0 < len(live) < rf:
            yield chunk, live


def re_replicate(db: Session, client: httpx.Client, rf: int) -> int:
    alive = {n.id: n for n in db.query(Node).filter(Node.status == "ALIVE")}
    fixed = 0
    for chunk, live in under_replicated_chunks(db, rf):
        holders = {r.node_id for r in live}
        candidates = sorted((n for n in alive.values() if n.id not in holders), key=lambda n: n.chunk_count)
        source = alive[live[0].node_id]
        for target in candidates[: rf - len(live)]:
            try:
                r = client.post(
                    f"{target.url}/replicate",
                    json={"chunk_id": chunk.id, "source_url": source.url, "checksum": chunk.checksum},
                    timeout=60,
                )
                r.raise_for_status()
            except httpx.HTTPError as e:
                log.warning("re-replication of %s to %s failed: %s", chunk.id, target.id, e)
                continue
            db.add(Replica(chunk_id=chunk.id, node_id=target.id))
            target.chunk_count += 1
            fixed += 1
            log.info("re-replicated chunk %s: %s -> %s", chunk.id[:8], source.id, target.id)
    db.commit()
    return fixed
