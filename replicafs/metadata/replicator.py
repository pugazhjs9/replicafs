"""Failure detection, bounded checksum scrubbing, and best-effort replica repair.

Timeouts make claims untrusted; returning nodes must pass revalidation before
ALIVE. Retained claims permit recovery after a complete outage. Each monitor
operation snapshots metadata, releases its read transaction, performs HTTP I/O,
then commits short mutations so slow storage never holds a database write lock.
"""

import hashlib
import logging
from datetime import timedelta
from types import SimpleNamespace

import httpx
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from replicafs.metadata.db import Chunk, Node, Replica, utcnow
from replicafs.security import cluster_headers

log = logging.getLogger("metadata.replicator")


def mark_dead_nodes(db: Session, dead_after_seconds: float) -> int:
    """Atomically expire silent nodes while retaining their untrusted inventory."""
    cutoff = utcnow() - timedelta(seconds=dead_after_seconds)
    # Recheck the cutoff in the UPDATE itself. A heartbeat committed after an
    # earlier SELECT must not be overwritten by a stale timeout decision.
    changed = (
        db.query(Node)
        .filter(
            Node.status.in_(("ALIVE", "RECOVERING")),
            Node.last_heartbeat < cutoff,
        )
        .update({Node.status: "DEAD"}, synchronize_session=False)
    )
    db.commit()
    db.expire_all()
    if changed:
        log.warning(
            "%d nodes missed heartbeats for %.0fs -> DEAD; replica claims untrusted",
            changed,
            dead_after_seconds,
        )
    return changed


def bounded_verified_read(client: httpx.Client, url: str, checksum: str, size: int) -> bool:
    """Verify a peer's bytes with bounded memory and stop at the first overflow.

    False means a definitive invalid copy (404, 413, wrong length or checksum).
    HTTPError means an unproven copy: a timeout or server error is not evidence
    of data loss. Streaming consumes at most ``size + 64 KiB`` of body bytes.
    """
    with client.stream("GET", url, headers=cluster_headers(), timeout=30) as response:
        if response.status_code in (404, 413):
            return False
        response.raise_for_status()
        digest = hashlib.sha256()
        received = 0
        for part in response.iter_bytes(chunk_size=64 * 1024):
            received += len(part)
            if received > size:
                return False
            digest.update(part)
        return received == size and digest.hexdigest() == checksum


def validate_recovering_nodes(db: Session, client: httpx.Client) -> int:
    """Revalidate each returning node's retained inventory before trusting it.

    Heartbeats must preserve RECOVERING. Confirmed bad copies are removed;
    transient errors keep the node RECOVERING for another pass. This small
    teaching system scans each recovering node's complete inventory per pass,
    while each HTTP read uses only a fixed-size buffer.
    """
    plans = []
    for node in db.query(Node).filter(Node.status == "RECOVERING").order_by(Node.id):
        claims = []
        for replica in db.query(Replica).filter(Replica.node_id == node.id).order_by(Replica.chunk_id):
            chunk = db.get(Chunk, replica.chunk_id)
            if chunk is not None:
                claims.append((chunk.id, chunk.checksum, chunk.size))
        plans.append((node.id, node.url, claims))
    db.rollback()
    results = []
    for node_id, url, claims in plans:
        verified = True
        bad_ids = []
        for chunk_id, checksum, size in claims:
            try:
                if not bounded_verified_read(client, f"{url}/chunks/{chunk_id}", checksum, size):
                    bad_ids.append(chunk_id)
            except httpx.HTTPError as exc:
                verified = False
                log.warning("returning node %s cannot yet verify chunk %s: %s", node_id, chunk_id, exc)
        results.append((node_id, url, {claim[0] for claim in claims}, verified, bad_ids))

    recovered = 0
    for node_id, url, checked_ids, verified, bad_ids in results:
        node = db.get(Node, node_id)
        if node is None or node.url != url:
            continue
        removed = 0
        for chunk_id in bad_ids:
            removed += (
                db.query(Replica)
                .filter(Replica.chunk_id == chunk_id, Replica.node_id == node_id)
                .delete(synchronize_session="fetch")
            )
            log.warning("returning node %s lost/corrupted chunk %s; claim removed", node_id, chunk_id)
        if removed:
            node.chunk_count = max(0, node.chunk_count - removed)
        # A concurrent upload may have added a claim while HTTP checks ran. It
        # must be checked in another pass before this node becomes trusted.
        current_ids = {row.chunk_id for row in db.query(Replica).filter(Replica.node_id == node_id)}
        if verified and current_ids <= checked_ids:
            # A concurrent expiry may have changed RECOVERING to DEAD. Only
            # promote the state we actually validated, without reviving DEAD.
            changed = (
                db.query(Node)
                .filter(Node.id == node_id, Node.url == url, Node.status == "RECOVERING")
                .update(
                    {Node.status: "ALIVE"},
                    synchronize_session=False,
                )
            )
            recovered += changed
            if changed:
                log.info("node %s revalidated and ALIVE", node_id)
    db.commit()
    db.expire_all()
    return recovered


def under_replicated_chunks(db: Session, rf: int):
    """Yield repairable (chunk, live_replicas) with 0 < live copies < rf.

    Zero-copy chunks cannot be repaired from peers. They still count as under
    replicated and unavailable in :func:`replication_health`.
    """
    alive_ids = {n.id for n in db.query(Node).filter(Node.status == "ALIVE")}
    for chunk in db.query(Chunk).order_by(Chunk.id):
        live = [r for r in chunk.replicas if r.node_id in alive_ids]
        if 0 < len(live) < rf:
            yield chunk, live


def replication_health(db: Session, rf: int) -> dict[str, int]:
    """Count redundancy honestly, including chunks with no reachable copies."""
    alive_ids = {n.id for n in db.query(Node).filter(Node.status == "ALIVE")}
    health = {"under_replicated_chunks": 0, "unavailable_chunks": 0, "healthy_chunks": 0, "live_replicas": 0}
    for chunk in db.query(Chunk):
        count = sum(r.node_id in alive_ids for r in chunk.replicas)
        health["live_replicas"] += count
        if count < rf:
            health["under_replicated_chunks"] += 1
        else:
            health["healthy_chunks"] += 1
        if count == 0:
            health["unavailable_chunks"] += 1
    return health


def re_replicate(db: Session, client: httpx.Client, rf: int) -> int:
    """Restore redundancy using alternate sources and all eligible targets.

    A free_bytes value of -1 means unknown capacity; zero means a full disk.
    Local budgets are reduced across all chunks in this pass, so exactly filling
    a destination never accidentally turns its remaining capacity into unknown.
    """
    alive = {
        node.id: SimpleNamespace(
            id=node.id, url=node.url, chunk_count=node.chunk_count, free_bytes=node.free_bytes
        )
        for node in db.query(Node).filter(Node.status == "ALIVE")
    }
    plans = [
        (chunk.id, chunk.checksum, chunk.size, [r.node_id for r in live])
        for chunk, live in under_replicated_chunks(db, rf)
    ]
    db.rollback()
    receipts = []
    for chunk_id, checksum, size, source_ids in plans:
        holders = set(source_ids)
        candidates = sorted(
            (
                node
                for node in alive.values()
                if node.id not in holders and (node.free_bytes == -1 or node.free_bytes >= size)
            ),
            key=lambda node: (node.chunk_count, node.id),
        )
        for target in candidates:
            if len(holders) >= rf:
                break
            for source_id in source_ids:
                source = alive[source_id]
                try:
                    response = client.post(
                        f"{target.url}/replicate",
                        json={"chunk_id": chunk_id, "source_url": source.url, "checksum": checksum},
                        headers=cluster_headers(),
                        timeout=60,
                    )
                    response.raise_for_status()
                    receipt = response.json()
                    if (
                        not isinstance(receipt, dict)
                        or receipt.get("chunk_id") != chunk_id
                        or receipt.get("checksum") != checksum
                        or receipt.get("size") != size
                    ):
                        log.warning("invalid repair receipt for chunk %s from %s", chunk_id, target.id)
                        continue
                except (httpx.HTTPError, ValueError) as exc:
                    log.warning("repair of %s from %s to %s failed: %s", chunk_id, source.id, target.id, exc)
                    continue
                receipts.append((chunk_id, target.id))
                holders.add(target.id)
                target.chunk_count += 1
                if target.free_bytes >= 0:
                    target.free_bytes = max(0, target.free_bytes - size)
                log.info("repaired chunk %s: %s -> %s", chunk_id[:8], source.id, target.id)
                break
    fixed = 0
    for chunk_id, target_id in receipts:
        # The single monitor avoids competing repairs; the composite key is the
        # final duplicate guard. A repeated pass with enough copies is a no-op.
        if db.get(Chunk, chunk_id) is None or db.get(Replica, (chunk_id, target_id)) is not None:
            continue
        target = db.get(Node, target_id)
        if target is None or target.url != alive[target_id].url:
            continue
        db.add(Replica(chunk_id=chunk_id, node_id=target_id))
        target.chunk_count = max(target.chunk_count, alive[target_id].chunk_count)
        budget = alive[target_id].free_bytes
        if budget >= 0:
            target.free_bytes = budget if target.free_bytes == -1 else min(target.free_bytes, budget)
        fixed += 1
    db.commit()
    db.expire_all()
    return fixed


def verify_replicas(
    db: Session,
    client: httpx.Client,
    limit: int = 100,
    start_after: tuple[str, str] | None = None,
) -> dict:
    """Check one bounded, rotating batch of live replica records.

    Pass ``next_cursor`` into the next call. Scanning wraps at the end so later
    replicas are never starved. Definitively bad copies are dropped; transport
    failures leave the records alone until the heartbeat failure detector acts.
    """
    if limit < 1:
        raise ValueError("scrub limit must be positive")
    ordered = (
        db.query(Replica)
        .join(Node)
        .filter(Node.status == "ALIVE")
        .order_by(Replica.chunk_id, Replica.node_id)
    )
    if start_after is not None:
        chunk_id, node_id = start_after
        batch = (
            ordered.filter(
                or_(
                    Replica.chunk_id > chunk_id, and_(Replica.chunk_id == chunk_id, Replica.node_id > node_id)
                )
            )
            .limit(limit)
            .all()
        )
        if not batch:
            batch = ordered.limit(limit).all()
    else:
        batch = ordered.limit(limit).all()
    next_cursor = (batch[-1].chunk_id, batch[-1].node_id) if batch else None
    checked = len(batch)
    plans = []
    for replica in batch:
        chunk = db.get(Chunk, replica.chunk_id)
        if chunk is not None:
            plans.append((chunk.id, replica.node_id, replica.node.url, chunk.checksum, chunk.size))
    db.rollback()
    bad = []
    for chunk_id, node_id, url, checksum, size in plans:
        try:
            if not bounded_verified_read(client, f"{url}/chunks/{chunk_id}", checksum, size):
                bad.append((chunk_id, node_id))
        except httpx.HTTPError as exc:
            log.warning("scrub could not reach chunk %s on %s: %s", chunk_id, node_id, exc)
    removed = 0
    for chunk_id, node_id in bad:
        changed = (
            db.query(Replica)
            .filter(Replica.chunk_id == chunk_id, Replica.node_id == node_id)
            .delete(synchronize_session="fetch")
        )
        if changed:
            node = db.get(Node, node_id)
            if node is not None:
                node.chunk_count = max(0, node.chunk_count - changed)
            removed += changed
            log.warning("scrub discarded missing/corrupt chunk %s on %s", chunk_id, node_id)
    db.commit()
    db.expire_all()
    return {"checked": checked, "removed": removed, "next_cursor": next_cursor}
