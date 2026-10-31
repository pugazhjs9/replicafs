"""Deterministic recovery tests: real SQLAlchemy records, fake HTTP peers."""

from datetime import timedelta

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from replicafs.common import sha256_hex
from replicafs.metadata import replicator
from replicafs.metadata.db import Base, Chunk, File, Node, Replica, utcnow

DATA = b"safe chunk bytes"
CHECKSUM = sha256_hex(DATA)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as session:
        yield session
    engine.dispose()


def add_node(db, name, status="ALIVE", age=0, free_bytes=-1, chunk_count=0):
    node = Node(
        id=name,
        url=f"http://{name}:8001",
        status=status,
        last_heartbeat=utcnow() - timedelta(seconds=age),
        free_bytes=free_bytes,
        chunk_count=chunk_count,
    )
    db.add(node)
    db.commit()
    return node


def add_chunk(db, name="chunk1", holders=()):
    record = File(
        id=f"file-{name}",
        name=f"{name}.bin",
        version=1,
        size=len(DATA),
        checksum=CHECKSUM,
        created_at=utcnow(),
    )
    chunk = Chunk(id=name, index=0, size=len(DATA), checksum=CHECKSUM)
    chunk.replicas = [Replica(node_id=node, chunk_id=name) for node in holders]
    record.chunks.append(chunk)
    db.add(record)
    db.commit()
    return chunk


def response_for(request, code=200, **kwargs):
    return httpx.Response(code, request=request, **kwargs)


def good_receipt(request):
    return response_for(request, json={"chunk_id": "chunk1", "checksum": CHECKSUM, "size": len(DATA)})


def test_dead_node_claims_retained_but_not_counted_as_live(db):
    add_node(db, "old", age=60)
    add_node(db, "fresh")
    chunk = add_chunk(db, holders=("old", "fresh"))
    assert len(chunk.replicas) == 2
    assert replicator.mark_dead_nodes(db, 10) == 1
    assert db.get(Node, "old").status == "DEAD"
    assert {replica.node_id for replica in chunk.replicas} == {"old", "fresh"}
    assert replicator.replication_health(db, 2)["live_replicas"] == 1
    assert replicator.mark_dead_nodes(db, 10) == 0


def test_failure_detection_strict_timeout_boundary(db, monkeypatch):
    instant = utcnow()
    monkeypatch.setattr(replicator, "utcnow", lambda: instant)
    for name, age in (("boundary", 10), ("expired", 10.001), ("recent", 9.9)):
        node = add_node(db, name)
        node.last_heartbeat = instant - timedelta(seconds=age)
    db.commit()
    assert replicator.mark_dead_nodes(db, 10) == 1
    assert db.get(Node, "boundary").status == "ALIVE"
    assert db.get(Node, "expired").status == "DEAD"


def test_returning_node_does_not_resurrect_lost_disk_claims(db):
    node = add_node(db, "returning", age=60)
    add_chunk(db, holders=("returning",))
    replicator.mark_dead_nodes(db, 10)
    node.status, node.last_heartbeat = "RECOVERING", utcnow()
    db.commit()
    assert db.query(Replica).count() == 1
    assert replicator.replication_health(db, 3)["unavailable_chunks"] == 1
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, 404))) as client:
        assert replicator.validate_recovering_nodes(db, client) == 1
    assert node.status == "ALIVE"
    assert db.query(Replica).count() == 0
    assert replicator.replication_health(db, 3)["unavailable_chunks"] == 1


def test_complete_outage_revalidates_surviving_disk_data(db):
    for name in ("a", "b", "c"):
        add_node(db, name, age=60)
    add_chunk(db, holders=("a", "b", "c"))
    assert replicator.mark_dead_nodes(db, 10) == 3
    assert replicator.replication_health(db, 3)["unavailable_chunks"] == 1
    for node in db.query(Node):
        node.status, node.last_heartbeat = "RECOVERING", utcnow()
    db.commit()
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, content=DATA))) as client:
        assert replicator.validate_recovering_nodes(db, client) == 3
        assert replicator.validate_recovering_nodes(db, client) == 0
    assert replicator.replication_health(db, 3) == {
        "under_replicated_chunks": 0,
        "unavailable_chunks": 0,
        "healthy_chunks": 1,
        "live_replicas": 3,
    }


def test_recovering_node_missing_heartbeat_becomes_dead_again(db):
    add_node(db, "returning", status="RECOVERING", age=60)
    add_chunk(db, holders=("returning",))
    assert replicator.mark_dead_nodes(db, 10) == 1
    assert db.get(Node, "returning").status == "DEAD"
    assert db.query(Replica).count() == 1


@pytest.mark.parametrize("status,body", [(404, b""), (413, b"too large"), (200, b"bad")])
def test_recovery_removes_confirmed_bad_claims_before_alive(db, status, body):
    node = add_node(db, "returning", status="RECOVERING", chunk_count=1)
    add_chunk(db, holders=("returning",))
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: response_for(req, status, content=body))
    ) as client:
        assert replicator.validate_recovering_nodes(db, client) == 1
    assert node.status == "ALIVE"
    assert db.query(Replica).count() == 0
    assert node.chunk_count == 0


@pytest.mark.parametrize("failure", ["timeout", "server_error"])
def test_transient_recovery_failure_never_promotes_node_alive(db, failure):
    node = add_node(db, "returning", status="RECOVERING")
    add_chunk(db, holders=("returning",))

    def handler(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("later", request=request)
        return response_for(request, 503)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.validate_recovering_nodes(db, client) == 0
    assert node.status == "RECOVERING"
    assert db.query(Replica).count() == 1
    assert replicator.replication_health(db, 3)["unavailable_chunks"] == 1
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, content=DATA))) as client:
        assert replicator.validate_recovering_nodes(db, client) == 1
    assert node.status == "ALIVE"


def test_recovering_node_without_claims_can_become_alive(db):
    node = add_node(db, "empty", status="RECOVERING")
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: pytest.fail("empty node has nothing to verify"))
    ) as client:
        assert replicator.validate_recovering_nodes(db, client) == 1
    assert node.status == "ALIVE"


def test_partial_recovery_never_trusts_unchecked_claim(db):
    node = add_node(db, "returning", status="RECOVERING")
    add_chunk(db, "good", holders=("returning",))
    add_chunk(db, "unreachable", holders=("returning",))

    def handler(request):
        if request.url.path.endswith("unreachable"):
            raise httpx.ReadTimeout("later", request=request)
        return response_for(request, content=DATA)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.validate_recovering_nodes(db, client) == 0
    assert node.status == "RECOVERING"
    assert replicator.replication_health(db, 3)["live_replicas"] == 0


def test_health_counts_unavailable_under_replicated_and_extra_copies(db):
    for name in ("a", "b", "c", "d"):
        add_node(db, name)
    add_node(db, "dead", status="DEAD")
    add_chunk(db, "lost", holders=("dead",))
    add_chunk(db, "partial", holders=("a",))
    add_chunk(db, "healthy", holders=("a", "b", "c"))
    add_chunk(db, "extra", holders=("a", "b", "c", "d"))
    assert replicator.replication_health(db, 3) == {
        "under_replicated_chunks": 2,
        "unavailable_chunks": 1,
        "healthy_chunks": 2,
        "live_replicas": 8,
    }
    assert [chunk.id for chunk, _ in replicator.under_replicated_chunks(db, 3)] == ["partial"]


def test_empty_cluster_health(db):
    assert replicator.replication_health(db, 3) == {
        "under_replicated_chunks": 0,
        "unavailable_chunks": 0,
        "healthy_chunks": 0,
        "live_replicas": 0,
    }


def test_repairs_to_distinct_nodes_and_second_pass_is_idempotent(db):
    for name in ("source", "b", "c", "d"):
        add_node(db, name)
    add_chunk(db, holders=("source",))
    calls = []

    def handler(request):
        calls.append(request)
        return good_receipt(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.re_replicate(db, client, 3) == 2
        assert replicator.re_replicate(db, client, 3) == 0
    assert len(calls) == 2
    assert {row.node_id for row in db.query(Replica)} == {"source", "b", "c"}


def test_failed_first_target_does_not_hide_later_targets(db):
    for name in ("source", "a-broken", "b-good", "c-good"):
        add_node(db, name)
    add_chunk(db, holders=("source",))

    def handler(request):
        return response_for(request, 507) if request.url.host == "a-broken" else good_receipt(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.re_replicate(db, client, 3) == 2
    assert {row.node_id for row in db.query(Replica)} == {"source", "b-good", "c-good"}


def test_corrupt_first_source_tries_other_survivor(db):
    for name in ("a-source", "b-source", "target"):
        add_node(db, name)
    add_chunk(db, holders=("a-source", "b-source"))
    calls = []

    def handler(request):
        import json

        payload = json.loads(request.content)
        calls.append(payload["source_url"])
        return response_for(request, 422) if "a-source" in payload["source_url"] else good_receipt(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.re_replicate(db, client, 3) == 1
    assert calls == ["http://a-source:8001", "http://b-source:8001"]


@pytest.mark.parametrize(
    "receipt",
    [
        {},
        {"chunk_id": "wrong", "checksum": CHECKSUM, "size": len(DATA)},
        {"chunk_id": "chunk1", "checksum": "bad", "size": len(DATA)},
        {"chunk_id": "chunk1", "checksum": CHECKSUM, "size": 0},
        ["not", "an", "object"],
    ],
)
def test_invalid_repair_receipt_never_creates_claim(db, receipt):
    add_node(db, "source")
    add_node(db, "target")
    add_chunk(db, holders=("source",))
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, json=receipt))) as client:
        assert replicator.re_replicate(db, client, 2) == 0
    assert db.query(Replica).count() == 1


def test_malformed_json_repair_receipt_is_rejected(db):
    add_node(db, "source")
    add_node(db, "target")
    add_chunk(db, holders=("source",))
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: response_for(req, content=b"invalid JSON"))
    ) as client:
        assert replicator.re_replicate(db, client, 2) == 0
    assert db.query(Replica).count() == 1


def test_repair_skips_dead_and_full_nodes_but_accepts_unknown_capacity(db):
    add_node(db, "source")
    add_node(db, "dead", status="DEAD")
    add_node(db, "full", free_bytes=0)
    add_node(db, "too-small", free_bytes=1)
    add_node(db, "unknown", free_bytes=-1)
    add_chunk(db, holders=("source",))
    hosts = []

    def handler(request):
        hosts.append(request.url.host)
        return good_receipt(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.re_replicate(db, client, 3) == 1
    assert hosts == ["unknown"]
    assert replicator.replication_health(db, 3)["under_replicated_chunks"] == 1


def test_no_spare_nodes_leaves_data_readable_but_under_replicated(db):
    add_node(db, "source")
    add_chunk(db, holders=("source",))
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: pytest.fail("no HTTP call expected"))
    ) as client:
        assert replicator.re_replicate(db, client, 3) == 0
    assert replicator.replication_health(db, 3)["unavailable_chunks"] == 0


def test_no_source_cannot_invent_lost_bytes(db):
    add_node(db, "target")
    add_chunk(db)
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: pytest.fail("no HTTP call expected"))
    ) as client:
        assert replicator.re_replicate(db, client, 3) == 0
    assert replicator.replication_health(db, 3)["unavailable_chunks"] == 1


def test_repair_network_failure_keeps_existing_claims(db):
    add_node(db, "source")
    add_node(db, "target")
    add_chunk(db, holders=("source",))

    def handler(request):
        raise httpx.ConnectError("offline", request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.re_replicate(db, client, 2) == 0
    assert db.query(Replica).count() == 1


@pytest.mark.parametrize(
    "status,body", [(404, b"missing"), (413, b"oversized"), (200, b"wrong"), (200, b"x" * len(DATA))]
)
def test_scrub_discards_missing_wrong_size_or_corrupt_bytes(db, status, body):
    add_node(db, "source", chunk_count=1)
    chunk = add_chunk(db, holders=("source",))
    assert len(chunk.replicas) == 1
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: response_for(req, status, content=body))
    ) as client:
        result = replicator.verify_replicas(db, client)
    assert result == {"checked": 1, "removed": 1, "next_cursor": ("chunk1", "source")}
    assert db.query(Replica).count() == 0
    assert chunk.replicas == []
    assert db.get(Node, "source").chunk_count == 0


def test_scrub_keeps_good_copy(db):
    add_node(db, "source")
    add_chunk(db, holders=("source",))
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, content=DATA))) as client:
        result = replicator.verify_replicas(db, client)
    assert result["checked"] == 1 and result["removed"] == 0
    assert db.query(Replica).count() == 1


@pytest.mark.parametrize("failure", ["timeout", "server_error"])
def test_scrub_does_not_interpret_temporary_network_failure_as_data_loss(db, failure):
    add_node(db, "source")
    add_chunk(db, holders=("source",))

    def handler(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("try later", request=request)
        return response_for(request, 503)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.verify_replicas(db, client)["removed"] == 0
    assert db.query(Replica).count() == 1


def test_scrub_cursor_rotates_and_wraps_without_starving_later_replicas(db):
    add_node(db, "source")
    for name in ("a", "b", "c"):
        add_chunk(db, name, holders=("source",))
    seen = []

    def handler(request):
        seen.append(request.url.path.rsplit("/", 1)[1])
        return response_for(request, content=DATA)

    cursor = None
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        for _ in range(4):
            result = replicator.verify_replicas(db, client, limit=1, start_after=cursor)
            assert result["checked"] == 1
            cursor = result["next_cursor"]
    assert seen == ["a", "b", "c", "a"]


def test_scrub_empty_cluster_and_invalid_limit(db):
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: pytest.fail("no HTTP call expected"))
    ) as client:
        assert replicator.verify_replicas(db, client) == {"checked": 0, "removed": 0, "next_cursor": None}
        with pytest.raises(ValueError, match="positive"):
            replicator.verify_replicas(db, client, limit=0)


def test_scrub_ignores_dead_node_records(db):
    add_node(db, "dead", status="DEAD")
    add_chunk(db, holders=("dead",))
    with httpx.Client(
        transport=httpx.MockTransport(lambda req: pytest.fail("dead node must not be contacted"))
    ) as client:
        assert replicator.verify_replicas(db, client)["checked"] == 0
    assert db.query(Replica).count() == 1


def test_shared_auth_forwarded_to_repair_and_scrub(db, monkeypatch):
    monkeypatch.setattr(replicator, "cluster_headers", lambda: {"Authorization": "Bearer secret"})
    add_node(db, "source")
    add_node(db, "target")
    add_chunk(db, holders=("source",))
    calls = []

    def handler(request):
        assert request.headers["Authorization"] == "Bearer secret"
        calls.append(request.method)
        return good_receipt(request) if request.method == "POST" else response_for(request, content=DATA)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.re_replicate(db, client, 2) == 1
        assert replicator.verify_replicas(db, client)["removed"] == 0
    assert calls == ["POST", "GET", "GET"]


def test_known_capacity_budget_reaching_zero_stays_full_for_later_chunks(db):
    add_node(db, "source")
    add_node(db, "limited", free_bytes=len(DATA))
    add_chunk(db, "chunk1", holders=("source",))
    add_chunk(db, "chunk2", holders=("source",))
    calls = []

    def handler(request):
        import json

        payload = json.loads(request.content)
        calls.append(payload["chunk_id"])
        return response_for(
            request, json={"chunk_id": payload["chunk_id"], "checksum": CHECKSUM, "size": len(DATA)}
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.re_replicate(db, client, 2) == 1
        assert replicator.re_replicate(db, client, 2) == 0
    assert calls == ["chunk1"]
    assert db.get(Node, "limited").free_bytes == 0
    assert replicator.replication_health(db, 2)["under_replicated_chunks"] == 1


@pytest.mark.parametrize("operation", ["scrub", "recover"])
def test_hostile_oversized_stream_is_stopped_without_reading_entire_body(db, operation):
    node = add_node(db, "source", status="RECOVERING" if operation == "recover" else "ALIVE")
    add_chunk(db, holders=("source",))

    class OversizedStream(httpx.SyncByteStream):
        consumed = 0
        closed = False

        def __iter__(self):
            for _ in range(1024):
                self.consumed += 8192
                yield b"x" * 8192

        def close(self):
            self.closed = True

    stream = OversizedStream()
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, stream=stream))) as client:
        if operation == "recover":
            assert replicator.validate_recovering_nodes(db, client) == 1
            assert node.status == "ALIVE"
        else:
            assert replicator.verify_replicas(db, client)["removed"] == 1
    assert db.query(Replica).count() == 0
    assert 0 < stream.consumed <= len(DATA) + 64 * 1024
    assert stream.closed


def test_revalidation_cannot_overwrite_a_concurrent_dead_transition(db):
    node = add_node(db, "source", status="RECOVERING")
    add_chunk(db, holders=("source",))

    def handler(request):
        # Another request's timeout check runs while this HTTP read is in flight.
        db.query(Node).filter(Node.id == "source").update({Node.status: "DEAD"})
        db.commit()
        return response_for(request, content=DATA)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.validate_recovering_nodes(db, client) == 0
    assert node.status == "DEAD"
    assert db.query(Replica).count() == 1


def test_claim_added_during_revalidation_must_wait_for_next_verification_pass(db):
    node = add_node(db, "source", status="RECOVERING")
    add_chunk(db, "existing", holders=("source",))

    def handler(request):
        # A pending upload commits a previously absent claim during the checks.
        add_chunk(db, "new", holders=("source",))
        return response_for(request, content=DATA)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.validate_recovering_nodes(db, client) == 0
    assert node.status == "RECOVERING"
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, content=DATA))) as client:
        assert replicator.validate_recovering_nodes(db, client) == 1
    assert node.status == "ALIVE"


@pytest.mark.parametrize("old_body", [DATA, b"old corrupt bytes"])
def test_second_restart_during_revalidation_invalidates_entire_old_result(db, old_body):
    node = add_node(db, "source", status="RECOVERING")
    add_chunk(db, holders=("source",))
    initial_generation = node.recovery_generation

    def handler(request):
        # A second startup lands while the previous incarnation's read is in
        # flight. The state is still RECOVERING, but this is a new generation.
        db.query(Node).filter(Node.id == "source").update(
            {Node.recovery_generation: Node.recovery_generation + 1},
            synchronize_session=False,
        )
        db.commit()
        return response_for(request, content=old_body)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert replicator.validate_recovering_nodes(db, client) == 0
    assert node.status == "RECOVERING"
    assert node.recovery_generation == initial_generation + 1
    # Neither an old valid read may promote the new process, nor an old corrupt
    # read may delete a claim whose fresh disk still needs to be examined.
    assert db.query(Replica).count() == 1
    with httpx.Client(transport=httpx.MockTransport(lambda req: response_for(req, content=DATA))) as client:
        assert replicator.validate_recovering_nodes(db, client) == 1
    assert node.status == "ALIVE"
