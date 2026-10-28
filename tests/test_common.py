import io
from dataclasses import dataclass

from replicafs.common import iter_chunks, pick_nodes, sha256_hex


def test_iter_chunks_splits_in_order_and_keeps_tail():
    data = b"a" * 10 + b"b" * 10 + b"c" * 3
    chunks = list(iter_chunks(io.BytesIO(data), chunk_size=10))
    assert [i for i, _ in chunks] == [0, 1, 2]
    assert [len(c) for _, c in chunks] == [10, 10, 3]
    assert b"".join(c for _, c in chunks) == data


def test_iter_chunks_empty_file_yields_nothing():
    assert list(iter_chunks(io.BytesIO(b""), chunk_size=10)) == []


def test_sha256_hex_is_stable():
    assert sha256_hex(b"hello") == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"


@dataclass
class FakeNode:
    id: str
    chunk_count: int


def test_pick_nodes_prefers_least_loaded_and_caps_at_k():
    nodes = [FakeNode("a", 5), FakeNode("b", 1), FakeNode("c", 3), FakeNode("d", 0)]
    assert [n.id for n in pick_nodes(nodes, 3)] == ["d", "b", "c"]


def test_pick_nodes_returns_fewer_when_not_enough_nodes():
    assert len(pick_nodes([FakeNode("a", 0)], 3)) == 1
