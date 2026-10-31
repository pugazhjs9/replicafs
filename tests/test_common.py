import io
from dataclasses import dataclass

import pytest

from replicafs.common import (
    iter_chunks, normalize_origin, pick_nodes, positive_float, positive_int,
    sha256_hex, validate_filename,
)


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


@pytest.mark.parametrize("size", [0, -1])
def test_invalid_chunk_size(size):
    with pytest.raises(ValueError):
        list(iter_chunks(io.BytesIO(b"data"), size))


def test_node_selection_boundaries():
    assert pick_nodes([], 3) == []
    assert pick_nodes([FakeNode("a", 0)], 0) == []
    with pytest.raises(ValueError):
        pick_nodes([], -1)


@pytest.mark.parametrize("name", ["", ".", "..", "../secret", "a/b", "a\\b", "bad\nname", "\x00", "x" * 256, "é" * 128])
def test_invalid_filename(name):
    with pytest.raises(ValueError):
        validate_filename(name)


@pytest.mark.parametrize("name", ["notes.txt", "hello world.pdf", "தமிழ்.txt", 'a"b.txt', "a" * 255])
def test_valid_filename(name):
    assert validate_filename(name) == name


@pytest.mark.parametrize("url", ["file:///tmp", "ftp://host", "http://user:pass@host", "http://host/a", "http://host?x=1", "http://host/#f", "http://host:bad", "http://", "http://bad host", "http://host\\evil"])
def test_invalid_peer_url(url):
    with pytest.raises(ValueError):
        normalize_origin(url)


@pytest.mark.parametrize("url", ["http://localhost:8001", "https://node.example", "http://[::1]:8001"])
def test_valid_peer_url(url):
    assert normalize_origin(url + "/") == url


@pytest.mark.parametrize("value", ["0", "-1", "no"])
def test_bad_positive_integer_config(monkeypatch, value):
    monkeypatch.setenv("EXAMPLE_SETTING", value)
    with pytest.raises(ValueError):
        positive_int("EXAMPLE_SETTING", 5)


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "no"])
def test_bad_positive_float_config(monkeypatch, value):
    monkeypatch.setenv("EXAMPLE_SETTING", value)
    with pytest.raises(ValueError):
        positive_float("EXAMPLE_SETTING", 5)


def test_config_defaults_and_values(monkeypatch):
    monkeypatch.delenv("EXAMPLE_SETTING", raising=False)
    assert positive_int("EXAMPLE_SETTING", 5) == 5
    assert positive_float("EXAMPLE_SETTING", 1.5) == 1.5
    monkeypatch.setenv("EXAMPLE_SETTING", "12")
    assert positive_int("EXAMPLE_SETTING", 5) == 12
