"""Helpers shared by the metadata server and storage nodes."""
import hashlib
import math
import os
from urllib.parse import urlsplit


def positive_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def positive_float(name: str, default: float) -> float:
    value = float(os.environ.get(name, default))
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")
    return value


def validate_filename(name: str) -> str:
    """Names are single, portable path components; Unicode and spaces are fine."""
    if (not name or name in {".", ".."} or len(name.encode("utf-8")) > 255
            or any(c in "/\\" or ord(c) < 32 or ord(c) == 127 for c in name)):
        raise ValueError("filename must be a single name of 1–255 UTF-8 bytes without control characters")
    return name


def normalize_origin(value: str) -> str:
    """A cluster peer must advertise an HTTP(S) origin, not an arbitrary URL."""
    try:
        parsed = urlsplit(value)
        valid = (parsed.scheme in {"http", "https"} and parsed.hostname
                 and parsed.username is None and parsed.password is None
                 and not parsed.query and not parsed.fragment and parsed.path in {"", "/"}
                 and "\\" not in value
                 and not any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value))
        parsed.port
    except ValueError as exc:
        raise ValueError("URL must be an HTTP(S) origin") from exc
    if not valid:
        raise ValueError("URL must be an HTTP(S) origin without credentials, path, query or fragment")
    return value.rstrip("/")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iter_chunks(fileobj, chunk_size: int):
    """Yield (index, bytes) slices of a binary file object, in order.

    The file is never fully loaded into memory: we only hold one chunk at a time.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    index = 0
    while True:
        data = fileobj.read(chunk_size)
        if not data:
            return
        yield index, data
        index += 1


def pick_nodes(nodes, k: int):
    """Choose up to k nodes for a new chunk, least-loaded first.

    'Load' is the number of chunks a node already holds. Spreading chunks evenly
    keeps any single node from becoming a hotspot and limits the blast radius
    when one node dies.
    """
    if k < 0:
        raise ValueError("k must not be negative")
    return sorted(nodes, key=lambda n: (n.chunk_count, n.id))[:k]
