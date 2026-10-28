"""Helpers shared by the metadata server and storage nodes."""
import hashlib


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iter_chunks(fileobj, chunk_size: int):
    """Yield (index, bytes) slices of a binary file object, in order.

    The file is never fully loaded into memory: we only hold one chunk at a time.
    """
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
    return sorted(nodes, key=lambda n: n.chunk_count)[:k]
