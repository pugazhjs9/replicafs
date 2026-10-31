# 1. The big idea

## The problem

A file on one disk has one place to fail. If that disk is unavailable, downloads stop. If its bytes are destroyed and there is no other copy or backup, the file is lost.

ReplicaFS teaches how to spread that risk. It stores multiple copies of each file piece, notices missing nodes, and makes replacement copies from surviving data.

## Why split a file?

A chunk is a numbered piece. With 1 MiB chunks, a file containing 2 MiB plus seven bytes has three chunks: two full chunks and one seven-byte tail. Downloads join the pieces in the same order.

Copying a chunk is easier than replacing an entire large file. Chunks can be placed across different nodes. The code moves one chunk at a time, which limits application memory compared with loading a whole file. HTTP libraries, multipart upload handling, concurrent requests, and temporary buffering also consume resources; chunk size is not a total process memory guarantee.

## Why three copies?

The replication factor is the desired number of separate node copies of each chunk. Three is a useful learning default: one lost replica can be replaced using another. At full three-copy replication, a chunk can remain readable after two of its holders fail, provided a valid reachable copy remains.

The guarantee is per chunk. Two nodes failing can affect different chunks differently. An upload made with fewer successful copies has less protection. A network partition can make a surviving copy temporarily unreachable. Read the actual write policy in the API reference.

Three copies take roughly three times the file bytes in storage, before old versions, metadata, temporary files, and abandoned chunks. That is the price paid for redundancy.

## Why four nodes in the example?

Imagine chunk A lives on nodes 1, 2, and 3. Node 2 stops. Nodes 1 and 3 still serve A, but only two copies remain. Node 4 can receive a replacement while node 2 is still unavailable.

With only three nodes and a target of three, there is no fourth destination. Downloads can still work, but full redundancy cannot be restored until another healthy destination exists.

## What does the metadata server do?

It stores a map:

```text
notes.txt, version 1
  chunk 0: checksum ..., copies on nodes 1, 2, 4
  chunk 1: checksum ..., copies on nodes 1, 3, 4
```

The database contains this map, names, sizes, times, and version numbers. The storage nodes contain the chunk bytes. The metadata server also transfers file bytes during requests, so it is both the coordinator and the data gateway. It needs disk space for upload handling and verified download staging.

Losing the metadata database is serious even if many chunks survive: the system loses its supported way to identify their names and order. Protect the map as well as the contents.

## How does it detect failure?

Nodes send heartbeats. The metadata server compares the last heartbeat time with a timeout. Silence beyond that timeout means the node is considered unavailable.

This is a suspicion, not a physical diagnosis. The cause could be a stopped process, an unplugged network, a slow machine, or a broken route. “DEAD” is the program's health label. The program cannot know which cause occurred from silence alone.

A shorter timeout detects failures faster but can trigger unnecessary repairs during brief network problems. A longer timeout delays repair but avoids some false alarms.

## How does it heal?

A background monitor counts trusted live copies of each chunk. When the count is below the target and at least one valid source remains, it asks another node to fetch and save that chunk. The target verifies the bytes before the metadata server records the new copy.

If every usable copy is gone, copying cannot invent the lost bytes. `unavailable_chunks` exposes that situation. Backups and independent failure domains remain necessary.

## Why hashes and versions?

SHA-256 produces a fingerprint from bytes. Comparing a stored fingerprint with a newly calculated one detects accidental changes with extremely high confidence. A checksum does not fix bytes by itself; another good replica supplies the replacement. It also is not encryption or proof that the original uploader was trustworthy.

Versions preserve earlier uploads under the same name. Soft delete marks records as hidden without erasing their chunks. These features help recover from accidental overwrites and deletes, but retained versions use disk until a future retention/garbage-collection feature removes them.

Next: [follow the operations step by step](02-how-it-works.md).
