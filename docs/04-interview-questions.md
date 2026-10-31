# 4. Explain your choices in an interview

Use these as prompts. Reproduce the behavior and understand the source before using a claim on your resume.

## “Explain it in thirty seconds.”

“ReplicaFS is a Python backend that splits uploaded files into chunks and keeps copies on different storage nodes. A metadata API tracks the file versions and chunk locations. Nodes report heartbeats; a background monitor detects unavailable nodes and recreates missing copies from healthy ones. SHA-256 detects bad data, and versioning with soft delete helps recover mistakes. I can demonstrate node failure and verify that downloaded bytes still match.”

## “Is this another S3?”

It is an educational distributed-storage implementation with a small feature set. It has no S3 compatibility claim, multi-tenant authorization, production durability target, or metadata high availability. Comparing its architecture and failure handling is useful; claiming equivalent reliability is unsupported.

## “What is the difference between replication and backup?”

Replication keeps extra copies for availability after failures. Those copies may share an operator mistake or a faulty software change. A backup is a separately recoverable snapshot with its own retention and restore procedure. ReplicaFS still needs metadata and chunk backups.

## “Why not put three copies in the same directory?”

One disk or host failure can remove all three. Independent node IDs are a software placement rule. Independent machines, disks, power, and networks are physical failure domains; deployment determines whether they are actually independent.

## “How do you know a node died?”

I do not know its physical state. I suspect unavailability after heartbeat silence crosses a threshold. That can also be a partition or a slow node. Timeout and monitor interval balance detection delay against false alarms.

## “Can you tolerate two failed nodes?”

For a chunk with three healthy copies on separate failed-independent nodes, two holder failures can leave a readable copy. That statement depends on full replication, reachability, valid bytes, and a working metadata server. Degraded writes have weaker protection. Losing all usable copies cannot be healed.

## “Why a spare node?”

After one of three holders fails, three-copy redundancy needs a fourth destination. A three-node cluster can remain readable after a failure but cannot return to three live copies while that holder stays down.

## “Why checksum every read?”

A successful disk or HTTP read can still return incorrect bytes. Comparing with the expected SHA-256 detects accidental corruption. A surviving good copy enables recovery. A checksum is not encryption or authentication.

## “What happens to a half-finished upload?”

The metadata commit happens after storage work meets the write policy. If the attempt fails, metadata rolls back and cleanup attempts to remove written chunks. Crashing or losing network access can leave orphan bytes because one SQL transaction cannot atomically cover independent remote disks. An orphan collector is a future improvement.

## “Why stage the download?”

A streamed response sends its status first. If a later chunk is unavailable, the API cannot replace an already-sent success status with a clean error. Staging and verifying the file before success gives a reliable error response, using temporary disk space and delaying the first byte.

## “What if a node returns with a wiped disk?”

Timeout detection makes its retained placement claims untrusted. A returning heartbeat sets RECOVERING; the standard node also sends a startup flag so even a fast restart triggers checking. The monitor checks those claims against bytes on disk before allowing ALIVE. Missing/corrupt claims are removed. Valid disks can also recover after a full storage outage. Arbitrary orphan bytes without metadata are not automatically recovered.

## “How do simultaneous uploads get different versions?”

The metadata process serializes the relevant mutation and the database also has a uniqueness constraint. One worker is the supported configuration. Multiple independent coordinators would need a database-backed allocation/locking strategy and a coordinated background repair owner.

## “What if the metadata server fails?”

It is a single point of failure. Persisted metadata survives an ordinary process restart, but the API is unavailable until restart. Losing its disk needs a backup restore. A production design would need replicated metadata, leader coordination, and tested failover; adding another API process is insufficient.

## “What would you improve first?”

Choose based on evidence: orphan and retention garbage collection for storage growth; indexed repair queues for scale; per-user ownership for a shared service; metadata replication for availability; or parallel transfers with bounded concurrency for throughput. Explain the extra failure cases each introduces.

## “How did you test it?”

Describe concrete evidence: isolated request and disk-error tests, checksum and path validation, repair retries, and real-process integration tests that stop nodes and compare downloaded bytes. Then show the test guide and actual output. Do not invent throughput numbers, uptime percentages, or a cloud deployment you have not run.
