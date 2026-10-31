# 2. How an operation works

The central idea is simple: the database describes complete file versions, and storage nodes hold independently verifiable pieces.

## Upload a file

1. The client sends `POST /files` as a multipart form. The metadata API checks the client token when configured and validates the file name.
2. A raw request-body guard caps incoming data at `MAX_FILE_BYTES + 64 KiB` to allow multipart framing while bounding upload handling, including requests without `Content-Length`. The API also checks the actual file content against `MAX_FILE_BYTES`, finds eligible storage nodes, and reads the file in `CHUNK_SIZE` pieces.
3. For each piece, it creates a random chunk identifier and calculates its SHA-256 checksum. It updates the whole-file checksum at the same time.
4. Placement considers node load and available space. The API sends bytes with an `X-Checksum` header to separate nodes, trying alternative destinations after failures.
5. Each node verifies the incoming checksum and request size. It writes a unique temporary file, flushes data to disk, and atomically replaces the final chunk path.
6. Successful destinations become proposed Replica records. The upload's File, Chunk, and Replica records become visible together in a database commit after all chunks meet the write policy.
7. If storage or the final commit fails, the API rolls back the database transaction and attempts to delete chunks already written for that attempt.

A database transaction is an all-or-nothing group of database changes. It cannot also make independent HTTP operations transactional. Cleanup is therefore **best effort**: a disconnected node or a crash can leave unreferenced bytes. There is no automatic orphan garbage collector yet.

The database enforces uniqueness of `(name, version)`. Upload serialization in one metadata process protects version allocation; this is why the supported launch commands use one worker. It is not a distributed lock across several metadata servers.

## Download a file

1. Resolve the latest visible version, or the requested positive `version` number. Hidden/deleted versions return 404.
2. Build an ordered read plan from its chunks and trusted live placements.
3. Read each chunk, trying alternate replicas when a node is unreachable, reports it missing, or returns bytes with the wrong size/checksum.
4. A definitive missing or corrupt copy is removed from the trusted map so repair can replace it. A temporary network failure is not proof that the disk copy is bad.
5. Stage verified bytes in a temporary file on the metadata host. Confirm the total file size and checksum.
6. Only after the file is verified does the API return a successful download response. The temporary file is closed after transmission.

Why stage the full download? HTTP sends the status before the body. Once `200 OK` has been sent, a later missing chunk cannot turn the response into a clean `503`. Verifying first provides a meaningful failure response, at the cost of first-byte latency, temporary disk space, and extra disk I/O.

This is a deliberate trade-off for this project. The application does not need to keep the whole download in RAM, but the metadata host needs space for concurrent staged files.

## A node stops

Suppose chunk A has copies on nodes 1, 2, and 3, and node 4 is available.

```text
node2 stops
    |
    v
heartbeats stop
    |
    v
last heartbeat becomes older than DEAD_AFTER_SECONDS
    |
    v
monitor marks node2 DEAD; retained claims stop counting as live
    |
    v
A has two live copies -> node4 fetches a verified copy from a survivor
    |
    v
metadata records node4 -> A has three live copies again
```

Detection time is approximately the timeout plus scheduling time until the next monitor pass. Repairs then take network and disk time. These settings are not hard real-time deadlines.

A download before the timeout can encounter the stopped node while it is still labelled ALIVE. It retries other listed replicas. After the timeout, new requests avoid that node's placements.

## The node returns

The standard storage process sends `restarted=true` on its first successful startup heartbeat; failed registration attempts keep that flag for retries. An existing node becomes **RECOVERING** on this signal, even if it restarts before the heartbeat timeout. A previously DEAD node also enters RECOVERING when heartbeats resume. A brand-new node starts ALIVE because it has no old claims. The database retains an existing node's old placement claims, but they do not count as live during recovery. The monitor fetches those chunks and compares their bytes with the recorded size and checksum. Verified claims can be trusted after recovery completes; missing/corrupt claims are removed. Temporary network failures leave recovery pending for a later pass.

This handles both a wiped disk and an ordinary restart with intact data. It also supports recovery after all storage nodes have been unavailable: surviving bytes can be validated instead of losing their map. Arbitrary orphan files without retained metadata claims are not automatically inventoried.

A registered node address is immutable: changing `NODE_URL` for the same `NODE_ID` returns 409. A new address needs a new node identity; it does not automatically adopt the old identity's claims. Use a stable DNS name/address across restarts. Never run two different storage nodes with the same `NODE_ID`.

## A disk silently changes bytes

Reads compare SHA-256 and expected size with the metadata record. Periodic integrity checking also examines replicas even when no client downloads the file. Bad or missing placements are dropped; the repair loop copies from a healthy source.

The integrity scan is bounded per pass so it does not read every stored byte on every monitor tick. Detection is eventual. Large clusters need indexed repair work queues and more advanced scanning policies.

## Delete, overwrite, restore

```text
upload notes.txt -> version 1
upload notes.txt -> version 2
GET notes.txt -> version 2
GET notes.txt?version=1 -> version 1
DELETE notes.txt -> both visible versions become hidden
POST notes.txt/restore -> deleted versions become visible again
```

Version history remains inspectable while the file is soft-deleted. Restore restores all deleted versions under the name; it is not a “make version 1 the new latest” operation. To create a new latest copy of version 1, download it and upload its bytes again.

If a new version is uploaded after deletion, its version number continues increasing. Restoration makes older deleted versions visible alongside it; the highest visible version remains the default download.

## What the tables mean

| Table | A row means |
|---|---|
| `nodes` | “This node has this address and heartbeat state.” |
| `files` | “This name has this version, size, checksum, and deletion state.” |
| `chunks` | “This piece belongs at this position in this file version.” |
| `replicas` | “This chunk has a trusted stored copy on this node.” |

Next: [find these decisions in the code](03-code-walkthrough.md).
