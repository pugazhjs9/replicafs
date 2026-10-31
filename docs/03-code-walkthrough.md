# 3. Read the code with a map

Read the small helpers first, then the storage server, database, repair loop, and metadata API. Function names are useful search terms: use your editor's “find in files” to locate them.

## Shared helpers: `replicafs/common.py`

`sha256_hex` turns bytes into a hexadecimal SHA-256 fingerprint.

`iter_chunks` is a generator: it reads a bounded slice and yields the chunk number plus bytes, then continues when the caller asks for the next one. Empty input produces no chunks. Nonpositive chunk sizes are invalid; zero must not silently turn a nonempty file into an empty one.

`pick_nodes` handles placement ordering. Counting chunks is an understandable approximation of load. It is not byte-perfect space accounting and does not model racks, geography, latency, or concurrent reservations.

## Authentication: `replicafs/security.py`

There are two different secrets:

- `API_TOKEN` protects client file operations and administration requests.
- `CLUSTER_TOKEN` protects internal heartbeats and storage operations. Metadata and storage processes share it.

Requests use `Authorization: Bearer <token>`. `cluster_headers` supplies that header for internal HTTP calls. Token checks use constant-time comparison to avoid an ordinary string comparison revealing information through timing. Empty tokens disable that check for local learning. This is a shared-secret design, not user accounts or file ownership.

Most process configuration is read during import/startup. Apply environment changes by restarting the affected processes; use the same internal token across the cluster.

## Storage: `replicafs/storage/app.py`

A storage node understands chunk IDs and bytes. It does not assign file versions or decide how many copies are needed.

`chunk_path` validates an identifier before converting it into a filesystem path. A client must not be able to turn an ID into `../another-directory`. Length and ASCII checks keep the accepted identifier format small and predictable.

`write_chunk_atomically` creates a unique temporary file in the destination directory, writes and flushes bytes, then uses replacement to publish the completed chunk. Unique temporary names prevent simultaneous writes from sharing one `.tmp` file. Cleanup removes an unsuccessful temporary write. Flushing and atomic replacement address particular failure windows; they are not a guarantee against every disk/controller or power failure.

`local_chunk_count` counts actual chunk files while excluding temporary files and directories. Heartbeats report this count and available filesystem bytes.

`PUT /chunks/{id}` bounds and validates bytes before saving. `GET` returns bytes plus their current checksum. `DELETE` supports cleanup after failed uploads. `/replicate` asks this node to fetch a chunk from another node, check it, and publish it locally.

The lifespan function starts the heartbeat worker. An event lets shutdown stop it rather than leaving an endless worker behind. HTTP errors are checked, so a rejected heartbeat is not treated as a successful registration. Startup heartbeats include `restarted=true` until one registration succeeds; later heartbeats send false. This lets metadata verify a fast-restarting existing node before trusting its old disk claims.

## Database: `replicafs/metadata/db.py`

SQLAlchemy maps Python classes to tables. A `Session` tracks changes and sends them to the database. `commit` saves the transaction; `rollback` abandons it; closing the session frees its resources.

The relationships follow:

```text
File.chunks -> ordered Chunk rows
Chunk.replicas -> Replica rows
Replica.node -> one Node row
```

The `(name, version)` constraint prevents duplicate version records. The `(chunk_id, node_id)` primary key prevents counting one node twice for the same chunk. Foreign keys connect related records. Size fields represent bytes; checksum fields describe expected content.

SQLite is the supported default and makes setup simple. Another SQLAlchemy database URL alone is not proof of support: it needs a suitable driver, migrations, deployment configuration, and its own integration validation.

Each node also has a recovery generation: a counter increased when it restarts.
An integrity pass remembers that counter and can approve the node only if the
counter still matches. Otherwise a second restart could lose the disk while an
older check was still running. Startup applies the small additive column/index
upgrades to existing SQLite databases without deleting their file records.

## Recovery: `replicafs/metadata/replicator.py`

`mark_dead_nodes` compares heartbeat times with a cutoff and marks stale nodes DEAD. Their retained claims stop counting as live copies. A returning heartbeat, or the `restarted=true` startup signal on an existing node, moves it to RECOVERING; retained claims must be checked against disk before it becomes ALIVE. This prevents a blank disk from restoring fictional copies while preserving the ability to recover intact disks after a full outage.

`under_replicated_chunks` finds chunks with surviving sources but insufficient copies. `replication_health` also counts completely unavailable chunks for monitoring; an unavailable chunk must not disappear from the health picture just because it cannot currently be copied.

`re_replicate` chooses destinations that do not already hold the chunk. It retries sources and targets on failure, supplies the cluster token, and records a placement only after a valid destination response. Capacity `-1` means unknown, `0` means full, and positive reports constrain placement. It must not count an HTTP error or malformed acknowledgement as a stored copy.

`verify_replicas` checks a bounded batch for actual size and checksum. Definitive 404/corruption removes a placement; a timeout alone does not prove the bytes are damaged. Batches rotate so an unchanged early part of the table does not permanently starve later replicas.

## Metadata API: `replicafs/metadata/app.py`

`get_db` gives each request its own database session and closes it afterward. Request handlers perform blocking database and HTTP work in ordinary `def` handlers, which FastAPI runs in a thread pool.

Startup initializes the schema and monitor; shutdown signals the monitor to stop and closes network resources. The monitor performs health detection, bounded integrity checks, and repair. Exceptions are logged and the loop continues; a transient error should not permanently stop self-healing.

The raw request-body guard limits incoming metadata requests to `MAX_FILE_BYTES + 64 KiB`, including streamed requests without `Content-Length`, before multipart processing can spool unlimited data. The upload handler then checks the file-content limit, write policy, node selection, cleanup list, version allocation, and final commit. `attempted` keeps enough information to attempt deleting bytes from an abandoned write, including a write whose acknowledgement was lost. It cannot undo a remote write on a currently unreachable node.

The download handler resolves a version and creates a plain read plan. It verifies and stages bytes before returning the response. Do not leave lazy SQLAlchemy relationships inside a generator that outlives its database session. `read_chunk` handles retry and verification; the replica-removal helper makes corrupt/missing copies eligible for repair.

Deletion and restoration update timestamps on metadata records. They do not erase the stored chunk bytes.

## Supporting files

| File | Why it exists |
|---|---|
| `docker-compose.yml` | Describes the local five-process cluster and its separate persistent volumes. |
| `Dockerfile` | Packages both APIs in one reusable, non-root container image. |
| `.env.example` | Lists local token settings without storing real secrets. |
| `scripts/dev_cluster.py` | Starts/stops a native cluster and keeps per-service logs. |
| `scripts/demo_failover.sh` | Gives a repeatable, byte-checked node-failure demonstration. |
| `tests/` | Encodes expected behavior and failures as automated checks. |
| `.github/workflows/` | Runs the repository's checks on changes in GitHub Actions. |

Next: [practice the APIs](05-try-it-yourself.md), then [read the test strategy](07-testing.md).
