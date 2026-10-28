# 3. Code walkthrough

About 450 lines of Python total. Open each file as you read.

```
replicafs/
  common.py                ← 30 lines: hashing, chunk iteration, node picking
  storage/app.py           ← ~130 lines: the dumb box
  metadata/db.py           ← ~80 lines: the four tables
  metadata/replicator.py   ← ~70 lines: failure detection + self-healing
  metadata/app.py          ← ~250 lines: the REST API
```

---

## `replicafs/common.py`

Three tiny functions used by both services.

**`sha256_hex(data)`** — the checksum. SHA-256 turns any bytes into a 64-character
hex string. Change one bit of input → completely different output. That's why it
detects corruption.

**`iter_chunks(fileobj, chunk_size)`** — a generator. Reads `chunk_size` bytes,
yields `(index, bytes)`, repeats until the file is empty. Because it's a generator,
only one chunk is in memory at a time. A 10 GB upload uses ~4 MB of RAM.

**`pick_nodes(nodes, k)`** — sort nodes by how many chunks they hold, take the first
`k`. Least-loaded first. This is the entire "placement policy". Real systems add
rack awareness, disk space, etc. — same idea, more inputs.

---

## `replicafs/storage/app.py` — the storage node

Config comes from environment variables (`NODE_ID`, `NODE_URL`, `METADATA_URL`,
`DATA_DIR`). The same code runs as node1, node2, node3 with different env vars.

**`chunk_path(chunk_id)`** — maps an id to a file on disk. The `isalnum()` check
is a security guard: without it someone could request `../../etc/passwd`.

**`write_chunk_atomically(path, data)`** — write to `path.tmp`, then
`os.replace(tmp, path)`. The rename is atomic: readers see either no file or the
complete file, never a partial one. This is a classic pattern you'll see in
databases, package managers, everywhere.

**`heartbeat_loop()`** — runs in a background thread started in `lifespan`.
Infinite loop: POST to `/nodes/heartbeat`, sleep 3 s. Wrapped in `try/except`
so a dead metadata server doesn't crash the node — it just keeps trying.

**Endpoints:**
- `PUT /chunks/{id}` — receive bytes, verify the `X-Checksum` header matches, write atomically.
- `GET /chunks/{id}` — read bytes, return them with a freshly computed `X-Checksum`.
- `DELETE /chunks/{id}` — remove (used for cleanup after a failed upload).
- `POST /replicate` — "go download chunk X from node Y and keep it." Verifies the
  checksum before saving, so a corrupt source can't spread.

Notice how little this file knows. No concept of files, versions, or other nodes'
health. That's deliberate — dumb nodes are easy to add, replace, and reason about.

---

## `replicafs/metadata/db.py` — the four tables

SQLAlchemy models. The relationship chain:

```python
File.chunks      → list of Chunk, ordered by index
Chunk.replicas   → list of Replica
Replica.node     → Node
```

Things worth noticing:
- `UniqueConstraint("name", "version")` — you can't have two "photo.jpg v2".
- `File.deleted_at` is nullable — `NULL` means "not deleted". That's the whole
  soft-delete mechanism.
- `Replica` has a composite primary key `(chunk_id, node_id)` — a chunk can't be
  recorded twice on the same node.
- `utcnow()` returns a *naive* UTC datetime so the comparison in `mark_dead_nodes`
  behaves identically on SQLite and Postgres.
- `DATABASE_URL` defaults to SQLite. Set it to `postgresql://...` and nothing else changes.

---

## `replicafs/metadata/replicator.py` — the self-healing loop

**`mark_dead_nodes(db, dead_after_seconds)`** — one SQL query:
`status == ALIVE AND last_heartbeat < now - 10s` → set DEAD. Returns how many flipped.

**`under_replicated_chunks(db, rf)`** — for every chunk, count replicas whose node
is ALIVE. Yield the chunk if `0 < count < rf`.

Why `0 <`? If a chunk has **zero** live copies, there's nothing to copy *from*.
We can't fix it; we can only wait for a node to come back. Yielding it would
make the re-replicator try and fail forever.

**`re_replicate(db, client, rf)`** — for each under-replicated chunk:
1. source = any ALIVE node that has it
2. targets = ALIVE nodes that *don't* have it, least-loaded first, take `rf - live` of them
3. POST `/replicate` to each target
4. on success, insert a Replica row and bump the target's chunk count

The comment "full scan is fine at this scale" is honest: with a million chunks
you'd index replicas by node and only rescan the dead node's chunks. Know this
for interviews.

---

## `replicafs/metadata/app.py` — the API

**Startup (`lifespan`)** — create tables, start `monitor_loop` in a daemon thread.
The loop calls `mark_dead_nodes` then `re_replicate` every `MONITOR_INTERVAL` seconds.

**`client = httpx.Client()`** at module level — one shared HTTP client with
connection pooling, used by both request handlers and the monitor thread.

### Node endpoints
- `POST /nodes/heartbeat` — upsert the Node row, set ALIVE, update `last_heartbeat`.
  The "node is back ALIVE" log line is how you'd notice recovery in production logs.
- `GET /nodes` — list with status.
- `GET /cluster` — counts. `under_replicated_chunks` here reuses the exact same
  generator the re-replicator uses, so the number you see is the number it will fix.

### `POST /files` — upload (read this one slowly)

Follow Story A from doc 2 against the code. Key lines:

- `record = File(..., version=(prev.version + 1) if prev else 1)` — versioning is one line.
- `written: list[tuple[str, str]]` — every successful PUT is remembered so we can
  clean up if we abort.
- `node.chunk_count += 1` inside the loop — this is why `pick_nodes` spreads
  chunks across >3 nodes *within* one upload, not just between uploads.
- The `except Exception:` block deletes orphans then **re-raises**. We don't
  swallow the error; the client gets a 503 and the database is untouched.
- `db.add(record); db.commit()` is the **last** thing. That's the commit point.

Note the endpoint is `def`, not `async def`. FastAPI runs it in a thread pool,
which is right for code that does blocking I/O (SQLAlchemy, synchronous httpx).

### `GET /files/{name}` — download

- Builds `plan`, a plain list of `(chunk_id, checksum, [live urls])`, **before**
  returning. The streaming generator must not touch the database session, because
  that session is closed once the function returns. Pre-computing the plan avoids
  a whole class of "session is closed" bugs.
- `StreamingResponse(stream(), ...)` sends chunks as they're fetched — the client
  starts receiving before the last chunk is read.
- `read_chunk` is the failover logic from Story B. `forget_replica` is what
  removes a corrupt copy from the index.

### Delete / restore / versions

Tiny. `soft_delete` sets `deleted_at` on every version of the name.
`restore` sets them back to `NULL`. `versions` lists all rows for a name.

---

## `tests/test_e2e.py` — the proof

The `Cluster` class starts one metadata server and four storage nodes as real
subprocesses on random free ports, with timeouts shortened (heartbeat 1 s, dead
after 3 s) so the test finishes in ~15 s.

Four tests, each a scenario from doc 2:
1. upload → 4 chunks, 12 replicas, bytes round-trip
2. kill a node → download still correct → wait until 0 under-replicated → node returns
3. corrupt a chunk file on disk → download still correct → replica count back to 6
4. two versions, download each, delete, 404, restore, download again

Why 4 nodes in the test but 3 in Docker? With rf = 3 and 3 nodes, when one dies
there's nowhere to put a third copy. The 4th node is the "spare" that lets the
test prove re-replication actually copies bytes.

Next: [04-interview-questions.md](04-interview-questions.md)
