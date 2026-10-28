# ReplicaFS

A fault-tolerant distributed file storage system, in the spirit of GFS / HDFS,
small enough to read in an afternoon and cheap enough to run on free-tier VMs.

Files are split into chunks, every chunk is stored on **3 separate storage nodes**,
and a central **metadata server** tracks where each chunk lives. If a node dies
you can still download your files, and the system **re-replicates** the lost
copies onto healthy nodes by itself. Checksums catch corrupted data; soft delete
and versioning let you recover files you deleted or overwrote.

```
                 upload / download / list / delete
   client  ───────────────────────────────────────▶  metadata server  (FastAPI + SQL)
                                                      │  ▲  files · chunks · replicas · nodes
                                                      │  │  heartbeats every 3s
                                           PUT/GET    │  │  re-replicator thread
                                           chunks     ▼  │
                                   ┌──────────┐  ┌──────────┐  ┌──────────┐
                                   │  node1   │  │  node2   │  │  node3   │   storage nodes
                                   │ chunks/  │  │ chunks/  │  │ chunks/  │   (dumb: store bytes by id)
                                   └──────────┘  └──────────┘  └──────────┘
```

**New to distributed systems?** Start with [docs/00-START-HERE.md](docs/00-START-HERE.md).

## Quick start

```bash
docker compose up -d --build          # 1 metadata server + 3 storage nodes
curl localhost:8000/nodes             # all three should be ALIVE after a few seconds

curl -F file=@some.pdf localhost:8000/files          # upload
curl localhost:8000/files                            # list
curl -o copy.pdf localhost:8000/files/some.pdf       # download
curl -X DELETE localhost:8000/files/some.pdf         # soft delete
curl -X POST localhost:8000/files/some.pdf/restore   # undo
curl localhost:8000/files/some.pdf/versions          # every version ever uploaded
curl -o old.pdf "localhost:8000/files/some.pdf?version=1"
curl localhost:8000/cluster                          # health overview
```

Interactive API docs: http://localhost:8000/docs

## Fault-tolerance demo

```bash
./scripts/demo_failover.sh
```

Uploads a 5 MB file, kills `node2`, proves the download still matches byte-for-byte,
then shows the re-replicator top the chunks back up when a node is available.
Watch it happen: `docker compose logs -f metadata`.

## How it works

| Concern | Mechanism |
|---|---|
| **Chunking** | Upload stream is read in `CHUNK_SIZE` slices (default 4 MB); nothing is buffered in full. |
| **Replication** | Each chunk is `PUT` to `REPLICATION_FACTOR` (3) least-loaded live nodes. The file record is committed only after every chunk is stored — a partial upload leaves no visible file. |
| **Failure detection** | Nodes POST a heartbeat every 3 s. A node silent for `DEAD_AFTER_SECONDS` (10 s) is marked `DEAD`; it is never asked for reads or writes until it heartbeats again. |
| **Self-healing** | A monitor thread scans for chunks with fewer live replicas than the factor and tells a healthy node to pull a copy from a surviving replica. |
| **Integrity** | SHA-256 per chunk, recorded at upload and verified on every read. A corrupt replica is dropped from the index and re-replicated from a good copy. Nodes write chunks to a temp file and `rename()`, so a crash never leaves a half-written chunk. |
| **Versioning** | Re-uploading a name creates `version+1`; old versions stay downloadable. |
| **Soft delete** | `DELETE` sets `deleted_at`; `restore` clears it. Chunks are never physically removed (GC is on the roadmap). |

### Data model

```
File (name, version, size, checksum, deleted_at)
 └── Chunk (index, size, checksum)
      └── Replica (node_id)  ──▶  Node (url, status, last_heartbeat)
```

## Layout

```
replicafs/
  common.py              chunking, hashing, node selection
  metadata/
    app.py               REST API: files, nodes, cluster
    db.py                SQLAlchemy models
    replicator.py        dead-node detection + re-replication loop
  storage/
    app.py               chunk store + heartbeat sender
scripts/demo_failover.sh end-to-end failure demo
tests/                   unit tests (pytest)
docker-compose.yml       local 4-process cluster
```

## Configuration

| Variable | Default | Where |
|---|---|---|
| `CHUNK_SIZE` | 4 MiB | metadata |
| `REPLICATION_FACTOR` | 3 | metadata |
| `DEAD_AFTER_SECONDS` | 10 | metadata |
| `MONITOR_INTERVAL` | 5 | metadata |
| `DATABASE_URL` | `sqlite:///./data/metadata.db` | metadata (any SQLAlchemy URL, e.g. Postgres) |
| `NODE_ID`, `NODE_URL`, `METADATA_URL`, `DATA_DIR` | — | storage node |
| `HEARTBEAT_INTERVAL` | 3 | storage node |

## Running without Docker

```bash
pip install -r requirements.txt
uvicorn replicafs.metadata.app:app --port 8000 &
NODE_ID=node1 NODE_URL=http://localhost:8001 uvicorn replicafs.storage.app:app --port 8001 &
NODE_ID=node2 NODE_URL=http://localhost:8002 uvicorn replicafs.storage.app:app --port 8002 &
NODE_ID=node3 NODE_URL=http://localhost:8003 uvicorn replicafs.storage.app:app --port 8003 &
pytest
```

## Roadmap

- [ ] Garbage-collect chunks of hard-deleted files / excess replicas
- [ ] Verify a returning node's chunks instead of trusting its old replica records
- [ ] Parallel chunk upload/download
- [ ] Deploy across real free-tier VMs (Oracle Cloud / GCP) with Tailscale
- [ ] Metadata server high availability

## Limitations (on purpose, for now)

Single metadata server (no HA), no auth, re-replicator does a full table scan,
and SQLite by default. Each of these is a documented upgrade path rather than a surprise.
