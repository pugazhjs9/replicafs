# 6. API and configuration reference

## Addresses and authentication

Metadata listens at `http://127.0.0.1:8000` in the demo. Open `/docs` for request schemas and interactive calls. Node ports are 8001–8004.

When a token is configured, send:

```text
Authorization: Bearer your-token-value
```

`API_TOKEN` is for client and administration endpoints. `CLUSTER_TOKEN` is for internal heartbeat and chunk traffic. They should be independent values. Empty values disable the respective checks for local learning. There are no per-user accounts or file ownership rules.

## Metadata API

| Method | Path | Token | Meaning |
|---|---|---|---|
| GET | `/health` | Public | Process/database liveness check. |
| GET | `/ready` | Public | Whether enough healthy nodes exist for the configured minimum write policy. |
| POST | `/files` | API | Multipart upload using field `file`; returns 201 after commit. |
| GET | `/files` | API | Latest visible version of each file name. |
| GET | `/files/{name}` | API | Download latest visible version. |
| GET | `/files/{name}?version=1` | API | Download a specific visible positive version. |
| GET | `/files/{name}/versions` | API | List retained versions, including deleted versions. |
| DELETE | `/files/{name}` | API | Soft-delete all currently visible versions of that name. |
| POST | `/files/{name}/restore` | API | Restore all deleted versions of that name. |
| GET | `/nodes` | API | Node addresses, heartbeat times, capacity/load, and state. |
| GET | `/cluster` | API | File/chunk/replica counts and replication health. |
| GET | `/metrics` | API | Prometheus text metrics for monitoring integrations. |
| POST | `/nodes/heartbeat` | Cluster | Register/update a node and its heartbeat. |

A filename is a single name, not a directory path. Use names such as `notes.txt` or `photo-2026.jpg`; path separators, control characters, and reserved path components are rejected. Unicode names require URL encoding in the request path; `curl --get --data-urlencode` helps with query values, while clients/libraries can encode paths. The download response uses a safe attachment name.

The upload response includes `name`, `version`, `size`, `checksum`, `chunks`, creation/deletion times, `replication_factor`, `min_replicas`, and `degraded`. `min_replicas` reports the least replica count across the file's chunks. An empty file has zero chunks, `min_replicas=0`, and `degraded=false`; it needs no stored chunk bytes, although the normal node-admission check still applies.

### Write policy: availability has a cost

The target `REPLICATION_FACTOR` defaults to 3, but `MIN_WRITE_REPLICAS` defaults to 1. An upload can therefore succeed with fewer than three copies of each chunk. The response exposes the reduced redundancy, and repair attempts to reach the target afterward. If that sole copy disappears before repair, the data is unavailable or lost.

For a stronger admission rule, set `MIN_WRITE_REPLICAS=3` with `REPLICATION_FACTOR=3`. Every chunk then needs three successful stored copies before upload commit. Insufficient healthy destinations or failed writes return 503. That protects initial redundancy while reducing write availability during failures. The minimum must be between 1 and the target factor.

Readiness uses the minimum, not the target: ready for degraded writes does not mean the cluster has full redundancy. Use `/cluster` to assess copy health.

### Error meanings

| Status | Typical reason |
|---|---|
| 400 / 422 | Malformed names, IDs, versions, checksums, URLs, counters, or other input. |
| 401 | A configured token is missing or incorrect. |
| 404 | Unknown/hidden file version, missing chunk, or nothing to restore/delete. |
| 409 | Registered node ID has a different URL, or a conflicting registration/version change requires retry. |
| 413 | File/chunk exceeds its configured byte limit. |
| 502 | A storage node could not fetch a replication source. |
| 503 | Insufficient usable storage or no verified source for requested data. |
| 507 | A storage operation cannot finish because of a disk/storage error. |

Use status codes in scripts and read the JSON `detail` field for the explanation. A download failure occurs before a successful body starts because the file is verified and staged first.

## Storage API

These are internal endpoints; clients normally use the metadata API instead.

| Method | Path | Token | Body / result |
|---|---|---|---|
| GET | `/health` | Public | Node identity and current chunk count. |
| PUT | `/chunks/{id}` | Cluster | Raw bytes; optional `X-Checksum` must match if supplied. Returns ID, checksum, size. |
| GET | `/chunks/{id}` | Cluster | Raw chunk bytes and current `X-Checksum`. |
| DELETE | `/chunks/{id}` | Cluster | Idempotently remove this node's copy. |
| POST | `/replicate` | Cluster | JSON `chunk_id`, `source_url`, `checksum`; fetch, validate, and atomically save. |

`source_url` must be an HTTP(S) base URL without credentials, query, or fragment. Allowing internal replication is a privileged capability: keep nodes and cluster credentials on a trusted private network. Token checks do not replace firewall rules or TLS.

Heartbeat JSON contains `node_id`, `url`, `chunk_count`, `free_bytes`, and optional `restarted` (default false). The standard storage node sends `restarted=true` until its first startup registration succeeds, then false; an already-registered node enters RECOVERING on this signal even before a timeout, while a brand-new node starts ALIVE. Node identities and their registered endpoint URLs must be unique. A different ID cannot claim an endpoint already registered to another ID. Registered URLs are immutable. Reusing a node ID with a different URL returns 409; choose a new ID for a new address. A new ID does not adopt the old ID's recorded chunks. Prefer stable DNS/private addresses across restarts. `free_bytes=-1` means unknown capacity (the omitted-field default), `0` means full, and positive values report available bytes. This report is not a disk-space reservation. Actual disk writes can still fail after a heartbeat reports space.

## Node states and health numbers

- **ALIVE:** eligible to serve recorded chunks and receive new placement.
- **DEAD:** heartbeat expired; its replicas do not count as live.
- **RECOVERING:** a returning node is being checked; retained placement claims are not trusted until validation completes.

An existing node returning after expiry or advertising `restarted=true` has its retained claims verified against the stored size and checksum before becoming trusted again. Missing/corrupt claims are removed; transient inability to verify can leave the node RECOVERING for another pass. This also allows a full cluster outage to recover when valid disks return. External heartbeat clients that omit the startup flag rely on timeout detection and periodic/read-time integrity checks.

A restart increases an internal recovery counter. A check started before another
restart cannot approve that newer process using the previous process's results.

`under_replicated_chunks` includes chunks below the target, including zero live copies. `unavailable_chunks` counts those with zero usable live copies. A chunk with two live copies and target three is under-replicated but available. A zero-copy chunk is both under-replicated and unavailable. Total recorded replicas and live replicas can differ while a node is DEAD or RECOVERING.

## Settings

Set environment variables before starting a process. Restart to apply changes. Compose reads `.env` automatically and supplies explicit values; the native launcher inherits exported variables. The standalone API defaults below differ from the demo chunk-size override.

| Variable | Standalone default | Process | Meaning |
|---|---|---|---|
| `DATABASE_URL` | `sqlite:///./data/metadata.db` | Metadata | Where metadata persists. |
| `CHUNK_SIZE` | 4194304 bytes (4 MiB) | Metadata | Maximum bytes per file chunk; demo uses 1048576. |
| `REPLICATION_FACTOR` | 3 | Metadata | Desired separate-node copies per chunk. |
| `MIN_WRITE_REPLICAS` | 1 | Metadata | Minimum successful copies per chunk to admit a write. |
| `MAX_FILE_BYTES` | 1073741824 (1 GiB) | Metadata | Per-upload content limit. |
| `DEAD_AFTER_SECONDS` | 10 | Metadata | Heartbeat age that causes expiry. |
| `MONITOR_INTERVAL` | 5 seconds | Metadata | Delay between health/repair passes. |
| `SCRUB_INTERVAL` | 30 seconds | Metadata | Interval between bounded integrity-check passes. |
| `SCRUB_BATCH_SIZE` | 100 | Metadata | Maximum normal replicas examined per integrity batch. |
| `API_TOKEN` | Empty | Metadata | Shared client/administration bearer token. |
| `CLUSTER_TOKEN` | Empty | Both | Shared internal bearer token; must match everywhere. |
| `NODE_ID` | `node1` | Storage | Stable, unique storage identity. |
| `NODE_URL` | `http://localhost:8001` | Storage | Address other services use to reach this node. |
| `METADATA_URL` | `http://localhost:8000` | Storage | Metadata address for heartbeats. |
| `DATA_DIR` | `./data/{NODE_ID}` | Storage | This node's chunk directory. |
| `HEARTBEAT_INTERVAL` | 3 seconds | Storage | Delay between heartbeat attempts. |
| `MAX_CHUNK_BYTES` | 8388608 (8 MiB) | Storage | Maximum accepted/fetched chunk size. |

Choose `CHUNK_SIZE <= MAX_CHUNK_BYTES` on every storage node. Keep heartbeat intervals comfortably below the dead timeout. Leave room for monitor work and network delays. Use one metadata worker; the supported architecture does not coordinate multiple independent monitors or version allocators.

A metadata request-body guard also caps raw incoming data at `MAX_FILE_BYTES + 65536` bytes (64 KiB of multipart framing allowance). It checks declared lengths and counts streamed body data when `Content-Length` is absent; the separate file-content check still enforces `MAX_FILE_BYTES`. A front proxy should also apply suitable request-body and concurrency limits. Multipart handling and verified downloads can use temporary disk on the metadata host, so configure and monitor that filesystem and its quotas too.
