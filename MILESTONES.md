# ReplicaFS completion milestones

The project statement asks for chunked file storage on separate nodes, multiple
copies, heartbeat failure detection, automatic repair, checksums, version history,
and reversible deletion. This plan turns the existing prototype into a tested,
explainable portfolio project. Each milestone is a meaningful Git commit; the
history is not backdated or filled with empty commits.

| Milestone | Deliverable | Evidence |
| --- | --- | --- |
| 1. Baseline and acceptance plan | Map the problem statement to implementation and tests | This plan and baseline test run |
| 2. Shared safety foundations | Input/config validation, shared optional tokens, database constraints | Helper, security, and database tests |
| 3. Reliable storage nodes | Bounded requests, durable atomic writes, safe replication, clean shutdown | Storage API and filesystem tests |
| 4. Reliable metadata API | Atomic upload visibility, version ordering, verified downloads, deletion/restore | Metadata API tests and concurrent uploads |
| 5. Failure detection and repair | Expired node invalidation, source fallback, periodic integrity checks | Repair tests and real process failures |
| 6. Repeatable system verification | Isolated real-process cluster, edge/failure/restart cases | End-to-end tests |
| 7. Reproducible development | Pinned dependencies, lint, coverage, CI | Automated checks |
| 8. Run and demonstrate | Four-node Docker setup, local launcher, repeatable failover exercise | Local process demo; Docker validation when available |
| 9. Learn and present | Beginner guides, architecture, API reference, deployment, limitations, resume guide | Documentation checked against actual behavior |

## Acceptance criteria

- Every uploaded chunk is copied to distinct healthy nodes; the API reports when
  fewer than the target number of copies could be written.
- A failed upload does not publish a partial file; cleanup is best effort because
  unreachable disks can retain orphaned bytes.
- Empty, binary, multi-chunk, and repeatedly uploaded files download correctly.
- Simultaneous uploads cannot silently overwrite a version.
- Reads verify chunk and file integrity and return a useful error if no valid
  copy remains.
- Killing a node preserves reads when surviving copies exist. With a spare node,
  repair restores the configured number of copies.
- A returning node with an empty or changed disk does not make missing chunks
  count as healthy replicas.
- Soft deletion hides files and all currently visible versions; restore makes
  deleted versions visible again. Old versions remain individually downloadable.
- Local operation, testing, failure demonstrations, and private-network VM
  deployment are explained step by step.

## Scope and honest limits

This is a single-metadata-process educational system, not a replacement for a
managed production object store. Metadata high availability, consensus, user
accounts, garbage collection, resumable transfers, and proof of operation on
actual cloud VMs are separate future work. Tests cover an explicit scenario
matrix; no finite test suite can prove every possible failure or interleaving.

## Baseline

The repository starts with five helper tests and four real-process system tests.
The first sandboxed run passed the five helper tests; starting local TCP servers
required execution outside the filesystem/network sandbox. Final measured
results belong in the testing guide after implementation and verification.

## Completed implementation history

| Commit | Milestone delivered |
| --- | --- |
| `937c5c8` | Acceptance plan and honest project scope. |
| `ed76135` | Shared validation, authenticated internal/client requests, SQLite safety. |
| `07103a5` | Bounded durable chunk storage and filesystem/API regressions. |
| `7a8d88e` | Retained untrusted claims, verified recovery, capacity-aware repair and integrity scans. |
| `b4ecbab` | Atomic file visibility, concurrent versions, verified downloads, limits and metrics. |
| `d4c1490` | Fast-restart notifications, finite configuration, private internal HTTP traffic. |
| `29692e6` | Authenticated real-process fault and edge-case acceptance tests. |
| `1485bf2` | Dependency snapshot, lint/coverage/security checks, Python matrix and Docker CI. |
| `3a10170` | Four-node Docker deployment, native launcher and byte-checked failover demo. |
| `3631714` | Lifecycle and non-destructive legacy SQLite migration tests. |
| `cde3c7a` | Supervised Linux metadata/storage services with private-network settings. |
| `4d206e3` | Recovery generation guard: stale checks cannot approve a newer restart. |

Commit `925ea55` provides the beginner lessons; the final documentation commit
records [298 passing tests and 95.7% combined coverage](docs/10-verification.md).
All nine planned local milestones are delivered. These complete the educational backend;
actual cloud provisioning and production metadata high availability remain
outside the delivered scope.
