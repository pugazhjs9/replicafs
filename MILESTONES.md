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
