# 9. Milestones, assessment demo, and resume evidence

A good project history records meaningful working steps. More commits are useful when each one explains a real decision and has a reviewable scope. Empty commits, backdated activity, and inflated performance claims do not improve the project.

## The milestone story

The repository's [acceptance checklist](../MILESTONES.md) tracks the implementation. Read it alongside `git log --oneline` to connect a commit with its behavior.

| Milestone | What it teaches | Evidence to show |
|---|---|---|
| Baseline and scope | Translate a problem statement into testable requirements. | Acceptance criteria and existing behavior. |
| Shared validation and authentication | Reject bad input and separate client access from node trust. | Validation/token tests and example rejected requests. |
| Durable chunk service | Safe path handling, request bounds, atomic replacement, controlled disk failures. | Storage API tests and concurrent-write checks. |
| Failure detection and repair | Health is a timeout-based suspicion; copies need verified sources and distinct targets. | Recovery tests and repair logs. |
| Returning-node verification | A heartbeat does not prove a disk still contains its old bytes. | RECOVERING tests, wiped-disk and full-outage scenarios. |
| File API correctness | All-or-nothing metadata visibility, safe errors, version sequencing, delete/restore. | Metadata API tests and the version demo. |
| Real cluster testing | Small units are insufficient to prove processes cooperate. | Real HTTP/process test results and byte comparisons. |
| Operations and repeatable delivery | A project must be easy to start, inspect, and stop safely. | Compose health checks, launcher, failure script, CI results. |
| Teaching and handover | Explain the design and its boundaries to another developer. | These docs, deployment/backup plan, honest resume bullets. |

A local passing milestone does not prove cloud deployment or production readiness. State which environment you actually ran.

## Five-minute assessment demonstration

Prepare beforehand: start a fresh disposable local cluster, run the checks, open the code and docs, and keep logs visible. Avoid deleting valuable volumes just to reset a demo.

1. **Thirty seconds: problem.** Show the metadata/nodes diagram. Explain how one disk is a single point of data loss.
2. **One minute: file behavior.** Upload a multi-chunk file. Show its chunk count, target replication factor, and checksum. Download and compare bytes.
3. **One minute: fault tolerance.** Stop a holder, download again, compare bytes. Explain that timeout-based detection runs in the background.
4. **One minute: self-healing.** Show three-copy redundancy returning while that holder remains down. Point to the fourth destination and repair log.
5. **Forty-five seconds: recover a mistake.** Upload a second version, fetch the first, delete, show 404, restore.
6. **Forty-five seconds: evidence and limits.** Show test output for the exact revision. Explain the single metadata server, temporary download staging, and shared-token limitations.

Use `scripts/demo_failover.sh` for the repeatable failure portion. If asked “what happens if every copy is gone?”, answer directly: the system cannot reconstruct unavailable bytes without a surviving copy or backup.

## Resume wording you can substantiate

**Project title:** ReplicaFS — Fault-Tolerant Distributed File Storage Backend

**Technologies:** Python, FastAPI, SQLAlchemy, SQLite, HTTPX, Docker Compose, pytest, GitHub Actions.

Possible bullets, after you can explain and demonstrate each one:

- Built a distributed file-storage backend with configurable chunking, separate-node replication, SHA-256 integrity checks, version history, and soft-delete recovery.
- Implemented heartbeat-based failure detection, verified returning-node recovery, and automatic repair of missing replicas from surviving copies.
- Added validation, shared-token access control, atomic chunk writes, controlled unavailable-data responses, and automated unit/API and real-process fault-recovery tests.
- Packaged a repeatable four-node demonstration with persistent storage, health checks, CI, and beginner-friendly architecture/deployment documentation.

Use three bullets if space is tight. Explain your own contribution accurately. If some code was developed with assistance, be ready to describe how you reviewed and tested it; do not claim design decisions you cannot explain.

## Add numbers only after measuring

A truthful statement might be “tested a file split into four chunks with three stored copies per chunk” when you can show that exact test. “99.999% durable,” “production-grade,” “zero data loss,” “handles millions of files,” and an unmeasured throughput figure are unsupported.

For a benchmark, record:

| Field | Why it matters |
|---|---|
| Git commit | Identifies the code tested. |
| CPU/RAM/disk and OS | Makes the environment understandable. |
| Number of physical hosts | Separates local containers from independent machines. |
| File sizes and count | Describes the workload. |
| Chunk size, target/minimum replicas | Defines storage and durability settings. |
| Concurrency and warm/cold cache | Affects timing and memory substantially. |
| Upload/download/repair measurements | Distinguishes different operations. |
| Repeated samples and failures | Shows variation instead of one lucky result. |

Do not add an experiment result to the README until you have saved its method and output. A local benchmark is evidence for that workload and environment only.

## Practice explaining without memorizing

Draw the four tables. Trace a chunk from request to disk. Stop a node and predict the state transitions. Explain why copying cannot fix zero surviving bytes. Show where an unsuccessful upload rolls back and where cleanup can still leave an orphan. These are stronger interview signals than a long feature list.
