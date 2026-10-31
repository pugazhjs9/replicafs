# Verification record — 31 October 2026

This records observed results, not a guarantee about every possible failure.
The tested implementation revision is
[`4d206e3`](https://github.com/pugazhjs9/replicafs/commit/4d206e39dee92e4c77a38daf3d6ef48185f8089a).
Later documentation changes do not change the tested backend.

## Environment and method

- macOS on Apple Silicon, CPython 3.13.0, pytest 9.2.0.
- Docker Engine 28.1.1; the container image runs Linux/Python 3.13.
- A clean `git archive` export was tested with the project's virtual environment.
  Only committed source was measured; an unrelated untracked Python backup in
  the working directory was preserved and was absent from the export.
- All integration tests started real HTTP servers on random loopback ports,
  with separate temporary databases/disks and both authentication tokens enabled.

## Full automated suite

```bash
python -m pytest --cov=replicafs --cov-branch --cov-report=term-missing --cov-report=xml --cov-fail-under=85
```

**Result: 298 passed in 52.19 seconds; combined statement/branch coverage
95.65% (rounded report: 95.7%).** No tests were skipped. Runtime varies by host;
this is test duration, not a storage throughput benchmark.

| Test file | Cases | Main purpose |
| --- | ---: | --- |
| `test_common.py` | 48 | Chunk boundaries, hashes, names, URLs, settings. |
| `test_database.py` | 9 | Constraints, SQLite settings, safe legacy upgrades. |
| `test_e2e.py` | 18 | Real processes, faults, restarts, disk loss and byte comparisons. |
| `test_limits.py` | 5 | Declared, streamed and misleading upload-body lengths. |
| `test_metadata.py` | 94 | File API, placement, concurrency, rollback, health and auth. |
| `test_replicator.py` | 45 | Expiry, repair, checksums, bounded reads and restart races. |
| `test_runtime.py` | 4 | Startup failures, clean shutdown, repeated lifespans. |
| `test_security.py` | 3 | Independent optional shared-token checks. |
| `test_storage.py` | 72 | Atomic disk writes, errors, limits, replication and heartbeats. |

The run emitted one upstream Starlette TestClient deprecation warning about its
HTTPX integration. It did not fail a test. Database-connection resource warnings
found during earlier verification were corrected by explicitly closing the
integration test connections.

## Other completed checks

| Check | Observed result |
| --- | --- |
| Ruff lint and format checks | Passed. |
| `pip check` | No broken requirements. |
| Runtime `pip-audit` on `requirements.txt` | No known vulnerabilities found on this date. |
| Compose configuration and demo Bash syntax | Passed. |
| Docker build/start | Metadata and all four storage nodes became healthy. |
| Docker failover script | Download matched before/during/after node failure; three live copies per chunk restored while the failed node stayed down; node restarted successfully. |
| Docker persistent-volume upgrade | Final schema upgrade started successfully against the existing test volumes. |
| Native launcher smoke exercise | Authenticated upload, clean shutdown, same-directory restart and persisted download passed. |
| Documentation | Local Markdown links and command/configuration consistency checked. |

The Docker demonstration uploaded a 5,242,887-byte file as six 1-MiB-or-smaller
chunks, initially stored as 18 copies. After failure, live-copy count returned to
18 before the failed node returned. Retained records for the dead node explain
why total recorded replicas can exceed live replicas.

## Boundaries of the evidence

The GitHub workflow defines additional Python 3.12/3.13 Linux checks and a Docker
demo; read the actual [Actions runs](https://github.com/pugazhjs9/replicafs/actions)
for their current status. The local result above does not stand in for a remote
CI result.

No paid or free-tier VMs were provisioned. The multi-host guide and systemd units
are provided for deployment; systemd execution needs verification on the target
Linux hosts. There is no measured cloud availability, durability percentage,
throughput claim, or metadata high-availability implementation. Remaining limits
are explained in the README and deployment guide.
