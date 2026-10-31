# 7. Test strategy and case coverage

Testing asks a precise question: “For this input and failure, did the program behave as expected?” A finite suite cannot cover every machine, disk failure, network schedule, or hostile input. The goal here is repeatable evidence for the important behaviors and boundaries.

See the [dated verification record](10-verification.md) for measured results and
the exact tested revision.

## Install and run

From an activated virtual environment in the repository root:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -v
```

`pytest` discovers functions named `test_*`. `-v` prints scenario names. A passed assertion means its expected condition held. A failed assertion gives you the observed value and the source line to inspect.

Useful focused runs:

```bash
python -m pytest tests/test_common.py tests/test_security.py -v
python -m pytest tests/test_storage.py -v
python -m pytest tests/test_metadata.py tests/test_limits.py -v
python -m pytest tests/test_replicator.py -v
python -m pytest tests/test_database.py tests/test_runtime.py -v
python -m pytest tests/test_e2e.py -v
```

The real-process tests require permission to bind local TCP ports and start child processes. Restricted execution environments can block those operations. An environment failure is not a reason to silently skip the integration suite or claim it passed.

## Three layers of evidence

**Unit tests** call small functions with controlled values. They isolate chunk boundaries, validation, state transitions, and repair selection.

**API/component tests** run requests against FastAPI with temporary storage and an isolated database. Mock HTTP transports simulate precise responses such as timeouts, 404s, incorrect bytes, and malformed receipts. Mocked failures make hard-to-reproduce branches repeatable, but do not prove that real networking works.

**Integration tests** start an actual metadata server and separate storage servers with Uvicorn on loopback ports. They upload bytes, stop/restart processes, alter on-disk chunks, and compare downloads. They exercise real HTTP, persistence, and background workers together. Docker is not required for these tests.

## Scenario map

| Concern | Cases to inspect | Main evidence |
|---|---|---|
| Chunking and hashes | Empty input, exact boundary, partial tail, ordering, invalid chunk size, known SHA-256 value. | `test_common.py` |
| Input and configuration | Bad names/paths/URLs, valid names, invalid numeric settings, positive bounds. | Common and API tests |
| Shared-token access | Optional mode, missing/wrong/correct token, independent internal token, forwarding. | `test_security.py`, storage/API tests |
| Chunk persistence | PUT/GET/DELETE, checksum mismatch, oversized/chunked requests, symlinks and invalid IDs. | `test_storage.py` |
| Atomic writes | Concurrent writers, failed replacement, failed flush, preserved old bytes, temporary cleanup. | `test_storage.py` |
| Disk failures | Controlled write/read/delete errors, unavailable health, directories excluded from counts. | `test_storage.py` |
| Replication pull | Bad source URL, invalid checksum/ID before network, source errors, corruption, limits, token forwarding. | `test_storage.py` |
| Failure detection | Timeout boundary, stale ALIVE/RECOVERING nodes, retained but untrusted claims. | `test_replicator.py` |
| Returning disks | Valid disk, wiped/corrupt data, fast restart startup signal, transient verification errors, full outage recovery. | `test_replicator.py`, integration tests |
| Repair | Distinct targets, alternate sources/targets after failure, capacity, no spare, no source, idempotence. | `test_replicator.py` |
| Repair acknowledgements | Malformed/mismatched receipts never create trusted placements. | `test_replicator.py` |
| Integrity scans | Correct/corrupt/missing copies, timeout retention, bounded rotating cursor. | `test_replicator.py` |
| Upload transaction | Multi-chunk success, failed attempt cleanup/rollback, file and raw-body limits including unknown length, degraded/strict policy, version allocation. | Metadata API and integration tests |
| Download | Exact bytes, alternate replica, missing/corrupt all copies, clean failure before body success. | Metadata API and integration tests |
| Version lifecycle | Re-upload, old-version download, soft delete, restore, upload after delete, history. | Metadata API and integration tests |
| Health and monitoring | Empty cluster, distinct file/version counts, live versus recorded replicas, unavailable chunks. | Metadata API and repair tests |
| Real failure recovery | Node stopped, download matches, redundancy restored while it remains stopped, restart. | `test_e2e.py`, Docker demo |

Check actual test names and assertions for the exact current cases. This map is an explanation of intended behavior, not a promise of exhaustive coverage.

## What makes a useful recovery assertion?

A request returning 200 is insufficient: verify the bytes or whole-file checksum. “Under-replicated count is zero” is insufficient if zero-copy chunks are accidentally excluded: assert unavailable chunks and live-copy counts too. Waiting for a node to return is insufficient to prove repair: check redundancy while it is still down.

For corruption, select a specific chunk and modify that replica. A download may choose a different good replica, so an integrity-repair assertion should observe the bad placement being removed/replaced or inspect the repaired bytes. Avoid a test that passes merely because it never read the corrupted copy.

Use bounded polling for background behavior. A fixed sleep can be unnecessarily slow on a fast host and too short on a slow one. A poll should fail with a clear timeout and leave logs for diagnosis.

## Understand failures before changing expectations

1. Re-run the single failing test with `-v` and inspect its assertion.
2. Decide whether the problem is setup, timing, implementation, or an incorrect expectation.
3. Read the relevant service log and the actual HTTP response.
4. Fix the cause; keep the regression test when it protects meaningful behavior.
5. Run the affected tests, then the required full checks before committing.

Do not make a failing test pass by removing its assertion or weakening a durability requirement without updating the documented contract.

## What remains outside this proof

The suite does not establish production durability, peak throughput, long-running leak freedom, arbitrary crash consistency, true power-loss behavior, TLS correctness, public-cloud cost, or independent-machine availability. The local cluster shares one host and kernel. Real deployments need controlled soak/fault testing, monitored backups, restore exercises, and workload measurements.

If you report test counts or coverage in a presentation, capture them from the exact commit and command you ran. Code coverage tells you which statements ran; it does not prove the assertions were sufficient.

## Run the same quality checks as CI

```bash
python -m ruff check replicafs tests scripts
python -m ruff format --check replicafs tests scripts
python -m pip check
python -m pytest --cov=replicafs --cov-branch --cov-report=term-missing --cov-report=xml --cov-fail-under=85
python -m pip_audit -r requirements.txt --disable-pip --no-deps
docker compose config --quiet
bash -n scripts/demo_failover.sh
```

Ruff checks imports and common coding mistakes; its formatter checks consistent
layout. `pip check` checks dependency compatibility. Coverage records executed
statements and branches; CI requires at least 85% combined coverage. The audit
queries published dependency advisories and needs network access. A clean audit
is a dated check of known issues, not a promise that a package has no flaws.

GitHub Actions runs these checks, the full suite on Python 3.12 and 3.13, and a
separate Docker build/failover job. Inspect the actual run linked by the README
badge before claiming CI is passing. Use a clean checkout when measuring project
coverage: copied Python backup files inside the source directory can appear as
extra untested modules.
