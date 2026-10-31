# 5. Run it and learn by changing it

These exercises assume blank local tokens from `.env.example`. If `API_TOKEN` is enabled, add `-H "Authorization: Bearer $API_TOKEN"` to the client requests shown below. Do not run the destructive corruption exercise against valuable data.

## Route A: Docker

Install Git, Docker with the Compose plugin, and `curl`. Docker Desktop includes Compose on macOS/Windows. Start Docker before running commands.

```bash
git clone https://github.com/pugazhjs9/replicafs.git
cd replicafs
cp .env.example .env
docker compose up -d --build
docker compose ps
curl --fail http://127.0.0.1:8000/nodes
```

`up` starts the services; `-d` leaves them in the background; `--build` creates an image from the current source. The first build downloads dependencies. The metadata API starts first, and node services wait for its health check. Wait until `/nodes` contains four ALIVE nodes.

In a second terminal, watch decisions:

```bash
cd replicafs
docker compose logs -f metadata
```

`-f` follows new log lines. Ctrl+C stops following logs; it does not stop the cluster.

## Route B: native Python

Use this instead of Route A, so both routes do not compete for the same ports. Python 3.13 is the reference version; a virtual environment keeps this project's libraries separate from other projects.

```bash
cd replicafs
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/dev_cluster.py
```

Windows PowerShell activates the environment with `.venv\Scripts\Activate.ps1`; use `python scripts/dev_cluster.py` afterward. The shell examples and automated Docker demonstration use Bash or a compatible shell.

The launcher creates one metadata process and four node processes. Files stay in `data/dev`; service logs are in `data/dev/logs`. It waits until the nodes register and prints the API docs address. Keep this terminal running. Use a second terminal for client calls; Ctrl+C in the launcher terminal stops its child servers.

The launcher reads exported environment variables, not `.env`. For example, set `API_TOKEN` and `CLUSTER_TOKEN` in your shell before starting if you want native token protection. `--base-port 9000 --data-dir data/dev9000` moves the API to port 9000 and the nodes to 9001–9004 with a separate dataset. Reuse the same ports when restarting an existing dataset: registered node addresses are immutable. A separate `--data-dir` creates another independent local dataset.

## Exercise 1: upload and compare

```bash
printf 'Hello from ReplicaFS\n' > hello.txt
curl --fail -F file=@hello.txt http://127.0.0.1:8000/files
curl --fail http://127.0.0.1:8000/files
curl --fail -o downloaded.txt http://127.0.0.1:8000/files/hello.txt
cmp hello.txt downloaded.txt
```

Read the upload's JSON. `size` is bytes; `version` starts at 1; `chunks` is 1 for this small file; `checksum` is the whole-file fingerprint. `cmp` exits successfully without output when the downloaded bytes match.

Open **http://127.0.0.1:8000/docs**. Expand an endpoint, choose “Try it out,” enter values, and execute. The HTTP status and response body are the same API you used from `curl`.

## Exercise 2: see chunking

Create a file slightly bigger than three demo chunks:

```bash
python3 -c "from pathlib import Path; import os; Path('big.bin').write_bytes(os.urandom(3 * 1024 * 1024 + 7))"
curl --fail -F file=@big.bin http://127.0.0.1:8000/files
curl --fail http://127.0.0.1:8000/cluster
```

With the demo's 1 MiB setting, this upload has four chunks. Its ideal placement has twelve copies: four chunks times three replicas. Cluster totals also include previous files and versions, so do not expect the global count to be exactly twelve on a reused dataset.

With Docker, inspect one node:

```bash
docker compose exec node1 ls -la /data
```

Random-looking IDs are chunk filenames. The metadata database connects them to file names and order. The native equivalent is inspecting `data/dev/node1`.

## Exercise 3: prove failure recovery

The repeatable Docker demonstration is:

```bash
./scripts/demo_failover.sh
```

Read each step and the script itself. It uploads fresh data, stops node2, compares bytes during failure, waits for all chunks to have enough live copies, then restarts node2. The fourth node provides replacement destinations while the failed node stays down.

To explore manually:

```bash
docker compose stop node2
curl --fail -o after-failure.bin http://127.0.0.1:8000/files/big.bin
cmp big.bin after-failure.bin
curl --fail http://127.0.0.1:8000/nodes
curl --fail http://127.0.0.1:8000/cluster
```

Wait beyond the 10-second timeout and the next monitor pass. Node2 becomes DEAD. Under-replication may disappear quickly because the other three nodes can repair it; polling can miss that brief intermediate state. A correct demonstration checks the final live-copy counts while node2 is still down.

```bash
docker compose start node2
```

Use `stop` for a controlled persistent outage. A restart policy can cause a crashed process/container to return automatically, so a brief crash is a different exercise.

## Exercise 4: versions and undo

```bash
printf 'draft 1\n' > notes.txt
curl --fail -F file=@notes.txt http://127.0.0.1:8000/files
printf 'draft 2\n' > notes.txt
curl --fail -F file=@notes.txt http://127.0.0.1:8000/files
curl --fail http://127.0.0.1:8000/files/notes.txt
curl --fail 'http://127.0.0.1:8000/files/notes.txt?version=1'
curl --fail http://127.0.0.1:8000/files/notes.txt/versions
curl --fail -X DELETE http://127.0.0.1:8000/files/notes.txt
curl -i http://127.0.0.1:8000/files/notes.txt
curl --fail -X POST http://127.0.0.1:8000/files/notes.txt/restore
curl --fail http://127.0.0.1:8000/files/notes.txt
```

The request after deletion should show 404. Restore reveals the retained versions and default download selects the highest visible version. On a reused dataset version 1 might be from an earlier exercise; inspect `/versions` first.

## Exercise 5: corrupt a disposable copy

For a fresh demo cluster containing `big.bin`, change one real chunk file on node1:

```bash
docker compose exec node1 python -c "from pathlib import Path; p=next(p for p in Path('/data').iterdir() if p.is_file() and not p.name.startswith('.')); p.write_bytes(b'CORRUPTED COPY'); print(p.name)"
curl --fail -o after-corruption.bin http://127.0.0.1:8000/files/big.bin
cmp big.bin after-corruption.bin
curl --fail http://127.0.0.1:8000/cluster
```

This changes only one replica, and another good copy can serve the bytes. On a reused cluster the selected chunk might belong to another file; periodic integrity checking can still detect it, but this particular download may not touch it. The automated tests select exact chunk IDs for precise assertions.

## Exercise 6: run the automated checks

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -v
```

Tests use their own temporary directories and random ports. They do not need the demo cluster. Read [the testing guide](07-testing.md) to understand which cases each layer covers.

## When something does not start

| Symptom | Check |
|---|---|
| Cannot connect to Docker | Start the Docker daemon/Desktop. |
| Address already in use | Stop the other cluster or change native `--base-port`. |
| No ALIVE nodes | Read storage logs; verify `METADATA_URL`, matching cluster token, and reachable `NODE_URL`. |
| 401 response | Client/internal token missing or different; restart after changing settings. |
| 503 upload/download | Inspect `/cluster`, node logs, free disk, and available replica counts. |
| An old file reappears after restart | Persistent volumes/data directories intentionally survived. |
| Disk usage keeps increasing | Every version and soft-deleted file keeps its bytes; no retention GC exists. |
| Permission denied after upgrading an old Docker image | Older root-created volumes may need a backed-up ownership migration for the non-root service user. Inspect logs and preserve data before changing ownership. |

## Stop and preserve, or remove demo data

```bash
docker compose down
```

This stops the cluster and keeps volumes. Use `docker compose down -v` only when you intend to erase this Compose project's stored metadata and chunks. Native Ctrl+C preserves its data directory. Keep your original source files separately.
