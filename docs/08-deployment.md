# 8. Deploy on separate machines and plan recovery

The local Compose cluster is the starting point. This guide describes a small private-network deployment; it does not provision cloud resources or claim a deployment has already been run.

## Understand the cost and failure domains first

“Free tier” is a project constraint, not a permanent billing guarantee. Provider offers, regions, quotas, VM availability, storage limits, and network-transfer charges change. Check the provider's current account-specific terms before creating anything. Set budgets/alerts where available, and avoid assuming that an idle VM or extra disk is free.

A metadata machine plus four storage machines is the clearest failure demonstration. Fewer machines may fit a student budget but provide different guarantees. Several containers on one VM all fail if that VM or its disk fails. Place replicas on separate hosts/disks when host-failure tolerance matters.

You need persistent disk, Python or a container runtime, clocks synchronized reasonably well, and a network on which every node can reach the metadata API and other nodes. The single metadata server remains a single point of failure.

## Step 1: create a private addressing plan

Use a cloud private network or an encrypted private overlay/VPN. The example addresses below are placeholders; replace them with reachable addresses assigned to your machines.

| Machine | Example private address | API |
|---|---|---|
| Metadata | `10.44.0.10` | 8000 |
| node1 | `10.44.0.11` | 8001 |
| node2 | `10.44.0.12` | 8002 |
| node3 | `10.44.0.13` | 8003 |
| node4 | `10.44.0.14` | 8004 |

`NODE_URL` must be reachable from the metadata server and every storage node. `localhost` on node1 refers to node1 itself; it does not refer to the metadata machine. Docker service names such as `http://node1:8001` work on the local Compose network, not automatically between unrelated VMs.

The local Compose file publishes only to `127.0.0.1` so its APIs stay on the host's loopback interface. For multi-host operation, bind to the actual private/VPN interface and apply network rules deliberately. Compose dependency and network behavior are documented in [Docker's service reference](https://docs.docker.com/reference/compose-file/services/) and [networking guide](https://docs.docker.com/compose/how-tos/networking/).

## Step 2: restrict access and set secrets

Permit private traffic as follows:

- Metadata port 8000: from your client/admin device and the storage nodes.
- Storage ports: from the metadata machine and other storage nodes for replication.
- Administrative SSH: from your authorized device/address.

Do not expose storage APIs directly to the public internet. Node registration and replication are powerful operations. Shared tokens are useful but do not create user-level isolation or make arbitrary public deployment safe.

Generate two independent random values locally:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

Run that command separately for each token. Store `API_TOKEN` only on the metadata server and authorized clients. Store `CLUSTER_TOKEN` on the metadata server and all storage nodes. Use the service manager's protected environment/secret mechanism; do not commit values or put them in screenshots. Match the cluster token across services.

Use HTTPS for traffic crossing untrusted networks, or an authenticated encrypted private overlay end to end. The application does not terminate TLS itself. An API token over plain public HTTP can be captured.

## Step 3: install the same revision on each machine

```bash
git clone https://github.com/pugazhjs9/replicafs.git
cd replicafs
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Use the same reviewed commit on each host. Run under a dedicated non-root OS user with permission to its persistent data directory. The included Docker image already uses a non-root user; custom mounts still need compatible ownership.

## Step 4: run metadata

On the metadata host, with tokens supplied securely in its environment:

```bash
export DATABASE_URL=sqlite:////srv/replicafs/metadata.db
export CHUNK_SIZE=1048576
export REPLICATION_FACTOR=3
export MIN_WRITE_REPLICAS=3
export DEAD_AFTER_SECONDS=10
export MONITOR_INTERVAL=5
.venv/bin/python -m uvicorn replicafs.metadata.app:app --host 10.44.0.10 --port 8000 --workers 1
```

Create `/srv/replicafs` beforehand as an administrator and give the service user ownership. Choose a persistent disk path, not a container's disposable writable layer. `MIN_WRITE_REPLICAS=3` requires three successful copies before new file commits; its stricter availability trade-off is explained in the API reference.

Run **one metadata worker**. More Uvicorn workers would create independent in-process locks and monitors, which this design does not coordinate.

## Step 5: run each storage node

On node1, again with the shared cluster token in its environment:

```bash
export NODE_ID=node1
export NODE_URL=http://10.44.0.11:8001
export METADATA_URL=http://10.44.0.10:8000
export DATA_DIR=/srv/replicafs/chunks
export HEARTBEAT_INTERVAL=3
export MAX_CHUNK_BYTES=8388608
.venv/bin/python -m uvicorn replicafs.storage.app:app --host 10.44.0.11 --port 8001 --workers 1
```

Create the writable data directory first. Repeat on the other nodes with unique IDs, addresses, and ports. Use a supervisor such as the host's service manager for startup/restart, log collection, and controlled shutdown. Foreground commands are useful for verification but do not by themselves survive a logout or reboot.

Do not run multiple different disks under the same node ID. Do not share one data directory between unrelated nodes. Keep each node's identity and registered URL stable across restarts. Registration rejects a different URL for an existing ID with 409. Prefer a stable private DNS name if VM addresses may change. A new node ID does not automatically adopt an old ID's metadata claims.

## Step 6: verify the deployment

From an authorized client on the private network:

```bash
curl --fail -H "Authorization: Bearer $API_TOKEN" http://10.44.0.10:8000/nodes
curl --fail -H "Authorization: Bearer $API_TOKEN" http://10.44.0.10:8000/cluster
curl --fail -H "Authorization: Bearer $API_TOKEN" -F file=@hello.txt http://10.44.0.10:8000/files
curl --fail -H "Authorization: Bearer $API_TOKEN" -o recovered.txt http://10.44.0.10:8000/files/hello.txt
cmp hello.txt recovered.txt
```

Confirm four ALIVE nodes, the desired live-copy count, no unavailable chunks, and exact bytes. Stop one holder, verify download, wait for repair on a spare while the holder stays stopped, then bring it back. The standard storage process marks its startup heartbeat with `restarted=true`, so an existing node enters RECOVERING even after a fast restart before timeout. A node returning after heartbeat expiry is also verified. Its retained placement claims must pass checking before it becomes ALIVE. External heartbeat clients omitting that startup flag still depend on timeout and ordinary integrity checks.

Measure deployment latency and recovery time from observed timestamps. Do not copy local timings into a cloud performance claim.

## Optional: keep the Linux services running with systemd

The foreground commands above help you verify addresses and tokens. For a small persistent Linux deployment, the repository includes [metadata](../deploy/systemd/replicafs-metadata.service) and [storage](../deploy/systemd/replicafs-storage.service) unit files. These run one process as the `replicafs` OS user, restart after failure, and send logs to the journal. They use `/opt/replicafs` for code, `/etc/replicafs` for settings, and `/srv/replicafs` for persistent data. Systemd is a Linux service manager; these instructions do not apply to macOS's local launcher.

**Prerequisites on each VM:** a system-wide Python 3.13 installation with virtual-environment support, Git, `sudo` access, and systemd. Check `python3.13 --version` and `systemctl --version`. Install missing prerequisites using your Linux distribution's supported instructions. A Python executable installed only inside your personal home directory will not work with `ProtectHome=true`.

### A. Install code under a dedicated user on every VM

Run these on each fresh machine. If the `replicafs` account already exists, inspect it and skip its creation rather than creating a second account. On distributions using another `nologin` path, adjust the shell path accordingly.

```bash
sudo useradd --system --user-group --home-dir /opt/replicafs --no-create-home --shell /usr/sbin/nologin replicafs
sudo install -d -o replicafs -g replicafs -m 0750 /opt/replicafs
sudo install -d -o replicafs -g replicafs -m 0750 /srv/replicafs
sudo install -d -o root -g root -m 0700 /etc/replicafs
sudo -u replicafs git clone https://github.com/pugazhjs9/replicafs.git /opt/replicafs
```

Choose the same reviewed revision on every host. Replace `YOUR_REVIEWED_COMMIT_HASH` below with its actual Git hash before executing; it is a placeholder, not a branch name.

```bash
sudo -u replicafs git -C /opt/replicafs checkout YOUR_REVIEWED_COMMIT_HASH
sudo -u replicafs python3.13 -m venv /opt/replicafs/.venv
sudo -u replicafs /opt/replicafs/.venv/bin/python -m pip install -r /opt/replicafs/requirements.txt
```

The clone, virtual environment, and dependency installation run as the service user. The service itself will not run as root. These are installation instructions for your machines; no VM resources are created by this repository.

### B. Configure only the metadata host

```bash
sudo install -o root -g root -m 0600 /opt/replicafs/deploy/systemd/metadata.env.example /etc/replicafs/metadata.env
sudoedit /etc/replicafs/metadata.env
sudo install -o root -g root -m 0644 /opt/replicafs/deploy/systemd/replicafs-metadata.service /etc/systemd/system/replicafs-metadata.service
```

In the environment file, replace the example `BIND_HOST` with this VM's private/VPN interface address. Set `PORT=8000`; keep the persistent database path under `/srv/replicafs`. Replace **both token placeholders** with independent random values you generated earlier. The cluster token must match every storage host. Keep `MIN_WRITE_REPLICAS=3` if you want the strict three-copy admission policy. The file uses `KEY=value` assignments, without shell `export` or command substitution.

Do not copy your local demo database into this deployment accidentally. If this is a restore, use the matching backup set and original registered node addresses instead, following the restore procedure below.

### C. Configure each storage host

```bash
sudo install -d -o replicafs -g replicafs -m 0750 /srv/replicafs/chunks
sudo install -o root -g root -m 0600 /opt/replicafs/deploy/systemd/storage.env.example /etc/replicafs/storage.env
sudoedit /etc/replicafs/storage.env
sudo install -o root -g root -m 0644 /opt/replicafs/deploy/systemd/replicafs-storage.service /etc/systemd/system/replicafs-storage.service
```

For node1, use its actual private address in `BIND_HOST` and `NODE_URL`, `NODE_ID=node1`, and `PORT=8001`. `METADATA_URL` must reach the metadata host. Insert the same cluster token used there. On node2–node4, edit the identity, address, advertised URL, and port for that machine. Do not leave the example token text or reuse node1's identity/address on another VM. Never copy the client API token onto storage hosts unnecessarily.

The root-only environment files are read by the system service manager and passed to the non-root process. Their contents are not loaded by a shell. Keep real environment files outside Git.

### D. Verify and start the appropriate unit

On the metadata host:

```bash
sudo systemd-analyze verify /etc/systemd/system/replicafs-metadata.service
sudo systemctl daemon-reload
sudo systemctl enable --now replicafs-metadata.service
sudo systemctl status replicafs-metadata.service --no-pager
sudo journalctl -u replicafs-metadata.service -n 50 --no-pager
```

On each storage host:

```bash
sudo systemd-analyze verify /etc/systemd/system/replicafs-storage.service
sudo systemctl daemon-reload
sudo systemctl enable --now replicafs-storage.service
sudo systemctl status replicafs-storage.service --no-pager
sudo journalctl -u replicafs-storage.service -n 50 --no-pager
```

`daemon-reload` makes systemd read changed unit files. `enable --now` starts a service and enables it at boot. `status` shows whether its process runs; it does not prove full replication. Use `/health`, `/ready`, `/nodes`, and `/cluster` from the earlier verification step after all machines have started.

The units request networking before startup. If your VPN has a separate service that becomes ready later, add an appropriate systemd dependency for that VPN on your hosts; the generic `network-online.target` cannot prove every private peer is reachable. If repeated startup failures hit the restart limit, fix the address/settings, then use `systemctl reset-failed` and start the service again.

### E. Stop, update, and inspect

```bash
sudo journalctl -u replicafs-metadata.service -f
sudo systemctl stop replicafs-metadata.service
sudo systemctl start replicafs-metadata.service
```

Use `replicafs-storage.service` for equivalent storage-host commands. Ctrl+C stops following the journal only. Stop affected services before changing their code/dependencies, check out the reviewed revision as `replicafs`, install requirements into its virtual environment, and start again. Editing an environment file requires a restart; editing a unit also requires `daemon-reload`.

The units allow persistent writes under `/srv/replicafs` and provide private temporary directories for upload/download staging. They restrict writes to the rest of the filesystem and disable privilege escalation. You still need disk monitoring, private networking, backups, and restore testing. Systemd directive behavior is described in its [service](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html) and [execution-environment](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html) references.

**Validation boundary:** these templates were reviewed as text in the macOS development environment; a local systemd manager was not available. Run `systemd-analyze verify` on the target Linux hosts, then actually start and verify the services before calling the cloud deployment tested. Systemd's [verification documentation](https://www.freedesktop.org/software/systemd/man/latest/systemd-analyze.html) explains the checker.

## Disk space and monitoring

Watch node free disk, metadata/temp-disk free space, DEAD/RECOVERING nodes, under-replicated chunks, unavailable chunks, and repeated repair errors. `/metrics` exposes a monitoring representation; `/cluster` is useful for interactive checks.

Recovery currently scans a returning node's retained inventory in one monitor pass; a large inventory or slow peer can delay other monitor work. Normal integrity checking uses a bounded rotating batch.

Every retained version and soft-deleted file consumes space. Three-copy data needs roughly 3× content storage before overhead and old data. Metadata download staging also needs temporary space for concurrent requests. No garbage collector or retention policy automatically controls these totals.

## Back up the map and the bytes together

A metadata-only backup cannot recover destroyed chunks. A chunks-only backup lacks a supported reconstruction of names, versions, and order.

For a straightforward learning-scale backup:

1. Stop client writes and stop the cluster cleanly, including its repair loop.
2. Snapshot/copy the metadata database and every storage node's complete data directory as one labelled backup set.
3. Record the Git revision, configuration without secrets, node identities, and backup time.
4. Keep copies outside the same failure domain. Protect their contents as you would the original files.
5. Restore into a separate environment and download known files to verify checksums.

The native launcher stores its complete local dataset under `data/dev`, so after stopping it you can archive that directory. Docker uses named volumes; use your host/provider's documented volume-backup procedure after `docker compose stop`. `docker compose down -v` destroys the volumes and is not a backup step.

SQLite's backup API can create a consistent database backup while it is open, but coordinating that snapshot with remote chunk changes needs an application-aware backup design. Do not assume a live copy of one database file plus independently changing chunk directories is a complete consistent backup. See [SQLite's official backup guidance](https://www.sqlite.org/backup.html).

## Restore procedure

Stop the replacement services, restore the matching metadata and node-data backup set, restore directory ownership and the original node identities, and start metadata followed by the nodes. Preserve the registered node URLs as reachable stable DNS names/private addresses as well as the node identities; expect verification before old claims are trusted. Changing the registered topology requires a deliberate metadata migration, which this runbook does not automate. Confirm readiness, cluster health, and byte-for-byte downloads of known files before reopening client writes.

A process restart is different from metadata-disk loss. The first normally uses existing persistent files. The second requires a backup. Automatic metadata failover and disaster reconstruction from orphan chunks are outside this version's scope.
