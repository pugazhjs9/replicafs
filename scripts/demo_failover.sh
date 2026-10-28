#!/usr/bin/env bash
# End-to-end proof that ReplicaFS survives a node failure.
#
#   1. upload a 5 MB random file (-> 5 chunks x 3 replicas)
#   2. kill node2
#   3. download the file while node2 is dead and verify the bytes are identical
#   4. watch the re-replicator restore full redundancy on the surviving nodes
#   5. bring node2 back
#
# Run from the repo root with the cluster already up: docker compose up -d --build
set -euo pipefail

BASE=${BASE:-http://localhost:8000}
TMP=$(mktemp -d)
NAME=demo_$(date +%s).bin

step() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

step "1. upload 5 MB file"
head -c $((5 * 1024 * 1024)) /dev/urandom > "$TMP/in.bin"
ORIG=$(shasum -a 256 "$TMP/in.bin" | cut -d' ' -f1)
curl -sf -F "file=@$TMP/in.bin;filename=$NAME" "$BASE/files" | python3 -m json.tool
curl -s "$BASE/cluster" | python3 -m json.tool

step "2. kill node2"
docker compose kill node2
echo "waiting for the metadata server to notice (DEAD_AFTER_SECONDS=10)..."
sleep 13
curl -s "$BASE/nodes" | python3 -c 'import json,sys; [print(f"  {n["id"]}: {n["status"]}") for n in json.load(sys.stdin)]'

step "3. download while node2 is dead"
curl -sf -o "$TMP/out.bin" "$BASE/files/$NAME"
GOT=$(shasum -a 256 "$TMP/out.bin" | cut -d' ' -f1)
if [ "$ORIG" = "$GOT" ]; then
  echo "  ✅ checksums match — file survived the node failure"
else
  echo "  ❌ checksum mismatch"; exit 1
fi

step "4. re-replication"
curl -s "$BASE/cluster" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(f"  under-replicated chunks now: {d["under_replicated_chunks"]}")'
echo "  (with only 2 of 3 nodes alive, rf=3 cannot be met — chunks stay at 2 copies until a node returns)"

step "5. restart node2"
docker compose start node2
sleep 5
curl -s "$BASE/nodes" | python3 -c 'import json,sys; [print(f"  {n["id"]}: {n["status"]}") for n in json.load(sys.stdin)]'
curl -s "$BASE/cluster" | python3 -m json.tool

echo
echo "Done. Try: docker compose logs -f metadata   (to watch heartbeats / re-replication)"
rm -rf "$TMP"
