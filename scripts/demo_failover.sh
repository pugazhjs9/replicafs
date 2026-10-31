#!/usr/bin/env bash
# Run from the repo root after: docker compose up -d --build
# If enabled, export API_TOKEN to match the metadata container before running.
set -euo pipefail

BASE=${BASE:-http://127.0.0.1:8000}
DEMO_TMP=$(mktemp -d)
NAME="replicafs-demo-$(date +%s)-$$.bin"
STOPPED=0
AUTH=(-H "Accept: */*")
if [ -n "${API_TOKEN:-}" ]; then AUTH=(-H "Authorization: Bearer $API_TOKEN"); fi

cleanup() {
  if [ "$STOPPED" -eq 1 ]; then docker compose start node2 >/dev/null || true; fi
  python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1])' "$DEMO_TMP"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_state() {
  python3 - "$BASE" "${API_TOKEN:-}" "$1" <<'PY'
import json, sys, time, urllib.request
base, token, state = sys.argv[1:]
headers = {"Authorization": "Bearer " + token} if token else {}
deadline = time.monotonic() + 90
while time.monotonic() < deadline:
    try:
        def get(path):
            request = urllib.request.Request(base + path, headers=headers)
            with urllib.request.urlopen(request, timeout=3) as response:
                return json.load(response)
        nodes = {n["id"]: n["status"] for n in get("/nodes")}
        cluster = get("/cluster")
        if state == "ready":
            ok = sum(s == "ALIVE" for s in nodes.values()) >= 4
        elif state == "healed":
            ok = (nodes.get("node2") == "DEAD"
                  and cluster["under_replicated_chunks"] == 0
                  and cluster["unavailable_chunks"] == 0)
        else:
            ok = nodes.get("node2") == "ALIVE"
        if ok:
            print(json.dumps(cluster, indent=2))
            break
    except (OSError, ValueError, KeyError):
        pass
    time.sleep(0.5)
else:
    raise SystemExit("Timed out waiting for " + state + ". Check docker compose logs metadata.")
PY
}

printf '\n1. Wait for four live storage nodes\n'
wait_state ready
python3 - "$DEMO_TMP/in.bin" <<'PY'
import os, pathlib, sys
pathlib.Path(sys.argv[1]).write_bytes(os.urandom(5 * 1024 * 1024 + 7))
PY
printf '\n2. Upload a multi-chunk file\n'
curl --fail --silent --show-error "${AUTH[@]}" -F "file=@$DEMO_TMP/in.bin;filename=$NAME" "$BASE/files" | python3 -m json.tool
curl --fail --silent --show-error "${AUTH[@]}" -o "$DEMO_TMP/before.bin" "$BASE/files/$NAME"
cmp "$DEMO_TMP/in.bin" "$DEMO_TMP/before.bin"

printf '\n3. Stop node2, then download using surviving replicas\n'
STOPPED=1
# Stop keeps the node down with a restart policy; absent heartbeats trigger detection.
docker compose stop node2
curl --fail --silent --show-error "${AUTH[@]}" -o "$DEMO_TMP/after.bin" "$BASE/files/$NAME"
cmp "$DEMO_TMP/in.bin" "$DEMO_TMP/after.bin"
printf 'Byte-for-byte download matches while node2 is stopped.\n'

printf '\n4. Wait for three-copy redundancy while node2 stays DEAD\n'
wait_state healed
curl --fail --silent --show-error "${AUTH[@]}" -o "$DEMO_TMP/healed.bin" "$BASE/files/$NAME"
cmp "$DEMO_TMP/in.bin" "$DEMO_TMP/healed.bin"

printf '\n5. Start node2 again\n'
docker compose start node2
STOPPED=0
wait_state returned
printf '\nPASS: bytes survived failure and redundancy recovered before the node returned.\n'
printf 'The uploaded demo file remains available as %s.\n' "$NAME"
