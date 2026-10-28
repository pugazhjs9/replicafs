# 5. Try it yourself

Nothing teaches a distributed system like breaking one. Each exercise takes a
few minutes. Keep a second terminal open with:

```bash
docker compose logs -f metadata
```

so you can watch the server's decisions as they happen.

## Setup

```bash
cd ~/Desktop/replicafs
docker compose up -d --build
curl localhost:8000/nodes        # wait until all 3 say ALIVE
```

## Exercise 1 — watch a file become chunks

```bash
head -c 3500000 /dev/urandom > big.bin        # 3.5 MB
curl -F file=@big.bin localhost:8000/files
curl localhost:8000/cluster
```

Questions: how many chunks? (3.5 MB / 1 MB chunk size → 4, the last one partial.)
How many replicas? (4 × 3 = 12.) Open http://localhost:8000/docs and try the same
calls from the browser.

## Exercise 2 — look inside a node

```bash
docker compose exec node1 ls -la /data
```

Those filenames are chunk ids. Nothing tells you which file they belong to —
only the metadata server knows. That's the "dumb node" idea made visible.

## Exercise 3 — kill a node, keep downloading

```bash
docker compose kill node3
curl -o out.bin localhost:8000/files/big.bin
cmp big.bin out.bin && echo IDENTICAL
curl localhost:8000/nodes          # within ~10 s node3 shows DEAD
curl localhost:8000/cluster        # under_replicated_chunks > 0
```

In the log terminal you'll see: `node node3 missed heartbeats for 10s -> DEAD`.

## Exercise 4 — bring it back

```bash
docker compose start node3
curl localhost:8000/cluster        # under_replicated_chunks back to 0
```

Log line: `node node3 is back ALIVE`.

## Exercise 5 — corrupt a chunk on purpose

```bash
# pick any chunk file on node1 and overwrite its first bytes
docker compose exec node1 sh -c 'f=$(ls /data | head -1); printf "GARBAGE" | dd of=/data/$f conv=notrunc 2>/dev/null; echo corrupted $f'
curl -o out.bin localhost:8000/files/big.bin
cmp big.bin out.bin && echo "STILL IDENTICAL"
```

Watch the log: `CORRUPT replica of ... on http://node1:8001 — dropping it, trying next`,
then a few seconds later `re-replicated chunk ...`. The system healed itself.

## Exercise 6 — versions and undo

```bash
echo "draft 1" > notes.txt;  curl -F file=@notes.txt localhost:8000/files
echo "draft 2" > notes.txt;  curl -F file=@notes.txt localhost:8000/files
curl localhost:8000/files/notes.txt                 # draft 2
curl "localhost:8000/files/notes.txt?version=1"     # draft 1
curl -X DELETE localhost:8000/files/notes.txt
curl localhost:8000/files/notes.txt                 # 404
curl -X POST localhost:8000/files/notes.txt/restore
curl localhost:8000/files/notes.txt                 # draft 2 again
```

## Exercise 7 — add a 4th node (the real re-replication demo)

Copy the `node3` block in `docker-compose.yml`, rename it `node4` with port 8004,
then `docker compose up -d`. Now repeat Exercise 3: kill node3 and watch
`under_replicated_chunks` go from N back to **0 while node3 is still dead** —
because node4 is receiving copies. That's the re-replicator doing its real job.

## Exercise 8 — change the rules

Edit `docker-compose.yml`:
- `DEAD_AFTER_SECONDS: "3"` → how fast does death detection become? Any false alarms?
- `REPLICATION_FACTOR: "2"` → kill one node; how exposed are you now?
- `CHUNK_SIZE: "262144"` (256 KB) → upload the same file; how many chunks?

Each change is one line and the behaviour difference is immediately visible.

## Exercise 9 — run the automated proof

```bash
.venv/bin/python -m pytest -v
```

Read `tests/test_e2e.py` alongside the output. Every scenario above has a test.

## Cleanup

```bash
docker compose down -v      # -v also deletes the stored chunks
```
