# 2. How it works, step by step

Four stories. Follow each one with the diagram from doc 1 in your head.

## Story A — You upload a 5 MB file

`curl -F file=@movie.bin localhost:8000/files`

1. The metadata server asks its database: *which nodes are ALIVE right now?*
   Say node1, node2, node3.
2. It checks: *does a file called `movie.bin` already exist?* If yes, this
   upload becomes version 2. If not, version 1.
3. It reads the first 1 MB from the upload stream. That's **chunk 0**.
4. It computes `sha256(chunk 0)` and generates a random id for the chunk.
5. It picks 3 nodes — the ones currently holding the *fewest* chunks, so load
   spreads evenly — and does `PUT http://nodeX:800X/chunks/<id>` to each one,
   sending the bytes plus the checksum in a header.
6. Each node recomputes the checksum on arrival. If it matches, it writes the
   bytes to a temp file and renames it into place. (Rename is atomic on every
   OS, so a crash mid-write never leaves a half chunk that looks real.)
7. The metadata server remembers in memory: "chunk 0 → node1, node2, node3".
8. Repeat from step 3 for chunks 1, 2, 3, 4.
9. **Only now** does it write the File + Chunk + Replica rows into the database,
   in one transaction. This is the **commit point**.

Why step 9 matters: if anything fails at steps 3–8 (a node dies, the client
disconnects), nothing was ever written to the database, so from the user's
point of view the file simply doesn't exist. No half-files. The code also
makes a best-effort attempt to delete the orphaned chunks from the nodes.

What if only 2 of the 3 PUTs succeed? We accept it, log a warning, and the
re-replicator makes the third copy in a few seconds. What if 0 succeed? The
upload fails with HTTP 503 — we never pretend to have stored something we didn't.

## Story B — You download it

`curl -o copy.bin localhost:8000/files/movie.bin`

1. Find the latest non-deleted version of `movie.bin`.
2. For each chunk in order, build a list of the replicas that are on **ALIVE** nodes.
   If any chunk has zero live replicas → HTTP 503 right away ("chunk has no live replica").
3. Start streaming. For chunk 0: ask the first live node. Recompute the checksum
   on the bytes it returns.
   - Matches → send those bytes to the client, move to chunk 1.
   - 404 / timeout → try the next node.
   - Checksum wrong → **this copy is corrupt.** Delete its Replica row from the
     database, try the next node. The re-replicator will notice this chunk now
     has 2 copies and make a new one.
4. The client receives the chunks back-to-back, which is exactly the original file.

Note the design choice: we never return bytes we haven't verified. Silent
corruption is impossible to pass through.

## Story C — node2 dies

`docker compose kill node2`

```
t = 0 s    node2 is killed. Its heartbeat thread dies with it.
t = 3 s    metadata server expected a heartbeat. None arrived. (Nothing happens yet —
           maybe it's just a slow network.)
t = 5 s    monitor thread runs: node2 last heard 5 s ago, limit is 10 s → still ALIVE.
t = 10 s   monitor thread runs: node2 last heard 10 s ago → mark DEAD. Log a warning.
t = 10 s   same monitor pass: scan every chunk. Any chunk whose live copies < 3?
           Every chunk that was on node2 → yes, now 2 live copies.
           Is there an ALIVE node that doesn't already hold it? (needs ≥ 4 nodes)
             → tell that node: POST /replicate {chunk_id, source_url: node1, checksum}
             → that node downloads from node1, verifies checksum, saves.
             → add a Replica row. Chunk back to 3 live copies.
```

Meanwhile, any download that happens between t=0 and t=10 still works: the
download code tries node2, gets a connection error, moves on to node1 or node3.
The user never notices.

**Why wait 10 seconds? Why not mark DEAD after one missed heartbeat?**
A single missed heartbeat might just be a network hiccup. If we declared death
too eagerly, we'd start copying chunks around for nothing, and the node would
come back 2 seconds later. The 10 s window is a trade-off: longer = fewer false
alarms, shorter = faster recovery. HDFS uses 10 *minutes* by default.

## Story D — node2 comes back

`docker compose start node2`

1. Its heartbeat thread starts, posts to the metadata server.
2. Metadata server: "node2 was DEAD, now ALIVE" — flips the status.
3. Its old Replica rows were never deleted, so the chunks it still has on disk
   instantly count as live copies again. Some chunks may now have 4 copies.
   That's harmless (just wasted disk) and cleaning it up is on the roadmap.

What if node2's disk was wiped while it was down? Then its Replica rows are
lies — the metadata thinks node2 has chunks it doesn't. Today the download code
handles this gracefully (gets 404, tries the next node). The proper fix —
verifying a returning node's chunks before trusting it — is a planned next step
and a great thing to mention in an interview as "a limitation I'm aware of".

## The database, in plain words

Four tables:

```
nodes     one row per storage node: id, url, ALIVE/DEAD, last heartbeat time
files     one row per (name, version): size, whole-file checksum, deleted_at
chunks    one row per chunk: which file, position (0,1,2…), size, checksum
replicas  one row per (chunk, node) pair: "this chunk lives on this node"
```

Everything the system knows is in those four tables. The file *bytes* are only
on the nodes' disks.

Next: [03-code-walkthrough.md](03-code-walkthrough.md) — the actual code, file by file.
