# 1. The big idea

## The problem in one sentence

If you save a file on one computer and that computer dies, the file is gone.

That's it. Everything in this project exists to solve that one sentence.

## The obvious fix, and why it's not enough

"Just keep a copy on a second computer."

Good instinct — that's exactly what we do. But the moment you have more than
one computer, new questions appear that a single-computer program never has to answer:

1. **Which computer has my file?** You need a list somewhere.
2. **How do I know a computer is dead?** It can't tell you — it's dead.
3. **When one dies, who makes a new copy?** Otherwise you slowly lose all your copies.
4. **What if a copy got silently damaged?** A bad disk can flip bits without any error.
5. **What if I delete something by mistake?** With copies everywhere, "undo" is harder.

ReplicaFS is a small, readable answer to all five. Google (GFS), Hadoop (HDFS),
and Amazon (S3) answer the same five questions at planet scale. The ideas are
identical; only the size differs.

## The five ideas (memorize these)

### Idea 1 — Chunking: cut the file into pieces

A 1 GB file is not stored as one blob. It's cut into fixed-size pieces (we use
4 MB; the demo uses 1 MB so small files still split). Each piece is called a **chunk**.

Why bother?
- A chunk is small enough to copy around quickly when a node dies.
- Different chunks of one file can live on different computers, spreading the load.
- You never need 1 GB of RAM to handle a 1 GB file — just one chunk at a time.

### Idea 2 — Replication: keep 3 copies of every chunk

Every chunk is written to **3 different computers**. The number 3 is called the
**replication factor**. With 3 copies, two computers can die at the same time
and you still have your data. (HDFS also defaults to 3. Not a coincidence.)

### Idea 3 — Metadata server: one brain that knows where everything is

The computers that hold chunks are called **storage nodes**. They are
deliberately dumb: "store these bytes under this id", "give me the bytes for
this id". They don't know what a *file* is.

One separate program, the **metadata server**, keeps the map:

```
file "photo.jpg" version 1
  chunk 0  →  on node1, node2, node3
  chunk 1  →  on node1, node3, node4
  chunk 2  →  on node2, node3, node4
```

"Metadata" just means "data about the data" — names, sizes, locations — but
never the file contents themselves. GFS calls this the *master*, HDFS calls it
the *NameNode*. Same thing.

### Idea 4 — Heartbeats: silence means death

Every storage node sends a tiny HTTP message to the metadata server every 3
seconds: "I'm node2, I'm alive, I hold 17 chunks." That message is a **heartbeat**.

If the metadata server hasn't heard from node2 for 10 seconds, it marks node2
**DEAD** and stops sending it reads or writes. Nobody told it node2 died. The
*absence* of heartbeats is the signal. This is how almost every distributed
system detects failure.

### Idea 5 — Self-healing: a background loop that fixes the copy count

When node2 dies, every chunk that was on node2 now has only 2 live copies
instead of 3. A background thread in the metadata server (the **re-replicator**)
constantly asks: "Does any chunk have fewer than 3 live copies?" If yes, it tells
a healthy node: "Go fetch chunk X from node1 and keep a copy."

Without this, every failure permanently reduces your safety margin. With it, the
cluster drifts back to full strength on its own.

## Two bonus ideas

**Checksums.** When a chunk is uploaded we compute its SHA-256 hash (a 64-char
fingerprint) and store it in the metadata. Every time we read a chunk back we
recompute the hash. If it doesn't match, the bytes changed on disk — we throw
that copy away, read from another copy, and let the re-replicator make a fresh one.

**Soft delete + versions.** Deleting a file only sets a `deleted_at` timestamp;
nothing is physically removed, so `restore` is just clearing that timestamp.
Uploading a name that already exists creates version 2; version 1 stays
downloadable. So "I accidentally overwrote it" is also recoverable.

## Picture to keep in your head

```
                      you (curl / browser / app)
                               │
                               │  upload / download / list / delete
                               ▼
                     ┌───────────────────┐
                     │  metadata server  │   knows WHERE everything is
                     │  (the brain)      │   never holds file bytes
                     └───────────────────┘
                        ▲    │     ▲
            heartbeats  │    │     │  "store / give me chunk <id>"
            every 3 s   │    ▼     ▼
              ┌──────┐  ┌──────┐  ┌──────┐
              │node1 │  │node2 │  │node3 │   dumb boxes full of chunks
              └──────┘  └──────┘  └──────┘
```

Next: [02-how-it-works.md](02-how-it-works.md) — what actually happens, step by step.
