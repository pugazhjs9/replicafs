# 4. Questions you will be asked

Practice saying these answers out loud. Interviewers care less about the exact
words and more about whether you *understand the trade-off*.

---

**Q: Explain the project in 30 seconds.**

It's a small distributed file system. Files are cut into chunks, each chunk is
stored on three separate nodes, and a central metadata server tracks where each
chunk lives. Nodes send heartbeats; if one goes silent it's marked dead, reads
automatically fall back to the other copies, and a background job re-replicates
the lost chunks onto healthy nodes. Checksums catch corrupted data and soft
delete with versioning lets users undo mistakes. It's the same architecture as
GFS and HDFS, at a size you can read in an afternoon.

---

**Q: Why split into chunks instead of storing whole files?**

Three reasons: (1) a chunk is small enough to re-copy quickly when a node dies —
re-replicating 4 MB is fast, 4 GB is not; (2) chunks of one file spread across
nodes, so reads and writes are parallelisable and no single node is a hotspot;
(3) memory — we only ever hold one chunk in RAM, so file size doesn't matter.

---

**Q: Why 3 replicas? Why not 2 or 5?**

It's a cost/safety dial. 2 copies means one failure leaves you with zero
redundancy while re-replication runs — a second failure in that window loses
data. 3 copies survives two simultaneous failures. 5 costs 67% more disk for
protection against a scenario that's rare. 3 is the industry default (HDFS,
Cassandra, GFS) for exactly this reason. It's a config variable in my system.

---

**Q: How do you detect that a node has failed?**

Heartbeats. Every node POSTs "I'm alive" every 3 seconds. A monitor thread on
the metadata server marks a node DEAD if its last heartbeat is older than 10
seconds. The node doesn't announce its death — it *can't* — so the absence of
heartbeats is the signal.

---

**Q: Why 10 seconds? What if you made it 1 second?**

A short timeout means faster recovery but more false positives: a GC pause or
a network blip would trigger needless re-replication, and then the node comes
back and you have 4 copies to clean up. A long timeout means fewer false alarms
but a longer window with reduced redundancy. 10 s was reasonable for a LAN;
HDFS defaults to 10 minutes because re-replicating a whole DataNode is expensive.

---

**Q: What happens if a node dies in the middle of an upload?**

The PUT to that node fails. If at least one other node accepted the chunk, I
log a warning and continue — the re-replicator will add the missing copy within
seconds. If *no* node accepted it, the upload returns 503. In either case the
database is only written after every chunk is stored, so a failed upload leaves
no half-file visible to the user. I also best-effort delete any chunks that did
land, so they don't become orphans.

---

**Q: What happens if the metadata server dies?**

Everything stops — no uploads or downloads — but no data is lost, because the
chunks are still on the nodes and the metadata is in a database on disk.
Restart it and it carries on. That's the main limitation: it's a single point of
availability, the same as the original GFS master and HDFS NameNode before HA
was added. The fix would be a standby metadata server with a replicated database,
which is on my roadmap.

---

**Q: How do you know a chunk hasn't been corrupted on disk?**

Every chunk's SHA-256 is recorded at upload. On every read, the node reports the
checksum of what's on disk now, and the metadata server also recomputes it over
the bytes it received. Mismatch → that replica is removed from the index, the
read falls back to another copy, and the re-replicator creates a fresh copy from
a verified source. Corrupt bytes are never returned to the client.

---

**Q: A node comes back after being dead. What state is it in?**

Its old replica records were never deleted, so if its disk is intact, its chunks
immediately count as live copies again — possibly giving some chunks 4 copies,
which is harmless. If its disk was wiped, those records are stale; reads handle
that (404 → next replica), but the honest answer is that I should verify a
returning node's chunk list before trusting it. That's a known gap.

---

**Q: How does the system pick which nodes get a chunk?**

Least-loaded first — sort alive nodes by chunk count, take the top 3. I also
increment the count in memory during an upload so consecutive chunks of the same
file spread across nodes rather than all landing on the same three. Real systems
add rack awareness (don't put all 3 copies in one rack) and free-disk checks.

---

**Q: What's the biggest thing you'd change for production?**

Metadata server HA, and replacing the re-replicator's full chunk scan with an
index by node so a dead node's chunks are found in O(its chunks) rather than
O(all chunks). Also garbage collection — soft-deleted files currently hold their
chunks forever.

---

**Q: How did you test it?**

Unit tests for the pure functions, and an end-to-end suite that starts a real
5-process cluster, kills a node, corrupts a chunk on disk, and asserts the
downloads still match byte-for-byte and the cluster returns to full replication.
Plus a shell demo against the Docker stack that does the same thing visibly.

---

**Q: Why FastAPI / Python? Isn't that slow for a storage system?**

For a learning-scale system the bottleneck is disk and network, not Python.
FastAPI gave me streaming uploads/downloads and automatic API docs for free.
If throughput mattered I'd move the storage node to Go or Rust first, because
it's the dumbest component and the one that moves the most bytes.
