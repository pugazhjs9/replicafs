# Learn ReplicaFS from the beginning

You do not need distributed-systems experience. Start with the words below, run one command at a time, and read the output before moving on.

## What are you building?

A backend is the part of an application that receives requests, does work, and returns results. ReplicaFS receives files through HTTP and keeps copies of their pieces on several storage programs. A different program remembers which pieces belong to each file.

Think of a notebook cut into numbered pages. You photocopy each page three times and store the copies in different cupboards. An index tells you where each page is. Losing one cupboard should not lose the notebook.

## The words you need first

| Word | Simple meaning | Example here |
|---|---|---|
| Process | A program that is currently running. | One Uvicorn storage server. |
| Server | A program that waits for requests. | The metadata API on port 8000. |
| Client | A program making a request. | `curl`, your browser, or a Python script. |
| HTTP | Rules for requests and responses between programs. | `GET /files/hello.txt`. |
| API / endpoint | An agreed way to ask a program to do something / one address for doing it. | `POST /files` accepts uploads. |
| Port | A numbered entrance into a computer's networking. | 8000 and 8001 are different entrances. |
| Node | One member of the storage cluster. | `node1`. |
| Cluster | Programs working together as one service. | One metadata process plus four storage processes. |
| Chunk | One piece of a file. | A 1 MiB slice in the demo. |
| Replica | One stored copy of a chunk. | The copy of chunk 0 on node2. |
| Metadata | Information describing the actual data. | File names and chunk locations. |
| Database | Structured information stored for later queries. | SQLite stores the metadata tables. |
| Heartbeat | A repeated “I am alive” message. | A node reports every three seconds. |
| Checksum | A fingerprint calculated from bytes. | SHA-256 detects a changed chunk. |
| Version | A particular saved revision of a file. | `notes.txt` version 1 and version 2. |
| Commit | A recorded unit of change. | A Git milestone, or a completed database transaction. |
| Environment variable | A setting supplied to a process when it starts. | `NODE_ID=node2`. |
| Container / volume | An isolated process environment / persistent storage outside its writable layer. | Docker runs node1 with its own volume. |

“MiB” means 1,048,576 bytes. “MB” often means 1,000,000 bytes. The code's default chunk is 4 MiB; the demo's chunk is 1 MiB.

## Read and practice in this order

| Step | Read | Then do |
|---|---|---|
| 1 | [The big idea](01-the-big-idea.md) | Draw one file, two chunks, three copies each. |
| 2 | [Hands-on setup](05-try-it-yourself.md) | Start the cluster and upload `hello.txt`. |
| 3 | [How it works](02-how-it-works.md) | Trace one upload and download on your drawing. |
| 4 | [Code walkthrough](03-code-walkthrough.md) | Open the referenced source files beside the guide. |
| 5 | [API reference](06-api-reference.md) | Upload two versions, delete, and restore. |
| 6 | [Testing guide](07-testing.md) | Run the tests and inspect a failure scenario. |
| 7 | [Deployment guide](08-deployment.md) | Explain why four containers on one laptop are not four failure domains. |
| 8 | [Interview guide](04-interview-questions.md) and [milestones](09-milestones-and-resume.md) | Give a five-minute demonstration without reading a script. |

## How to read a command

```bash
curl --fail -F file=@hello.txt http://127.0.0.1:8000/files
```

`curl` makes an HTTP request. `--fail` makes an HTTP error cause a nonzero exit code, so scripts notice failures. `-F` sends a form; `file` is the form field's name; `@hello.txt` means read that local file. `127.0.0.1` means your own computer. `8000` is the port. `/files` is the API endpoint. The response is JSON: structured text containing values such as the version number and checksum.

A line starting with `#` in a shell block is a comment. Run commands from the repository root unless told otherwise. A command with a foreground server keeps running; open a second terminal for later client commands. Stop the server with Ctrl+C.

## Your first learning checkpoint

Before moving on, explain these in your own words:

1. Why do we need both chunk bytes and a metadata database?
2. What would happen if three copies were stored on the same physical disk?
3. Why does a node stop sending heartbeats when its process stops?
4. Why can a checksum detect a bad copy but not recreate the missing bytes?
5. Why is a passing demo evidence of a behavior, rather than proof that the system never fails?
