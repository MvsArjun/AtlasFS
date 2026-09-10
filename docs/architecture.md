# AtlasFS architecture

## Current milestone

AtlasFS currently contains a dependency-free protocol library and a Linux/POSIX
TCP transport library, plus a persistent `ObjectStore`. `TcpClient` encodes
requests, sends complete frames, and uses `StreamParser` to receive responses.
`TcpServer` listens only on loopback,
accepts connections sequentially, parses arbitrary receive chunks, invokes a
request handler, and reliably sends encoded responses. `StorageRequestHandler`
decodes operation payloads and delegates filesystem work to `ObjectStore`. There
is no replication or networked coordinator yet.

AtlasFS also has an in-process control-plane foundation:

```text
                 MetadataCoordinator
                         |
              +----------+----------+
              |                     |
         NodeRegistry       RendezvousPlacement
              |                     |
              +----------+----------+
                         |
                  PlacementPlan
```

The existing TCP, handler, and `ObjectStore` path is the data plane. The metadata
coordinator, registry, and placement policy form the control plane. They do not
perform filesystem or network I/O, and the data plane does not yet consult them.

The parser consumes arbitrary byte fragments because TCP preserves byte order but
not application message boundaries. It may emit zero, one, or many messages per
call. Incomplete frames remain buffered. A malformed header is a connection-level
protocol error: the parser enters a failed state until reset, preventing unsafe
attempts to resynchronize within an untrusted byte stream.

## Decisions

### Explicit binary framing

AtlasFS uses a fixed-width, big-endian header followed by an opaque payload. This
is compact, deterministic, and exercises the actual stream-framing problem. HTTP
was rejected as the internal protocol because it adds semantics AtlasFS does not
need and obscures this foundational systems concern. A schema framework such as
Protocol Buffers remains an option for future payloads, but is unnecessary for
the envelope.

### Bounded payloads

The parser rejects declared payloads larger than a configurable limit before
waiting for their bytes. This bounds memory exposure from malformed or hostile
peers. The initial default is 16 MiB; large objects will later be split into
bounded chunks rather than increasing frame sizes without limit.

### Single-threaded parser ownership

`StreamParser` has no internal locking. One connection handler owns one parser.
This keeps synchronization outside a low-level value parser and makes ownership
clear.

### Blocking sequential transport

The server uses blocking POSIX sockets and handles one connection at a time. This
is the smallest execution model that proves real TCP framing and lifecycle
correctness. `recv()` output is passed directly to `StreamParser`; the transport
does not duplicate framing logic. `send_all()` retries interrupted calls and
continues after short writes. A move-only `Socket` closes its descriptor exactly
once.

The listening socket binds to `127.0.0.1`; port zero is supported and the chosen
port is obtained with `getsockname()`. `stop()` shuts down the listener and any
active connection so a test or owner can deterministically unblock the server.
Malformed, oversized, and incomplete frames close only their connection. The
server continues accepting later connections.

The default transport handler remains storage-free. For request message types it
returns a `response` with the same request ID, flags, and payload. Receiving a
response-type message where a request is expected produces an `error_response`.
Storage servers instead install `StorageRequestHandler`.

### Persistent chunked object store

The configured root uses separate committed-manifest, immutable-generation, and
transaction namespaces:

```text
objects/
├── committed/<object-id>                 # authoritative manifest
├── chunks/<generation-id>/
│   ├── manifest                          # identical protected manifest
│   ├── chunk_000000
│   ├── chunk_000001
│   └── ...
└── transactions/<generation-id>/         # never visible as an object
```

Every object, including empty and small objects, uses this representation. The
chunk size is configurable and defaults to 1 MiB. Generation IDs are independent
of logical object IDs so later placement and replication work need not rename the
physical chunk namespace.

Identifiers use a strict ASCII allowlist and cannot contain path separators,
absolute-path syntax, traversal components, or the internal temporary-file prefix.
GET opens files with `O_NOFOLLOW` and accepts only regular files.

The explicit big-endian manifest is:

```text
4 bytes  magic "AMNF"
2 bytes  format version (1)
2 bytes  reserved zero
8 bytes  object length, big endian
4 bytes  configured chunk size
4 bytes  chunk count
4 bytes  whole-object CRC-32C
2 bytes  generation-ID length
N bytes  generation ID
for each chunk: 4-byte index, 4-byte length, 4-byte CRC-32C
4 bytes  CRC-32C of every preceding manifest byte
```

PUT verifies the client checksum, creates a private transaction directory, writes
and syncs every chunk, writes and syncs the manifest, syncs the transaction
directory, renames it into the immutable generation namespace, and syncs that
parent. It then writes and syncs a temporary committed manifest, atomically
renames that file over the object ID, and syncs the committed directory. The final
manifest rename is the visibility boundary, so a failed overwrite leaves the old
manifest and generation reachable. Old generations are garbage-collected only
after the new manifest commits.

On construction, the store removes transaction contents, temporary committed
manifests, and generation directories not referenced by a valid committed
manifest. It never promotes incomplete work. This establishes atomic namespace
visibility and best-effort durability using Linux filesystem primitives; it does
not claim protection from filesystems or hardware that violate `fsync()`/rename
guarantees.

GET validates manifest integrity and mathematical consistency, rejects unexpected
generation entries, reads chunks by numeric index, validates exact chunk lengths
and CRC-32C values, reassembles the object, and finally validates its whole-object
CRC-32C. DELETE unlinks the committed manifest first, syncs it, then removes its
immutable generation. DELETE is idempotent with respect to final state,
while its result distinguishes the first successful removal from later
`not_found` attempts.

### Membership and placement

A storage node has a stable validated node ID, host, port, and coordinator-owned
state. ACTIVE nodes are eligible for new placement. DRAINING nodes remain known
but receive no new chunks, while UNAVAILABLE nodes are likewise excluded. No
health detector changes these states yet.

`NodeRegistry` uses node-ID ordering, so listing and placement input never depend
on unordered-container iteration. Every successful add, remove, or actual state
change increments an in-process 64-bit membership version. Rejected mutations and
no-op state assignments do not. Placement plans record the version used.

Chunk identity is the length-delimited tuple `(object ID, generation ID, chunk
index)`. Placement uses rendezvous hashing: AtlasFS computes explicitly defined
64-bit FNV-1a scores for each eligible node and selects the greatest score, with
node ID as a deterministic tie-breaker. FNV-1a is used for stable placement, not
security. This is reproducible across insertion orders and processes. Adding a
node only keeps an existing primary or moves it to the new node; removal affects
only chunks assigned to the removed node. No migration is performed.

`ObjectMetadata` reuses the durable manifest's size and integrity fields without
putting placement into that manifest. `PlacementPlan` separately attaches one
primary node to each generation-aware chunk and preserves the manifest metadata.
Its single-primary field can later evolve into a replica list without changing
chunk identity or the local manifest format.

Membership, states, and placement plans are intentionally in-memory and assume
single-threaded access. Object data and manifests remain durable.

## Intended evolution

The next dependency is replicated chunk writes using an explicit fixed membership
snapshot and failure semantics. Coordinator networking, persistent membership,
production concurrency, and automatic recovery remain deferred.
