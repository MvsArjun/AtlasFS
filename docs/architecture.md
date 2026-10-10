# AtlasFS architecture

## Current milestone

AtlasFS currently contains a dependency-free protocol library and a Linux/POSIX
TCP transport library, plus a persistent `ObjectStore`. `TcpClient` encodes
requests, sends complete frames, and uses `StreamParser` to receive responses.
`TcpServer` listens only on loopback,
accepts connections sequentially, parses arbitrary receive chunks, invokes a
request handler, and reliably sends encoded responses. `StorageRequestHandler`
decodes operation payloads and delegates filesystem work to `ObjectStore`. The
wire-facing path remains single-node; replication is deliberately in-process.

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
                         |
                  ReplicaManager
                  /      |      \
                 v       v       v
          StorageNode A  B       C
                 |       |       |
            ObjectStore ObjectStore ObjectStore
```

The coordinator, registry, and placement policy are the control plane and never
touch storage. `ReplicaManager` is the replication layer. `LocalStorageNode` is
the storage boundary and owns one independent `ObjectStore` root. The existing
TCP path remains a separate transport integration.

```text
                   ReplicaManager
                         |
              +----------+----------+
              |                     |
       LocalStorageNode     RemoteStorageNodeClient
              |                     |
          ObjectStore         StorageNodeServer
                                    |
                                ObjectStore
```

The remote path reuses the 24-byte frame and incremental parser. Its storage
protocol is separate from logical-object PUT/GET/DELETE. A durable operation
journal fingerprints PREPARE and records PREPARED, COMMITTED, or ABORTED. Exact
duplicates are deterministic; conflicting ID reuse and invalid transitions are
rejected. On restart unfinished PREPARE is conservatively recovered as ABORTED.
If a COMMIT response is lost, the client reports an unknown result and the
manager does not blindly roll it back; QUERY_OPERATION resolves durable state.

Remote repair publication uses the same `StorageNode` publisher boundary as local
repair and message types 212/213. The bounded request contains no path: it binds
the repair plan and execution IDs, intended target node, object and generation,
complete protected manifest, exact chunk metadata, and bytes. The server rejects
wrong-node requests before storage access and derives every location through
`ObjectStore`'s validated layout. The existing temporary-write, file-sync,
reread, checksum, atomic-rename, and directory-sync path provides publication.

The storage node durably fingerprints each execution under its repair journal.
An exact duplicate returns the recorded result after checking successful physical
state; conflicting execution-ID reuse is rejected. A restart ignores incomplete
journal temporary files, while `ObjectStore` removes only its clearly named
incomplete repair artifacts. A bounded socket timeout prevents indefinite waits.
After a mutating request is sent, transport failure or a malformed/lost response
is `REMOTE_RESULT_UNKNOWN`, because publication may already have occurred. There
is no automatic resend.

### Distributed operation recovery

`RecoveryCoordinator` consumes the same `StorageNode` interface as replication,
so local and remote participants are queried uniformly. It always queries every
named participant and sorts observations by node ID. Returned operation IDs and
durable fingerprints are checked before classifying the snapshot as all committed,
all aborted, all prepared, mixed prepared/committed, mixed prepared/aborted,
mixed terminal, unavailable, unknown, or identity conflict.

Inspection never mutates participant state. Explicit completion first takes a
fresh snapshot and may commit only PREPARED nodes when no aborted, unavailable,
unknown, or conflicting participant exists. Explicit rollback may abort PREPARED
nodes only when no participant is committed or unavailable. Partial actions are
reported and never retried automatically. In particular, unavailable is not
equivalent to aborted, and durable COMMITTED state is never "uncommitted."

### Durable coordinator operation records

The coordinator stores one control-plane record per tracked operation in
`coordinator/operations/<operation-id>`. The versioned big-endian `ACOP` format
contains the operation ID and type, fingerprint, object and generation IDs,
placement-plan identity, historical membership version, replication factor,
immutable participant-to-chunk mapping, lifecycle, and latest participant
observations. A CRC-32C protects the complete record body. Publication uses a
fully written and synchronized temporary file, atomic rename, and parent-directory
synchronization, so startup sees an old valid record or a new valid record rather
than a partial update. Invalid published records stop startup and are not deleted.

These records preserve coordinator knowledge and observations; they are not a
consensus log and never override participant journals. Construction captures the
original placement rather than consulting current membership after restart, so a
removed or unavailable historical node is retained and is never substituted.
Startup only reconstructs the index. An explicit inspection queries that exact
participant set through `RecoveryCoordinator`, replaces stale observations, and
persists the resulting classification. Completion and rollback remain explicit
operator actions; restart performs no automatic decision, retry, or repair.

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

Membership is durably published as a strict, CRC-protected `AMBR` record at
`coordinator/membership/registry`. Mutations are applied to a candidate registry,
advance its monotonic version, and become visible only after temporary-file
write, file synchronization, atomic rename, and directory synchronization.
Malformed published state stops startup without deletion.

Administrative state and health are independent inputs to eligibility. An ACTIVE
node is eligible only when its health is freshly verified as HEALTHY; DRAINING
remains an operator decision even if heartbeats fail. Each synchronous heartbeat
verifies the returned node identity. A bounded consecutive-failure threshold
moves health through SUSPECT to UNAVAILABLE, while a later successful check only
restores health and performs no repair or data movement. Persisted health is a
last-known observation and is marked stale after coordinator restart.

Current membership versions affect only new placement. Durable operation records
retain their historical membership version and original participant-to-chunk
mapping, so failure detection, removal, addition, or recovery of a node cannot
rewrite old recovery participants. No health transition automatically
commits, aborts, retries, or repairs a distributed operation.

### Durable current-replica catalog

The authoritative view of present replica placement is a single snapshot at
`coordinator/replicas/catalog`. It is deliberately separate from historical
`ACOP` operation records and `ARPL` repair records: those records explain what an
operation attempted and what a repair observed, while the catalog answers which
replicas are current now. Catalog publication never edits or reinterprets either
historical journal.

The version-1 `ARPC` format is deterministic, bounded, and big-endian. Its header
contains the magic, format version, zero-valued reserved fields, global catalog
version, and record count. Each identity-sorted record contains object ID,
generation ID, chunk index, chunk length and CRC-32C, the catalog version of its
last update, and a node-ID-sorted replica set. A replica entry records its node
ID, typed state (`PRESENT`, `MISSING`, `CORRUPT`, or `UNAVAILABLE`), integrity
checksum, and evidence version. Explicit lengths delimit every variable field,
and a final CRC-32C covers the complete preceding snapshot.

Decoding rejects unknown versions, nonzero reserved fields, truncation, trailing
bytes, invalid or duplicate identities, unsorted or duplicate replicas, invalid
states and identifiers, inconsistent chunk metadata, invalid state-specific
integrity evidence, impossible counts or sizes, and checksum corruption. Startup
reconstructs the in-memory index only from a complete valid snapshot. A corrupt
published file stops construction and remains on disk for diagnosis; it is not
deleted, repaired, or replaced automatically.

Each actual upsert or erase advances the global 64-bit version exactly once;
rejected requests and no-op upserts do not. A live record's version identifies
its last published mutation, while the monotonically increasing snapshot version
prevents a removed or superseded version from being reused. Mutations are first
applied to a candidate snapshot, written and synchronized through a temporary
file, atomically renamed, and followed by parent-directory synchronization. A
failure before rename leaves the prior in-memory snapshot authoritative. A
failure after rename makes crash durability unknowable, so the process exposes
`DURABILITY_UNKNOWN` and rejects all later mutations rather than guessing which
version survived.

Compare-and-swap is the atomic unit for a single chunk. A request names the exact
global catalog version and full expected replica state plus the desired replica
state.
The catalog validates all three under its coordinator-local mutex, then publishes
the candidate with the existing write, sync, rename, and directory-sync protocol.
A success advances the global version and only the changed record's version. A
stale version or state returns a typed conflict together with the current record;
an identical desired state is a no-op. Pre-rename failure retains the old state,
while post-rename directory-sync failure exposes `DURABILITY_UNKNOWN` and poisons
later mutations in that process.

This serialization supports concurrent logical writers within one coordinator;
it is not a distributed lock. The composition root must explicitly populate the
catalog and enforce one active catalog owner. Cross-process writers, leases, and
consensus remain unsupported.

### Operator-directed repair planning

`RepairPlanner` is a read-only control-plane component. For each committed plan
chunk it starts from the matching authoritative catalog record, then compares the
requested replication factor with replicas on ACTIVE, freshly HEALTHY nodes.
Sources and targets are selected deterministically by node ID; a target must be
eligible and must not already appear in the catalog record. Missing healthy
sources become UNRECOVERABLE plans, while target exhaustion is retained as an
explicit no-eligible-target reason rather than silently ignored.

Each `ARPL` v3 repair record binds object, generation, chunk, membership version,
a checksum of the complete health evidence, and the exact global catalog version
and replica state observed during planning. Validation detects membership,
health, generation, chunk, or catalog changes as stale. Records are strictly
parsed, CRC-32C protected, and atomically published beneath
`coordinator/repairs/`. The store reconstructs records on restart and rejects
corruption without deletion. Legacy v1 and v2 records remain readable but cannot
be approved for execution because they lack catalog evidence. Planning does not
contact storage nodes, copy chunks, mutate manifests or membership, retry
operations, or alter historical operation participants.

### Operator-approved repair execution

`RepairExecutor` is separate from the read-only planner. An operator must durably
transition a plan from PLANNED to APPROVED before execution. Under a local mutex,
the executor checks the current membership and health evidence, exact generation
manifest, source chunk, target absence or equivalence, and catalog evidence. It
persists EXECUTING before data movement and performs a final evidence check
immediately before initiating the transfer.

`ObjectStore::publish_repair_chunk` validates the complete manifest and chunk,
writes and synchronizes temporary files, rereads their CRC-protected content, and
only then atomically publishes them. `LocalStorageNode` and
`RemoteStorageNodeClient` implement the same typed publisher operation, so the
executor does not branch on location. It rereads the target before issuing a
catalog compare-and-swap with the version and full replica state captured by the
plan. Only confirmed physical publication followed by a durably committed CAS
permits COMPLETED. A conflict records STALE with a typed conflicting-repair
error; post-rename catalog uncertainty and remote-result uncertainty are distinct
FAILED outcomes. None is retried, replanned, or allowed to select a new target.
When the observed set is already at the requested replication factor, the CAS
replaces one deterministically selected unhealthy or non-present entry with the
verified target; otherwise it adds the target without rewriting healthy entries.

ARPL v3 stores catalog evidence in addition to approval, execution state, step
outcomes, execution identity, and typed terminal failure. An EXECUTING record
found after restart is never resumed automatically and exposes the completed
steps for inspection. Physical publication is deliberately distinct from
authoritative catalog commitment. If the CAS fails, the verified target remains
present but non-authoritative for later operator-directed reconciliation. AtlasFS
serializes catalog writers within one coordinator, not independent processes.

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
64-bit FNV-1a scores for each eligible node, sorts by descending score, and uses
node ID as a deterministic tie-breaker. The top distinct R nodes form the ordered
replica list. FNV-1a is used for stable placement, not security. Results are
reproducible across insertion orders and processes. Membership changes may alter
desired replicas, but perform no migration or repair.

`ObjectMetadata` reuses the durable manifest's size and integrity fields without
putting placement into that manifest. `PlacementPlan` separately attaches ordered
replica node IDs to every generation-aware chunk and preserves the manifest
metadata, requested replication factor, membership version, and explicit
under-replication flag.

### In-process replicated storage

`ReplicaManager` groups a plan by node and sends only the chunks assigned to that
node. Every replica uses the plan's generation ID. `ObjectStore` stages selected
chunks and the protected logical manifest in a private transaction, syncs and
verifies them, and moves them into the immutable generation namespace. Only after
all required nodes prepare does the manager commit each node's manifest.

A prepare or commit failure aborts every participant. Already committed nodes
atomically restore the previous manifest before removing the failed generation;
rollback deletion failures are reported separately. Reads inspect replicas in
deterministic order, recording missing or corrupt copies while using a later valid
copy, and verify the reassembled object's CRC-32C. Deletes visit every known node;
`not_found` is idempotent success and other failures produce `partial_delete`.
Degraded reads and membership changes never trigger automatic repair.

Membership and health observations are durable and assume single-threaded access.
Placement plans remain in-memory except for identity and participants captured by
durable operation records. Object data and manifests remain durable.

## Intended evolution

The next dependency is distributed repair crash recovery and reconciliation,
especially coordinator crashes between physical publication and authoritative
catalog commitment. Consensus, cross-process catalog writers, automatic retries,
and automatic operation recovery remain deferred.
