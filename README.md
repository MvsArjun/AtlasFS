# AtlasFS

AtlasFS is a C++20/Linux object-storage systems project. The current milestone
provides a documented binary wire format, incremental stream parser, blocking
POSIX TCP transport, and persistent single-node PUT, GET, and DELETE operations.
Objects survive server restarts and are verified with CRC-32C before being
returned. Storage splits every logical object into configurable chunks (1 MiB by
default), protects each chunk and the whole object, and commits through a durable
binary manifest.

An in-process metadata control plane models stable storage-node identities,
versioned membership, and deterministic top-R rendezvous placement. A separate
`ReplicaManager` executes those plans against independent `LocalStorageNode`
roots. Replicated PUT uses prepare/verify/commit with rollback, GET falls back
across missing or corrupt chunk replicas, and DELETE reports partial failure.

The storage-node boundary also has a versioned lower-level RPC protocol.
`RemoteStorageNodeClient` implements the same `StorageNode` interface over the
existing framed TCP transport, while `StorageNodeServer` delegates to a local
node and `ObjectStore`. PREPARE, COMMIT, and ABORT use bounded operation IDs,
reject conflicting reuse, and retain terminal state on disk. Connections are
persistent and synchronous; no automatic retry occurs.

Ambiguous operations can be inspected through `RecoveryCoordinator`. It queries
every named participant, validates operation IDs and fingerprints, and returns a
deterministically ordered distributed-state report. Inspection is read-only.
Completion or rollback occurs only through an explicit operator call; committed
participants are never converted to aborted, and unavailable participants remain
unknown rather than being guessed.

Coordinator operation identity and recovery context are durable under
`coordinator/operations/`. Each strict, checksummed record retains the original
membership version, placement mapping, participant set, lifecycle, and latest
observations. Restart loads these records but performs no automatic query,
completion, rollback, retry, or repair; a fresh explicit inspection supersedes
the persisted observations with authoritative storage-node journal state.
`DurableReplicaManager` wraps the existing replication algorithm so STARTED and
PREPARING are durable before participant work and the resulting terminal or
UNRESOLVED state is durable before the result is returned.

Membership is also persisted under `coordinator/membership/registry` with a
monotonic version, validated endpoints, administrative state, and last-known
health observations. Synchronous heartbeat clients verify the responding node's
identity and use a configurable consecutive-failure threshold. Restart marks
persisted health as stale until a fresh check; no heartbeat triggers repair,
operation recovery, data movement, or retry.

Replica repair is operator-directed at the planning layer. `RepairPlanner`
compares the authoritative replica catalog with current, freshly verified
membership and emits deterministic source/target plans or explicit
no-source/no-target outcomes. Plans are evidence-bound to membership, health,
generation, chunk, and the exact global catalog version and replica state, and
are durably stored under `coordinator/repairs/`. Planning never copies or deletes
data and never rewrites historical operation records.

Approved plans may be executed explicitly through `RepairExecutor`. Execution is
locally serialized, revalidates membership, health, generation, chunk, and exact
catalog evidence immediately before transfer, and journals progress in ARPL v3.
Local targets stage, sync, reread, and verify repair bytes before atomic
publication. Completion requires a durable catalog compare-and-swap against the
plan's evidence; a conflict makes the plan stale and unknown post-rename
durability is a typed failure. Historical operation records remain immutable.
There is no automatic approval, execution, retry, replan, source/target
reselection, or background repair.

An authoritative current-replica catalog abstraction is now durable in the
separate `coordinator/replicas/catalog` snapshot. Its strict, versioned `ARPC` format
records generation-aware chunk identity and metadata together with sorted replica
state and integrity evidence. Every real mutation advances a monotonic catalog
version and is published by synchronized temporary-file write, atomic rename,
and parent-directory synchronization. Corrupt published state stops startup
without deletion. If the rename succeeds but directory synchronization cannot be
confirmed, the catalog reports unknown durability and rejects later mutations in
that process. This current-state catalog is distinct from immutable `ACOP`
operation history and `ARPL` repair journals. Compare-and-swap validates the
exact global catalog version and full expected replica state before atomically
publishing one chunk's desired state. Successful mutations advance the global
version and only the changed record's version. One active coordinator/catalog
owner and explicit catalog population are required; cross-process writers are
deliberately deferred.

## Build and test

Prerequisites: CMake 3.20+, a C++20 compiler, Git, and network access on the first
configure (CMake fetches the pinned GoogleTest release).

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

The project enables `-Wall -Wextra -Wpedantic` on GCC and Clang. See
[`docs/architecture.md`](docs/architecture.md) and
[`docs/protocol.md`](docs/protocol.md) for the current design and its boundaries.

## Repository layout

```text
include/atlasfs/  Public C++ interfaces
src/              Implementations
tests/            Unit and loopback socket integration tests
docs/             Engineering decisions and protocol specification
```

## Current scope

Object identifiers contain only ASCII letters, digits, `.`, `_`, and `-`, are at
most 255 bytes, and cannot be `.` or `..`. Objects are limited to slightly below
16 MiB so complete requests and responses remain inside the framing limit. The
internal `.atlasfs-tmp-` prefix is reserved. Replication factors are bounded to
1–16 and default to one. If fewer eligible nodes exist than requested, the plan
is explicitly under-replicated and strict replicated PUT refuses to expose a
partial generation. Chunking remains invisible to the wire protocol, which still
transfers one bounded logical object per PUT or GET.

Node membership, plans, and replication orchestration are in-process and
single-threaded. Storage roots, chunks, manifests, and current replica metadata
are durable and independent. Multiple catalog writers, automatic repair,
consensus, and production concurrency remain deliberately out of scope.
