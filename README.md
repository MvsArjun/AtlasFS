# AtlasFS

AtlasFS is a C++20/Linux object-storage systems project with one coordinator,
chunked persistent storage, deterministic replication, durable recovery and repair,
an authoritative replica catalog, epoch fencing, and bounded POSIX TCP/OpenSSL
mutual-TLS transport. It provides reusable libraries and a benchmark executable,
not a packaged production daemon. Network listeners currently use loopback.
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
catalog evidence immediately before transfer, and journals progress in ARPL v3
(v4 for epoch-bound execution).
Local and remote targets use the same repair-publisher boundary. Remote requests
bind plan, execution, target-node, object, generation, manifest, chunk, and
integrity evidence without accepting filesystem paths. Targets stage, sync,
reread, and verify repair bytes before atomic publication. Completion requires a
confirmed physical publication followed by a durable catalog compare-and-swap
against the plan's evidence. A lost remote response remains explicitly unknown;
a CAS conflict leaves a physical but non-authoritative replica for operator
inspection. Historical operation identities and participants remain immutable;
explicit recovery may refresh observations and lifecycle knowledge. There is no automatic
approval, execution, retry, replan, source/target reselection, or background
repair.

Interrupted repair execution is reconciled only through an explicit
`RepairExecutor::reconcile` call. Reconciliation accepts a durable `EXECUTING`
plan or a narrowly defined ambiguous failure, revalidates the original
membership, health, placement, and catalog evidence, and reuses the original
execution ID. A lost remote response is resolved by replaying that exact request
against the target's durable idempotency journal. If the catalog contains the
one exact transition the plan would have committed, reconciliation finishes the
durable plan without copying again; any other catalog or membership change marks
the plan stale. Startup itself still performs no repair action.

Reconciliation replays every interrupted pre-CAS execution through the publisher,
even when bytes already exist, so repeated coordinator crashes cannot erase a
target journal's unknown outcome. The post-CAS completion path requires durable
publication and verification evidence and freshly verifies the target's manifest,
chunk length, and checksum. Missing or corrupt data leaves completion pending;
it does not undo the existing catalog transition. A catalog process with unknown
durability rejects reconciliation until restart establishes the surviving state.

An authoritative current-replica catalog abstraction is now durable in the
separate `coordinator/replicas/catalog` snapshot. Its strict, versioned `ARPC` format
records generation-aware chunk identity and metadata together with sorted replica
state and integrity evidence. Every real mutation advances a monotonic catalog
version and is published by synchronized temporary-file write, atomic rename,
and parent-directory synchronization. Corrupt published state stops startup
without deletion. If the rename succeeds but directory synchronization cannot be
confirmed, the catalog reports unknown durability and rejects later mutations in
that process. This current-state catalog is distinct from identity-preserving `ACOP`
operation history and `ARPL` repair journals. Compare-and-swap validates the
exact global catalog version and full expected replica state before atomically
publishing one chunk's desired state. Successful mutations advance the global
version and only the changed record's version. One active coordinator/catalog
owner and explicit catalog population are required; cross-process writers are
deliberately deferred.

## Release candidate scope

Milestone 23 audits the final planned single-coordinator architecture. The
[release-readiness report](docs/release-readiness.md) records actual verification,
publication provenance, compatibility and operator procedures. The proposed
candidate is **0.1.0-rc.1 (library/source)**; no formal release or tag is published.
Passing hosted CI does not establish production capacity or physical power-loss
durability. Exclusive durable roots, credential permissions, optional CRLs,
external epoch checkpoints and quiesced upgrades remain operator responsibilities.

Unconfirmed coordinator-store, replica and node-journal publication stays unknown:
coordinator stores reject
further mutations in that instance, and node commit/abort intent is persisted
before physical action. New node journals have versioned whole-record CRC-32C;
legacy journals remain readable but are not equivalently protected. Do not
downgrade newly written roots to older binaries. See the report before deployment.

## Build and test

Prerequisites: CMake 3.20+, a C++20 compiler, Git, and network access on the first
configure (CMake fetches the pinned GoogleTest release). The filesystem and
networking implementation requires a POSIX environment and OpenSSL 3.0+ development
headers/libraries. On Ubuntu: `sudo apt-get install g++ cmake ninja-build git libssl-dev openssl`.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

On Windows, the MSYS2 MSYS runtime provides the required POSIX interfaces. From
an MSYS2 MSYS shell, install and use the reproducible toolchain as follows (the
MinGW shells target the native Windows API and are not suitable here):

```sh
pacman -Syu
pacman -S --needed gcc cmake ninja git openssl-devel
cmake -S . -B build-msys -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build-msys --parallel
ctest --test-dir build-msys --output-on-failure
```

The project enables `-Wall -Wextra -Wpedantic` on GCC and Clang. See
[`docs/architecture.md`](docs/architecture.md) and
[`docs/protocol.md`](docs/protocol.md) for the current design and its boundaries.

## Benchmarks and stress verification

Benchmarks are opt-in standalone executables, not timing gates in CTest. They use
real durable local/replicated operations and ephemeral mutual-TLS identities on
loopback; they do not need production credentials. Build and run a Release sample:

```sh
cmake -S . -B build-bench -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build-bench --parallel 4
./build-bench/atlasfs_bench --iterations 40 --objects 40 --bytes 65536 \
  --chunk 16384 --nodes 4 --replicas 2 --workers 1 --seed 42 --output results.json
python3 benchmarks/compare.py before.json after.json
```

Use `--suite local|distributed|fault|all` and `--help` for bounded parameters.
JSON includes throughput, p50/p95/p99 latency, CPU time, reported peak RSS, disk
file bytes, known rejections/unknown outcomes, and recovery timing. Worker requests
queue through **one serialized coordinator**; independent connections may process
transport I/O concurrently, but durable handlers retain one serialized admission
boundary. Long runs are separate from short deterministic
`Stress*` regressions in CTest. See [Milestone 20](docs/milestone-20.md) for measured
baselines, limitations, fault coverage, and reproduction instructions.

## Repository layout

Milestone 22 adds `scheduling::AdmissionQueue`: one active caller lease and a
bounded FIFO of waiting callers, with explicit overload, cancellation, deadline,
and shutdown outcomes. Existing coordinator/client APIs remain synchronous and
are not independently thread-safe; callers must hold the same admission lease
around shared orchestration, epoch guards, catalog operations, and clients.
`TcpServer` defaults to four fixed connection workers with isolated TLS/parser
state, four pending handler admissions, and a million-request connection limit.
Authorization, epoch validation, publication, and journals remain inside one
serialized handler; response I/O does not hold that lease. Overload closes the
connection and never triggers automatic retry. Stop cancels pending work and
unblocks sockets, but cannot undo an executing durable operation.
See [Milestone 22](docs/milestone-22.md) for bounds, uncertainty, regressions,
and same-runner FIFO/mutex and independent-connection comparisons. Benchmark
`--admission mutex|fifo` and `--dispatch serial|bounded` expose behavioral
references for separate experiments; `--profile admission` and `--profile
dispatch` run three alternating paired rounds on Linux.

Milestone 21 adds opt-in transport/filesystem instrumentation and warm-up samples:
use `--warmup 3 --metrics on` (or `--metrics off` to measure collection overhead).
`--suite startup` characterizes retained object/chunk/journal/catalog reopening.
On Linux, run `python3 benchmarks/linux_run.py --executable build-bench/atlasfs_bench
--output-dir build-bench/results --runner-class manual-uncontrolled --profile
characterize --rounds 3`, then `python3 benchmarks/summarize.py build-bench/results`.
The manual Linux workflow supports explicitly labeled hosted observations or an
operator-controlled self-hosted runner; neither ordinary CI nor loopback traffic
is a production capacity claim. See [Milestone 21](docs/milestone-21.md) for counter
layers, temporary-space accounting, limitations and reproducible commands.
Secure TCP defaults to TCP_NODELAY after a same-runner three-round Linux A/B
experiment; `TlsConfig::tcp_nodelay=false` or benchmark `--tcp-nodelay off` opts
out. Replicated-read throughput improved 46.99x in that loopback workload;
this is not a real-network or production guarantee. Compact results and
provenance are in [benchmarks/results/m21](benchmarks/results/m21/README.md).

The GitHub Actions workflow uses Ubuntu 24.04 and GCC 13 with separate normal,
ASan/UBSan, and TSan jobs, serial CTest, and uploaded diagnostics. A workflow file
is not evidence that CI or sanitizers passed; inspect the actual run result.

```text
include/atlasfs/  Public C++ interfaces
src/              Implementations
tests/            Unit and loopback socket integration tests
docs/             Engineering decisions and protocol specification
```

## Current scope

Milestone 19 adds durable coordinator-epoch fencing under explicit,
operator-controlled activation. TLS-authorized nodes persist an exact successor
token; every mutating storage RPC checks that token through publication, and
catalog mutations and repair execution hold the matching local authority guard.
Pending/partial activation blocks coordinator work until every configured node
confirms the exact token. Existing TLS sessions do not retain stale mutation
authority. Same-epoch repair recovery preserves execution IDs and fingerprints;
new epochs cannot adopt old executions. This is **not consensus, automatic
failover, or safe multi-coordinator operation**. Provisioning and replacement
require quiescing existing writers, the complete participant set, persistent
enrollment state, and independent high-water checkpoints. See
[Milestone 19](docs/milestone-19.md) for APIs, migration, and failure semantics.

Milestone 18 adds explicit mutual-TLS constructors for `TcpClient`, `TcpServer`,
`RemoteStorageNodeClient`, and `StorageNodeServer`. TLS 1.3 authenticates certificate
chains and exact SAN URI role/node identities; a separate allowlist authorizes
storage operations for specific targets before dispatch. Secure endpoints never
fall back to plaintext. Existing constructors remain **insecure development-only**
compatibility paths: deployments must choose the TLS overload on both sides and
close/remove plaintext listeners. See [Milestone 18](docs/milestone-18.md) for
configuration, issuance, CRLs, credential rotation, failure behavior, and limits.

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
consensus, automatic credential management, and parallel durable handler execution
remain deliberately out of scope. Bounded connection I/O concurrency is supported;
one storage root still requires one owning server and serialized direct access.
