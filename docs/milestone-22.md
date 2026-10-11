# Milestone 22: fair admission and bounded server dispatch

## Status and scope

This document describes the implementation candidate and the reproduced
Milestone 21 baseline. Final performance measurements, verification evidence,
publication commit, and remote/CI confirmation are pending and must be recorded
before marking this milestone complete.

AtlasFS retains one authoritative coordinator, the existing epoch guard, and
the existing catalog CAS boundary. This change does not introduce consensus,
additional coordinators, independent catalog writers, automatic replanning, or
automatic mutation retries.

## Reproduced baseline

The published starting revision is
`323872f75f5650d56e89bf6f919573cccc4a113d`. Its unchanged fresh-checkout MSYS2
suite passed 325/325 tests in 39.47 seconds; benchmark smoke validated 24
workloads and 72 measured cycles.

[Linux baseline run 38113891187](https://github.com/MvsArjun/AtlasFS/actions/runs/38113891187)
reproduced the 39-report, three-round characterization at that revision. It used
shared GitHub-hosted Ubuntu 24.04.5, kernel `6.17.0-1022-azure`, an AMD EPYC 7763
exposing two CPUs with affinity `{0,1}`, 8,323,964,928 bytes of memory, GCC 13.3,
CMake 3.31.6, Ninja 1.13.2, OpenSSL 3.0.13, and Release `-O3 -DNDEBUG`.
Its machine fingerprint is
`b78ff2ba8010ed1c02bbea6f7573983212914cd35ffd7f4d5a1b4ace9e275a56`.

The replicated-read workload used seed 42, four storage nodes, R2, 16 retained
64 KiB objects, 16 KiB chunks, 80 measured cycles, four separate warmups,
instrumentation enabled, and TCP_NODELAY enabled. Values below are medians of
three per-round measurements, not pooled percentiles.

| Workers | Read operations/s | End-to-end p99 ms | Admission queue p99 ms |
| --- | ---: | ---: | ---: |
| 1 | 456.455852 | 2.404261 | 0.000201 |
| 2 | 446.6152003 | 178.133610 | 175.822203 |
| 4 | 446.6082864 | 178.917388 | 176.719063 |

Extra workers did not improve throughput. A waiting worker could remain blocked
until almost the entire 80-operation read batch completed. The queue tail is
much larger than one read's service time.

The filesystem, loopback network, warm-cache policy, and hosted-runner caveats
from [Milestone 21](milestone-21.md) continue to apply. These observations do not
establish production capacity or power-loss behavior on a controlled device.

## Two distinct bottlenecks

The original `coordinator_queue` mutex is in the benchmark's distributed
orchestration, not a production coordinator RPC endpoint. Holding it is required:
the shared `RemoteStorageNodeClient` instances contain mutable request IDs, TLS
streams, parsers, and pending responses. The shared manager, placement state,
and mutable plans also cannot be executed concurrently without a separate
ownership design. `std::mutex` does not provide an application FIFO contract;
the same worker can release and reacquire it ahead of existing waiters.

Separately, the original `TcpServer::serve` owns a complete connection
synchronously, including handshake, reads, request handling, and response I/O.
An idle authenticated connection can prevent any second TLS handshake. This is
connection head-of-line blocking, not catalog contention. Independent dispatch
measurements isolate that boundary instead of attributing both effects to one
mutex.

## Caller-thread FIFO admission

`include/atlasfs/admission.hpp` and `src/admission.cpp` provide
`scheduling::AdmissionQueue`. It is an admission primitive, not a task executor:
the admitted caller executes its operation on its own thread while owning a
move-only RAII lease. Exactly one lease can be active.

Capacity counts waiting callers, excluding that active lease. Capacity zero
permits one immediate operation but no waiters; the implementation rejects
capacity above 65,536. Waiters are linked stack-owned nodes rather than an
unbounded heap-backed task queue. New callers cannot bypass an existing waiter.

FIFO means order of successful enqueue linearization under the admission
mutex, not wall-clock invocation order. Diagnostic tickets identify that order;
they are neither durable execution IDs nor proof of completion order. Tickets
and cumulative counters saturate. Cancelled, expired, and shutdown-rejected
waiters are removed without blocking later live waiters.

| Result | Meaning before granting a lease |
| --- | --- |
| `admitted` | The caller owns the only active lease and may execute. |
| `overloaded` | Waiting capacity is exhausted; the operation did not execute. |
| `cancelled` | The caller's stop token was requested; the operation did not execute. |
| `deadline_expired` | The admission deadline elapsed; the operation did not execute. |
| `shutdown` | Admission is closing; new/pending operations do not execute. |

Shutdown takes precedence over cancellation, which takes precedence over an
expired deadline when these are observed together. `shutdown()` rejects new
and pending admission without revoking the active lease. `wait_idle(deadline)`
allows an owner to wait for active execution and pending cleanup. Lease
destruction releases admission on normal return and exceptions. Recursive
acquisition is unsupported. Owners must still join callers before destroying
the queue object; lease state ownership does not authorize calls through a
destroyed queue.

After grant, cancellation or deadline expiry does not revoke the lease,
interrupt durable publication, or undo remote I/O. The operation's existing RPC
and durability semantics still apply. Admission occurs before authority guards,
plan references, and shared-client use, preventing waiters from holding authority
while waiting for their turn.

The benchmark uses this API for serialized reads, mixed orchestration, and
repair execution/recovery. It keeps the original mutex path as a selectable
behavioral reference in the same instrumented binary, not a byte-identical
Milestone 21 executable. This is not a new production coordinator service or a claim that
all externally shared coordinator objects became internally thread-safe.

## Bounded, independent connection dispatch

`net::DispatchConfig` is appended to TCP and storage-server constructors.
The bounded default uses four pre-created workers, four waiting request slots,
and at most 1,000,000 handled requests per connection. Worker count must be
1..16, request waiting capacity 0..65,536, and connection request limit
1..1,000,000. These are per-server limits, not a process-wide connection quota.

There is no userspace queue of accepted connections. Each worker owns one
accepted socket, TLS stream, parser, peer policy, deadline, and response state.
When all worker slots are occupied, the acceptor promptly closes another
accepted socket before handshake or request execution. The kernel listen
backlog remains a separate OS-managed resource. Existing payload-size limits
remain enforced per frame.

Workers may perform TLS/socket processing concurrently, but every complete
request enters one FIFO admission gate before invoking the application handler.
This is a SINGLE serialized handler boundary for each server. It covers storage
records, repair records, fault flags, local storage operations, application
authorization, and epoch checks. Slow response I/O releases the handler lease
and does not own catalog or epoch authority. Catalog writers still require the
existing authoritative guard and versioned CAS.

Peer role, target permission, activation permission, and coordinator epoch are
checked by the application handler on each request. Authenticating a connection
does not authorize subsequent targets or stale epochs. A connection-local
parser and stream prevent shared mutable frame/TLS state.

TLS handshake, partial-frame reads, queue admission, and response writes retain
bounded transport deadlines. Partial bytes do not extend the request deadline;
a successful response starts the next idle/request window. The handler itself
is not forcibly preempted: filesystem operations and durability boundaries can
outlast a transport deadline. Arbitrary user-supplied handlers that never return
cannot be made bounded by forcibly killing a worker safely.

`DispatchSnapshot` reports accepted/rejected connections, active and peak active
connections, requests, request-limit closes, admission rejection/expiry,
disconnect cancellation, and current/peak request waiting. These are diagnostic
counters, not authenticated protocol acknowledgments.

### Disconnects, overload, and shutdown

When a queued request reaches its turn, the server checks stopping state and
raw socket disconnect evidence before invoking the handler. It does not consume
encrypted data outside OpenSSL. Buffered TLS records or `close_notify` are not
definitive cancellation evidence, so the request may instead reach its deadline
or execute before disconnect is observable.

Raw receive EOF is treated as cancellation before execution. Consequently TCP
write-half-close request/response operation is unsupported: EOF does not prove
that the peer also closed its reading side. Clients should maintain the
full-duplex authenticated session while awaiting the response.

Overloaded connections, request-admission failures, and expired queued requests
are closed rather than receiving a new synthetic success/overload wire message.
The server knows a pre-handler rejection did not execute, but a remote client
cannot always distinguish it from response loss after execution. Once a mutation
may have been sent, existing unknown-outcome classifications remain necessary.

`stop()` closes admission, shuts down the listener and owned sockets, and wakes
workers. Pending handlers do not run. Active handlers may finish durable work;
their response can be lost. Socket ownership is retired under the same lifecycle
lock used by shutdown before descriptor close/reuse. Workers are joined before
`serve()` returns. Unexpected handler/worker exceptions fail the endpoint closed,
preserve the first exception, clean up workers, and rethrow from `serve()`.
Authentication/transport failures remain connection-local.

There is no implicit retry. Operator-directed recovery/replay preserves the
same durable execution ID, request fingerprint, target, and epoch evidence.
Physical publication still precedes verification; verification precedes
authoritative catalog CAS; completion follows the required durable transition.
Historical operation records remain evidence rather than current replica
authority. Timeout, overload, disconnect, or cancellation never promotes failed
physical bytes into authoritative state.

## Repeated measurement method

`atlasfs_bench` accepts `--admission mutex|fifo`, `--admission-capacity`,
`--admission-timeout-ms`, `--dispatch serial|bounded`, `--dispatch-workers`,
`--dispatch-capacity`, and `--connection-requests`. The benchmark admission
default is FIFO with 16 waiting slots and a 5,000 ms admission budget.
Non-local bounded benchmark suites require at least two dispatch workers;
the server library itself still supports one worker. Use serial mode for the
one-slot behavioral reference.

The Linux `admission` profile uses one binary and one runner for three rounds.
Each round pairs mutex/FIFO at one, two, and four workers using the exact
80-cycle baseline configuration above. A fourth pair runs 400 cycles with four
workers to observe sustained mixed reads/writes; a fifth pair exercises repair,
recovery, response-loss, and epoch scenarios. The server stays in serial
reference mode on BOTH sides, isolating coordinator-side admission fairness.
Pair order reverses in round two. No mutation retry is added.

The separate Linux `dispatch` profile pairs serial/bounded serving on the same
runner for three rounds. Each fixture completes a first authenticated handshake
and holds that idle connection, or writes seven bytes of an incomplete frame.
It then times a second independent TLS connection and authorized query echo.
The fixed server deadline is 200 ms and client deadline 1,000 ms; these probes
do not mutate storage or claim parallel catalog throughput. Each case measures
12 cycles with two warmups. Both full-cycle and independent-connection
p50/p95/p99 are reported. Server snapshots explicitly include warmups.

The dispatch profile also holds coordinator admission at FIFO and pairs
serial/bounded servers for the same distributed workloads at one, two, and four
workers, the 400-cycle sustained four-worker workload, and the repair/recovery
fault suite. Both variants retain four configured dispatch workers, four request
waiting slots, 16 coordinator waiting slots, and the same deadlines; serial
reference mode intentionally uses only one connection at a time. These pairs
audit handler-gate overhead and throughput of the bounded default independently
of fairness gains. The complete dispatch profile contains 36 reports: six
serial/bounded pairs over three rounds, with every pair reversed in round two.

The deterministic capacity-zero probe holds the only active FIFO lease and
offers requests with no waiting capacity. Every offered request must be rejected
before execution, with zero RPCs and zero successful operations. Its offered
rejection rate is NOT storage capacity or successful-work throughput.

### JSON semantics and comparison safeguards

Benchmark JSON remains schema v2 with additive `scheduling_schema: 1` metadata.
Existing Milestone 21 reports remain valid. New rows distinguish attempts,
executed operations, successful operations, admitted operations, overload,
cancellation, admission deadline expiry, and shutdown rejection. Admission
outcomes partition attempts and never count a pre-execution rejection as
executed work. Expected fault rejections, uncertain outcomes, and transport
timeouts remain explicit.

The older `successes` field retains its original meaning: measured cycles with
no unexpected exception, INCLUDING correctly exercised expected rejections.
`successful_operations` and `successful_operations_per_second` exclude rejected,
unknown, timed-out, and non-executed work. `operations_per_second` is attempted
cycle throughput. Queue percentiles cover admission attempts, including rejected
ones; ordinary fairness comparisons have adequate capacity to avoid overload.
Legacy mutex queue capacity/peak are null because its waiting queue was neither
bounded nor measured. FIFO queue limits and high-water values are numeric.

`compare.py` requires explicit `--allow-admission-change` or
`--allow-dispatch-change` for the corresponding A/B variant. These flags do not
permit changed resource limits, deadlines, seeds, workload sizes, worker counts,
warmups, build flags, or instrumentation. It also checks runner class and exact
machine fingerprint. Source/build metadata remains attached to each raw report.
`summarize.py` rejects aggregation across different fingerprints and reports
medians of per-round percentiles, not pooled percentiles.

Correctness tests use controlled admission, promises, barriers, and injected
failures. Timing-sensitive performance observations are not CTest speed gates.
A clean sanitizer result is not proof of fairness or deadlock freedom.

Example Linux reproduction:

```sh
cmake -S . -B build-benchmark -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DATLASFS_BUILD_BENCHMARKS=ON -DCMAKE_CXX_COMPILER=g++-13
cmake --build build-benchmark --parallel 4
CXX=g++-13 python3 benchmarks/linux_run.py --executable build-benchmark/atlasfs_bench \
  --output-dir build-benchmark/admission --runner-class manual-uncontrolled \
  --profile admission --rounds 3
python3 benchmarks/summarize.py build-benchmark/admission
CXX=g++-13 python3 benchmarks/linux_run.py --executable build-benchmark/atlasfs_bench \
  --output-dir build-benchmark/dispatch --runner-class manual-uncontrolled \
  --profile dispatch --rounds 3
python3 benchmarks/summarize.py build-benchmark/dispatch
```

Use `controlled` only on an operator-controlled exclusive Linux runner. Hosted
workflow results must remain labeled `shared`. No ratio between different CPU
or VM fingerprints is valid optimization evidence.

## Final evidence pending

Local integrated verification of the candidate passed 363/363 CTest cases in
44.48 s on MSYS2 GCC 15.3. The 38 added cases comprise 16 admission tests,
15 dispatch tests and seven fenced/durable scheduling integration tests. The
production-only Release build with benchmarks enabled passed. Cppcheck 2.21
passed all 25 source/benchmark translation units. Instrumented and disabled
smoke each validated 27 workloads/81 measured cycles, with zero unexpected
errors; all 27 instrumented accounting snapshots were complete. The 13 Python
schema/comparison regressions passed. These local results are not Linux
sanitizer results.
The combined TLS/epoch/repair/recovery/scheduling repeat selected 77 tests;
all ten repetitions passed (770 executions, 88.37 s).

Before completion, record the final implementation commit, normal/ASan/UBSan/
TSan counts and timings, Cppcheck, production builds, repeated integration tests,
fresh-clone verification, smoke outcomes, paired admission/dispatch measurements,
raw-report provenance, normal push/remote SHA confirmation, and completed CI.
Interpret throughput regressions and queue/CPU/RSS tradeoffs explicitly rather
than assuming more workers improve throughput.

Remaining design limits include single-handler execution, shared-client
serialization, no handler preemption or rollback, bounded but finite connection
slots vulnerable to idle-peer occupancy until deadlines, coarse tail estimates
in small batches, loopback-only network characterization, unavailable controlled
hardware/physical network measurements, and underlying hosted-filesystem
durability limitations.
