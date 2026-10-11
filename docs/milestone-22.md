# Milestone 22: fair admission and bounded server dispatch

## Status and scope

The implementation is published and verified. The starting Milestone 21 revision
is `323872f75f5650d56e89bf6f919573cccc4a113d`; scheduling/dispatch implementation
is `020c8fab50ad9e3f1e266e2b2f8c3578efca38fd`, final runtime hardening is
`115415fe1d0c447c2dcdabce98dbb17e284265fa`, and corrected handoff-bound testing is
`3eb679b2cd7b980e86512c91a57c18be337832cc`. Subsequent delivery changes retain
results/documentation only. No published history was rewritten.

FIFO fairness, bounded overload and independent connection progress are
demonstrated, and correctness/security/recovery checks pass. The requested
universal throughput non-regression criterion is not established: small read/RPC
costs and variable repair/mixed stalls are reported explicitly below.

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
Each waiter owns a condition variable: lease release wakes only the FIFO head,
cancellation wakes only its caller, and shutdown wakes every pending waiter.
This avoids waking the entire bounded queue on every ordinary handoff.

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
Returned response payloads must also fit the configured transport limit before
encoding. An oversized trusted-handler result fails the endpoint closed; the
callback's internal allocation itself cannot be safely preempted by transport.
Unexpected worker-control and admission synchronization exceptions propagate
through endpoint failure and cleanup rather than masquerading as socket errors.

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
Additive `queue_max_ms` and `latency_max_ms` also report the worst measured wait
and cycle. In a long batch, only three starving workers among hundreds of
operations can be invisible to p99; percentiles alone are not a fairness proof.
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

## Verification history and investigated failures

The first hardened-code CI attempt
[38162648935](https://github.com/MvsArjun/AtlasFS/actions/runs/38162648935)
exposed an incorrect regression-test assumption: queue high-water can be four,
not three, during a handoff with no active handler. Normal repeated testing and
the TSan suite failed that assertion; their diagnostics did not report a data
race. The corrected bound is waiting ≤ min(configured capacity, worker count),
and active handler plus waiting callers ≤ worker count. The independent assertion
that handler concurrency is one remains unchanged. This is a test/documentation
correction, not relaxation of queue capacity or authority serialization. Final
CI passed the corrected test in the full normal and sanitizer suites and repeated
normal integrations, as recorded below.

The initial implementation `020c8fab50ad9e3f1e266e2b2f8c3578efca38fd`
passed [Linux verification](https://github.com/MvsArjun/AtlasFS/actions/runs/38114978604):
363/363 normal (7.11 s), ASan/UBSan (29.18 s), and TSan (19.26 s), both
27-workload/81-cycle smoke modes and 13 tooling tests in each job. Its normal
production/static checks and 70 TLS, 310 epoch/recovery and 400 scheduling
repeat executions passed. An independent remote fresh clone passed 363/363
(43.12 s), validated smoke, and reported a clean source checkout.

Initial three-round [admission measurements](https://github.com/MvsArjun/AtlasFS/actions/runs/38115003308)
on fingerprint `45ffedee19174ba645d32cde353b2908f17586b6c36ca9254b384ee57db53fa0`
(EPYC 9V74, two CPUs, 8,323,969,024 memory bytes) reduced four-worker read
queue p99 125.924→5.195 ms and end-to-end p99 127.581→6.913 ms; read
throughput 625.998→611.111/s (-2.38%). The 400-cycle mixed median was
160.476→137.531/s (-14.30%), so this is not evidence of universal non-regression.
Counts were identical: 2,400 file syncs, 2,400 directory syncs and 13,256,680
written bytes per mixed round. In the two slow FIFO rounds, sync wait grew
approximately 0.90→1.31 s and 0.94→1.24 s, accounting for most of the extra
0.42/0.47 s wall time. Hosted filesystem variation is a plausible contributor,
not a proven dismissal of the observed regression. The follow-up measurements
after targeted FIFO wakeups and final endpoint hardening are recorded below.

Initial three-round [dispatch measurements](https://github.com/MvsArjun/AtlasFS/actions/runs/38115005101)
on fingerprint `b78ff2ba8010ed1c02bbea6f7573983212914cd35ffd7f4d5a1b4ace9e275a56`
reduced independent-connection p99 202.707→3.487 ms (idle) and
202.774→3.449 ms (partial frame). Keeping FIFO constant, four-worker read
throughput was 430.559→428.584/s (-0.46%); sustained mixed was
130.716→131.467/s (+0.57%). These ratios are internal paired comparisons,
not ratios between different admission/dispatch runners.

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

The hardened local suite passed 365/365 (43.79 s), with 40 additions to M21:
16 admission, 17 dispatch and seven fenced/durable scheduling tests. All 420
targeted repeated executions passed, along with 40 extra response-limit repeats
and 50 corrected isolated-state/handler-serialization repeats. The earlier
770 combined TLS/epoch/repair/recovery/scheduling executions also passed.
Cppcheck 2.21 passed 25 source/benchmark translation units; production-only
Release with benchmarks enabled built successfully. Both final smoke modes
validated 27 workloads/81 cycles, and all 14 Python tooling tests passed.
An independent remote fresh clone of the hardened runtime passed 365/365
(43.66 s), valid smoke and all 14 tooling tests with a clean source checkout.
Fast-forwarding that independent checkout to the corrected handoff-bound test
revision also passed 365/365 (42.88 s).

[Corrected Linux CI 38162957314](https://github.com/MvsArjun/AtlasFS/actions/runs/38162957314)
at `3eb679b2cd7b980e86512c91a57c18be337832cc` completed successfully:

| Check | Actual result |
| --- | --- |
| Normal Debug | 365/365, 7.08 s |
| ASan/UBSan, leak detection enabled | 365/365, 43.43 s |
| TSan, halt on error | 365/365, 22.37 s |
| Instrumented and disabled smoke, each matrix job | 27 workloads/81 cycles each |
| Python tooling, each matrix job | 14/14 |
| TLS repair repeats | 7 × 10 = 70 executions |
| Epoch/repair/recovery repeats | 31 × 10 = 310 executions |
| Admission/dispatch/integration repeats | 42 × 10 = 420 executions |
| Normal Cppcheck and production-only Release + benchmarks | Passed |

The failed earlier CI attempt remains recorded above. It was investigated and
corrected; it is not counted as a passing sanitizer run. A clean sanitizer run
does not prove fairness, real-time bounds or deadlock freedom.

## Final paired Linux observations

Full paired JSON, comparison CSVs, summary tables and artifact provenance are
retained in [benchmarks/results/m22](../benchmarks/results/m22/README.md).
Each paired experiment used one clean Release binary, identical build flags,
seed, work volume, limits, warmups and instrumentation, and three alternating
rounds. All values below are medians of per-round values except explicitly
labeled maximum waits (the maximum across all measured rounds).

The final source-tree checksum is
`5677661cfb33a60b467a5316a5c28e26eb7a0a0509877465f5716725a8e77ebf`.
Admission/dispatch reports identify runtime commit `115415f`; the default
characterization identifies `3eb679b`, whose change was tests/documentation only.
Source/build metadata was preserved rather than relabeling earlier measurements.

### FIFO versus mutex, serial server held constant

[Admission run 38162678128](https://github.com/MvsArjun/AtlasFS/actions/runs/38162678128)
completed 30 reports/24,384 measured cycles, including 3,840 deliberately rejected
capacity-zero attempts. Unexpected errors and timeouts were zero. Its shared
runner fingerprint is
`ec6696a00ab35046df0c1cae4f42e5f3f55c614c45938afd67a24d7eefaa1c7d`:
EPYC 7763, two CPUs, 8,323,969,024 bytes memory; Ubuntu 24.04.5,
kernel 6.17.0-1022-azure, GCC 13.3, CMake 3.31.6, Ninja 1.13.2,
OpenSSL 3.0.13, Release `-O3 -DNDEBUG`, warm-cache loopback TLS.

| Read workers | Mutex → FIFO ops/s | Queue p99 ms | End-to-end p99 ms |
| --- | ---: | ---: | ---: |
| 1 | 447.631 → 447.957 | 0.000381 → 0.000862 | 2.485 → 2.437 |
| 2 | 437.023 → 434.588 | 178.803 → 2.583 | 181.185 → 4.872 |
| 4 | 437.542 → 431.378 | 180.394 → 10.236 | 182.643 → 13.198 |

Read-throughput changes were +0.07%, -0.56%, and -1.41%; this is fairness
improvement with small measured handoff overhead, not a throughput scaling
claim or proof of zero regression. Four-worker typical end-to-end p50/p95 rose
2.258/2.379 → 9.095/10.576 ms as waiting callers stopped being bypassed.
Queue p50/p95 rose 0.000221/0.002525 → 6.834/8.321 ms while the extreme tail
shrank. All other p50/p95 values remain in the retained summary CSV.

For the 400-cycle four-worker sustained read batch, ops/s were
443.428→438.089 (-1.20%), but maximum queue wait fell
903.938→7.716 ms. Its mutex p99 was only 0.002916 ms: three starved workers
among 400 operations can be invisible to p99. For sustained mixed work,
ops/s were 133.466→136.094 (+1.97%), end-to-end p99
38.475→36.827 ms, and maximum queue wait 3,049.131→41.303 ms.
This does not retroactively erase the initial mixed-throughput regression on
a different fingerprint, nor prove the wakeup change alone caused the difference.

Read CPU time for 80 cycles was 0.194577→0.195305 s (one worker),
0.200657→0.200729 s (two), and 0.198529→0.200742 s (four).
Process CPU utilization stayed about 109% (approximately 1.09 of two exposed
cores), and median process peak RSS was 19,423,232 bytes for both policies.
RSS is a whole-process high-water observation, not a per-operation allocation
bound or committed/physical filesystem footprint.

### Serial versus bounded dispatch, FIFO held constant

[Dispatch run 38162680828](https://github.com/MvsArjun/AtlasFS/actions/runs/38162680828)
completed 36 reports/24,528 cycles, including the same 3,840 deliberate overload
attempts, with zero unexpected errors or timeouts. Its different shared runner
fingerprint is
`c349ed44a59e634c5ef189b63523f6842ed7b42a50b3a773089412d312933a58`:
EPYC 9V45, two CPUs, 8,323,969,024 memory bytes; the same OS/kernel/toolchain,
Release flags, filesystem policy and loopback methodology. Ratios below are
internal to this job; admission and dispatch fingerprints are never divided.

| Independent authenticated connection | Serial → bounded p50 ms | p95/p99 ms |
| --- | ---: | ---: |
| First connection idle | 201.521 → 2.096 | 202.176 → 2.212 |
| First connection partial frame | 201.525 → 2.068 | 201.599 → 2.164 |

The 200 ms server deadline deliberately reveals full-connection blocking.
Both variants succeeded; bounded mode admitted two independent slots, serial
mode peaked at one. Twelve measured cycles per round make p95 and p99 both
the maximum sample: this is not precise production-tail estimation.

Distributed read ops/s for 1/2/4 workers were
810.456→791.739 (-2.31%), 784.560→788.161 (+0.46%), and
781.337→798.792 (+2.23%). Sustained read was 794.151→798.170 (+0.51%);
sustained mixed was 165.690→166.899 (+0.73%). Established-query RPC
p50/p99 with the four-worker configuration was 0.053620/0.115393→
0.055273/0.107812 ms; throughput 18,214.318→17,534.331/s (-3.73%).
The tiny-RPC handler gate has measurable overhead and is not advertised as free.
The sustained mixed maximum queue wait also worsened 97.027→568.050 ms;
filesystem/durable-handler stalls remain possible even with FIFO admission.
Bounded connection dispatch does not bound active filesystem service time.

Remote repair/publication/verification/catalog-CAS was 165.273→155.013/s
(-6.21%), p50 5.836→5.880 ms and p99 10.426→10.899 ms. Individual bounded
rounds were 155.013, 121.036 and 167.747/s, versus serial 165.273, 165.903
and 164.407/s. The slow second bounded round spent 118.43 ms in syncs versus
73.76 ms for serial, closely matching its 44.69 ms extra wall time. Variation
is explicitly retained; no universal repair throughput non-regression is proven.
Four-worker read CPU time was 0.109477→0.107032 s; process CPU stayed about
107%. Median peak RSS was 19,324,928→19,193,856 bytes, a small whole-process
observation rather than evidence of lower intrinsic dispatch memory needs.

### Default candidate characterization

[Characterization run 38162994312](https://github.com/MvsArjun/AtlasFS/actions/runs/38162994312)
completed 39 validated reports/11,160 measured cycles, including 960 deliberate
capacity-zero overload attempts, with zero unexpected errors and timeouts.
It uses the admission fingerprint `ec6696a0…`,
not the reproduced M21 fingerprint `b78ff2ba…`. Default FIFO/bounded read
ops/s were 445.017/433.960/428.326 for 1/2/4 workers; queue p99
0.000932/2.441548/8.008566 ms and end-to-end p99
2.655420/4.753834/10.314191 ms. No speedup ratio against the separately
reproduced M21 job is claimed. Characterization includes local counter overhead,
chunk/replication variants, retained startup state and verified recovery cycles.

Remaining design limits include single-handler execution, shared-client
serialization, no handler preemption or rollback, bounded but finite connection
slots vulnerable to idle-peer occupancy until deadlines, coarse tail estimates
in small batches, loopback-only network characterization, unavailable controlled
hardware/physical network measurements, and underlying hosted-filesystem
durability limitations.
