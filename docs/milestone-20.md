# Milestone 20 — end-to-end benchmarks and sustained reliability

## Delivery-state audit

Starting master was clean at `729e6aaa20524f628db75db2187fa768bd60144f`, equal to
fetched origin/master, not the older Milestone 18 commit. The 296-test baseline
passed again locally (exit 0, 24.27 s). Actual Milestone 19 Actions run
[38107405316](https://github.com/MvsArjun/AtlasFS/actions/runs/38107405316) logs
confirmed 296/296 in normal (5.29 s), ASan/UBSan (19.60 s) and TSan (14.85 s),
production build, static analysis, and repeated integration checks. The retained
independent remote fresh clone matched that commit and passed 296/296 (22.80 s).

The audit found no existing operation/resource metric API. The supported model is
still one active coordinator/catalog writer, synchronous persistent clients,
sequential connections per node server, explicitly driven repair/recovery and
operator-controlled epoch activation. Membership changes and failures never
elect a coordinator, retry application work or automatically replan.

## Harness and methodology

`benchmarks/atlasfs_bench.cpp` is optional (`ATLASFS_BUILD_BENCHMARKS=ON`) and has
no GoogleTest/reporting dependency. `scenario.hpp` shares real-operation fixtures
with deterministic stress regressions and reuses the OpenSSL certificate utility;
each run creates disposable keys in a private unique temporary directory. No
keys, certificates, payload datasets or transient builds are committed.

Parameters are iterations, object count, logical bytes, chunk bytes, node count,
replication factor, workers, seed, suite, output and an environment label. Payload
generation uses a specified seeded xorshift64; object selection is a specified
seeded modular sequence. Mixed workloads attempt one write per four requests;
small sample counts need not be exactly 75% reads. Work is deterministic, not an
uncontrolled random traffic generator. Concurrent admission order still depends
on the scheduler; only the generated request set is deterministic. Bounds reject invalid parameters before
creating storage. Each scenario owns its disposable directory; there is no
option that recursively clears a user-supplied storage root.

Each measured operation uses `steady_clock` around its real API call and result
verification, including queue wait for concurrent submitters. Throughput uses
successful logical bytes / batch wall time; operations/sec counts attempts / batch
wall time. Expected rejections and deliberately injected unknown observations
are separate from unexpected errors. Setup/seed/certificate generation, final
disk traversal and JSON serialization are excluded from operation timing.
Thread launch/join and metric aggregation are included in batch wall time.
Nearest-rank p50/p95/p99 use unaggregated operation samples. JSON emits one row per
workload with actual row-specific byte/chunk/node/replica/worker settings, rather
than implying every scenario uses the global cluster settings.

`getrusage(RUSAGE_SELF)` measures whole-process CPU (coordinator plus all in-process
node threads), not per-node CPU or utilization of the entire machine. CPU percent
is 100 * CPU seconds / batch seconds and may exceed 100 on multiple cores. Peak
RSS is the process-lifetime reported high-water mark, not a per-operation or
per-row delta. Linux/MSYS report it in KiB, converted to bytes; unavailable values
are null. MSYS CPU accounting is coarse for small samples. Final apparent disk
file sizes exclude PEM credentials; these are not allocated block sizes or
physical write volume. Temporary residual file bytes are measured. Transient
temporary disk peaks and encrypted network bytes are explicitly **not measured**
and reported as null, rather than inferred from logical payload size.

TLS setup measures client context/key loading + TCP connect + mutual TLS 1.3
handshake, separately from established-connection query RPCs. Storage reopening
measures startup scan + first verified read with existing OS cache: it is NOT a
cache-flushed disk cold-read benchmark. Loopback and in-process nodes are NOT a
WAN, production distributed system or independent-machine network benchmark.

## Workloads and failure boundaries

- Local 1 KiB, 64 KiB and configured-size writes/hot reads; multi-chunk objects;
  existing-object mixed read/write; metadata-scan reopening/first read.
- Real deterministic rendezvous placement, replicated prepare/verify/commit,
  verified reads and mixed updates over TLS with epoch-bearing RPCs. Configurable
  replication and nodes. Workers enqueue to one mutex-serialized coordinator;
  increasing workers measures admission/queue latency, not unsupported concurrent
  writers or parallel per-node connections.
- Remote repair through planning, durable approval/execution, publication,
  reread verification, exact catalog CAS and completion. Repair uses one healthy
  local source, one absent historical replica and a TLS replacement; row node
  count is the two participating live storage nodes, factor two.
- Interrupted repair cycles at all three durable hooks, with journal/catalog
  reopening and explicit same-ID reconciliation. Overall row latency includes
  initial execution plus recovery; nested recovery p50/p95/p99 measures only
  reopening + reconciliation/identity checks. Repair samples are capped to
  min(iterations, objects) to avoid timing already-completed duplicates as repairs.
- Lost repair responses stop the real serving loop after durable publication;
  node restart and exact replay retain the original execution/fingerprint.
  A separate transport-outage workload stops the node before dispatch, rejects
  apparent success and explicitly replays the same request on a fresh session.
- Replicated availability with a node stopped, then physical node reconstruction
  and restart. Partial activation blocks delivery to one participant, preserves
  unresolved intent, queries explicitly, then resumes the original activation
  and rejects mutation requests from persistent obsolete sessions. Partition
  injection suppresses delivery; it is not a real network topology partition.

Timeout counts include typed timeout outcomes the public API exposes; some
transport failures/timeouts intentionally collapse into `result_unknown`, so
zero typed timeouts is not proof no transport deadline could ever expire.
Unknown counters count intermediate observations even when explicit recovery
subsequently succeeds. No scenario silently changes request identity to retry.

## Measurement environment and original bottleneck

Local measurements: AMD Ryzen 5 6600H (6 cores/12 logical processors), approximately
15.2 GiB visible RAM, Windows 11 10.0.26300, MSYS2 3.6.10, GCC 15.3.0, CMake 4.4.4,
OpenSSL 3.6.5, Ninja, Release. Ephemeral storage uses the OS temporary directory
on the local SSD environment (Micron/SK Hynix disks present; physical volume
mapping was not controlled). No CPU affinity, frequency/power lock, cache purge
or external background-load isolation was applied. Numbers are reproducible
workload observations, not hardware-independent SLAs. Sanitized/Debug/shared-CI
timings are correctness diagnostics, not comparable performance baselines.

Before optimization, 2 MiB hot reads consumed about 99% process CPU and delivered
about 22 operations/sec. A separate `-pg -g` instrumented Release execution of
the SAME real local suite and GNU gprof attributed 99.41% of sampled CPU time
(6.74 of 6.78 seconds; 6808 calls) to bitwise CRC-32C. This is a CPU sample
profile, not a claim that 99.41% of all wall time is checksum work.

Changed only CRC calculation: an immutable constexpr 256-entry (1 KiB) table
replaces eight bit transitions per input byte. Polynomial, initialization,
final complement, arbitrary byte alignment, checksum values and all persisted/
wire fingerprints are unchanged. There are no platform-instruction requirements
or mutable initialization. Every integrity check, filesystem synchronization,
publication verification and catalog CAS remains. A standard check vector and
new bitwise-reference tests cover multiple seeds, lengths, unaligned spans,
every byte value, changed bytes and 1 MiB inputs.

Three sequential uninstrumented runs per variant used identical parameters:
40 iterations, 16 objects, 2 MiB configured size, 1 MiB chunks, seed 42, local
suite, same compiler/build/machine label. Median run throughput:

| Workload | Before ops/s | After ops/s | Ratio |
| --- | ---: | ---: | ---: |
| 1 KiB write | 62.92 | 59.30 | 0.94x |
| 64 KiB write | 58.78 | 59.29 | 1.01x |
| 64 KiB hot read | 663.90 | 1209.91 | 1.82x |
| 2 MiB write | 18.21 | 34.38 | 1.89x |
| 2 MiB hot read | 22.17 | 99.43 | 4.49x |
| 2 MiB mixed | 19.85 | 53.10 | 2.67x |
| Reopen + first 2 MiB read | 12.60 | 21.48 | 1.71x |

Small-write fluctuations are reported, not claimed as improvements. Timings are
not statistically powered significance tests. Raw p50/p95/p99, CPU and retained
disk values for every run are in `benchmarks/results/msys-before-{1,2,3}.json`
and `msys-after-{1,2,3}.json`; use the comparison tool for individual pairs.
Both variants were measured in an uncommitted benchmark working tree based on
Milestone 19; source_commit denotes that parent commit, not a clean benchmark
release. CRC source SHA-256 before:
`23aba020fcb46a03318eb0df1b238dca243fdd243b5c6de931816404352181ab`;
after: `adb12d2c73f0cabca67639cf2b0c586a37e2d8f9f1ad5044b9a94613d1ff513f`.
After outputs also include this source hash. Report-only harness additions
between variants did not alter real-operation boundaries or workload volume.

## Sustained results and scaling observations

The three sustained runs performed 11,448 measured workload cycles: local 8,000,
distributed 3,128 and fault/recovery 320. All had zero unexpected errors and zero
residual temporary bytes. Individual fault cycles contain multiple actual RPCs
and state transitions; cycle counts are not a count of network messages.

| Sustained workload | ops/s | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 16 KiB local mixed, 4 KiB chunks | 139.70 | 1.68 | 25.36 | 26.63 |
| 64 KiB replicated write, four nodes, factor two | 5.89 | 169.40 | 185.87 | 212.65 |
| Replicated read, four queued workers | 109.26 | 36.57 | 39.07 | 40.14 |
| Replicated mixed, four queued workers | 18.36 | 217.02 | 234.65 | 252.74 |
| TLS context + connect + handshake | 192.16 | 5.20 | 6.11 | 6.45 |
| Established epoch-query RPC | 491.78 | 2.05 | 2.49 | 2.72 |
| Remote 16 KiB repair through verification/CAS | 8.39 | 117.95 | 140.35 | 148.72 |

Recovery-only p50/p95/p99 at the three repair crash windows were respectively
64.69/74.27/77.02 ms before publication, 30.50/38.39/62.01 ms after publication,
and 15.76/22.58/28.01 ms after catalog CAS. Lost-response node restart + exact
replay + verified read was 37.33/53.09/74.01 ms. No committed catalog transition
was undone, and physical publication alone never created authority. Forty partial
epoch cycles explicitly resolved forty intermediate unknown observations and
rejected forty stale requests, with 53.84/63.10/128.65 ms overall cycle latency.

Whole-run reported peak RSS was 11.64 MiB local, 16.38 MiB distributed and
16.73 MiB fault/recovery. Local batches measured 187.56 CPU seconds / 240.12
wall seconds; distributed batches 41.22 / 65.46 seconds. Maximum retained
apparent data/journal bytes at row boundaries were 6,422,912 local and 16,978,528
distributed. These are process/in-process-node observations, not independent
server resource measurements or transient disk peaks.

Separate sequential one-factor sweeps (no simultaneous benchmark processes):

- Requested mixed-object counts 4/64/256, with 20 iterations, retained 12/104/296
  total local manifests including the size-specific workloads. Reopen/first-read
  p50 grew 7.15 -> 54.01 -> 147.33 ms. Startup scans all current manifests and
  generation directories; the MSYS filesystem emulation contributes CPU overhead.
  The long-local run's 256 manifests gave 130.12 ms p50. This is a concrete scan
  growth finding, not evidence of identical Linux SSD costs.
- For 64 KiB writes, 4 KiB versus 64 KiB chunks gave 24.67 versus 46.85 ops/s;
  p50 39.87 versus 16.81 ms. More files/checks/synchronizations incur overhead.
  This does not justify omitting synchronization or verification.
- Fixed four-node/factor-two read submissions: one worker yielded 134.64 ops/s,
  p50 6.81 ms; four workers 153.51 ops/s, p50 25.87 ms. Variance changes throughput
  modestly, while queueing multiplies latency; workers do NOT create parallel
  coordinator/node dispatch. Mixed p50 grew 10.22 -> 168.87 ms.
- Factors one/two/three read p50 were 3.75/6.81/10.94 ms. Source inspection confirms
  `ReplicaManager::get` intentionally reads/verifies every replica to return
  complete observations, even after finding valid bytes. Skipping replicas would
  change behavior, so this milestone does not optimize it away. PREPARE also
  sends full logical object bytes to each participating node, even if assigned
  only some chunks; actual encrypted network bytes remain unmeasured.
- Two versus four nodes at factor two gave similar verified-read throughput
  (135.62 versus 134.64 ops/s), consistent with serialized dispatch. Write numbers
  varied non-monotonically across factors/nodes; do not claim linear write scaling
  or a statistically significant winner from these single runs.

Raw sweep outputs use `msys-objects-*`, `msys-chunk-*`, `msys-workers-*`,
`msys-replicas-*`, and `msys-nodes-2.json` in `benchmarks/results/`. The three
sustained runs are `msys-long-local.json`, `msys-long-distributed.json` and
`msys-fault.json`. Nineteen small JSON reports total about 90 KiB; no datasets are
retained. Background builds/checks overlapped parts of the sustained run, so its
absolute numbers are observational. The scaling sweeps ran sequentially after
those checks; none of these laptop runs had external background-load/power
isolation. A dedicated Linux lab remains necessary for production capacity claims.

## Reliability verification

The expanded suite passed 309/309 locally (exit 0, final 39.09 s): original
296 plus two CRC reference tests and eleven stress tests. These repeat 128
overwrite/read/checksum cycles; multi-object node restarts/unavailability; lost
responses and repeated byte-equivalent replay; all pre-publication fault stages
with zero residual temporary files; multiple recovery interruptions at each
crash window; concurrent requests through one executor (one completion/CAS);
competing CAS (one winner, one conflict); stale repair evidence; corrupt/missing
epoch state; corrupt repair journals; eight partial epoch activations with stale
requests during and after explicit resume; and immutable historical records.
Existing epoch, TLS, catalog CAS and recovery regression tests remain unchanged.
Thirty-seven stress/epoch/repair cases also passed three repetitions (111 test
executions, exit 0, 57.43 s). Cppcheck 2.21 checked 23 implementation/harness files
successfully after removing an unnecessary certificate-fixture string copy.
A clean MSYS2 production-only Release build passed (exit 0).

Fault hooks model interrupted application steps and durable record reopening,
not power-cut testing of the filesystem/device. Failed publication can retain
non-authoritative orphan bytes; temporary cleanup does not imply a global data
rollback. No correctness failures were observed in these checks; performance
did not justify relaxing any invariant.

## Reproduction

```sh
cmake -S . -B build-bench -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build-bench --parallel 4
./build-bench/atlasfs_bench --suite local --iterations 40 --objects 16 \
  --bytes 2097152 --chunk 1048576 --seed 42 --output local.json
./build-bench/atlasfs_bench --suite local --iterations 1000 --objects 64 \
  --bytes 16384 --chunk 4096 --seed 42 --output long-local.json
./build-bench/atlasfs_bench --suite distributed --iterations 600 --objects 128 \
  --bytes 65536 --chunk 16384 --nodes 4 --replicas 2 --workers 4 --seed 42 \
  --output long-distributed.json
./build-bench/atlasfs_bench --suite fault --iterations 40 --objects 40 \
  --bytes 65536 --chunk 16384 --seed 42 --output fault.json
python3 benchmarks/compare.py before.json after.json
python3 benchmarks/validate.py benchmarks/results/*.json
```

Use `--label` for actual hardware/environment identity. To reproduce the original
bitwise variant, use a separate checkout retaining this harness with only
`src/checksum.cpp` from Milestone 19; do not reset the current published branch.
For profiling, configure a separate build with `-DCMAKE_CXX_FLAGS='-pg -g'`
and `-DCMAKE_EXE_LINKER_FLAGS=-pg`, run from that build directory, then
`gprof ./atlasfs_bench gmon.out`. Do not compare profiled timings to ordinary builds.

Correctness/build commands:

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure --timeout 60
ctest --test-dir build -R 'Stress|EpochRepair|EpochActivation|EpochRpc' \
  --repeat until-fail:3 --output-on-failure --timeout 60
cppcheck --enable=warning,performance,portability --std=c++20 --inline-suppr \
  --suppress=missingIncludeSystem --error-exitcode=1 -I include src benchmarks
```

Production-only build uses `-DBUILD_TESTING=OFF`, optionally leaving benchmarks
off to build only libraries. CI compiles normal/ASan+UBSan/TSan variants, runs all
CTest regressions, and separately smoke-runs the full benchmark with three
iterations, three objects, 4096 bytes, 1024-byte chunks and two submitters. JSON
schema, zero unexpected errors and percentile ordering are checked, without any
wall-clock performance threshold. Actual final counts, exit codes, fresh-clone
results and CI links are reported in the delivery report; workflow text alone is
not evidence of sanitizer success.

The first published Milestone 20 CI run passed all normal and sanitizer tests,
but the normal job stopped at Cppcheck: the analyzer could not infer the CLI's
nonzero-object precondition at repair-history seeding. An explicit empty-workload
guard now protects that access; no diagnostic suppression or invariant relaxation
was introduced. The final delivery report identifies the subsequent verification
run rather than treating the initially failed run as successful.

## Remaining scope and next engineering priority

No consensus, election, multi-coordinator operation, automatic replanning,
parallel connection serving or automatic identity-changing retry was added.
Single-owner catalog snapshots and synchronous clients remain requirements.
Epoch fencing still requires operator quiescence and complete participant
inventory; independently rooted writers/uncontacted nodes under partitions are
not made safe by this benchmark.

Follow-up priorities: measure startup scan and catalog snapshot growth on a
dedicated Linux host/real durable disk; add transport byte/syscall and disk peak
instrumentation; investigate verified-manifest reuse and catalog snapshot costs
without dropping freshness/integrity checks; then design bounded parallel node
dispatch/admission separately with explicit ownership. Larger changes need a
separate milestone and a new correctness model, not optimistic benchmark numbers.
