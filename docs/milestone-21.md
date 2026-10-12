# Milestone 21 — Linux characterization and explicit I/O observations

## Baseline and environment status

The audited baseline is clean master/origin/master
`e68c1ec5890c9937298944c16005bb1c3f8204a3`. All nineteen Milestone 20 reports remain
unchanged. The unchanged suite passed 309/309 again on MSYS2 (exit 0, 37.04 s),
and the baseline smoke validated 22 workloads/66 measured operations. Actual
[Milestone 20 Linux CI](https://github.com/MvsArjun/AtlasFS/actions/runs/38110030512)
logs independently confirmed normal 309/309 (8.21 s), ASan/UBSan 309/309 (39.92 s),
TSan 309/309 (21.18 s), validated benchmark smoke and the normal production build.

The local Windows machine has neither WSL nor Docker, and no controlled Linux
runner was supplied. This does not authorize installing a new OS or inventing a
remote host. Linux verification uses GitHub-hosted Ubuntu 24.04. Longer runs on
that runner are explicitly **shared-runner observations**, not dedicated hardware
capacity or an SLA. The manual workflow also supports an operator-provided,
exclusive runner labeled `self-hosted`, `linux`, `atlasfs-benchmark`. A label alone
does not establish exclusivity, stable frequency, or an isolated disk.

## Instrumentation ownership and safety

`metrics::Collector` is opt-in. `metrics::Scope` attaches a shared owner to the
calling thread; producers must explicitly attach their own scopes. Benchmark
workers and storage-serving threads inherit the intended collector explicitly.
Production callers without a scope are disabled by default. There is no mutable
process-global collector, protocol change, or durable-format change. A timer owns
its collector, even if the attaching scope ends before the timer does.

Counters are fixed-size relaxed atomics with saturating 64-bit addition; overflow
sets a flag rather than wrapping. Snapshots load counters independently, so they
are not transactions. Exact batch deltas require quiescent boundaries; reset is
supported only with producers quiescent. Reset affects counters, not disk gauges.
The benchmark takes deltas rather than resetting live producers. No event list,
payload, key, certificate, fingerprint, execution ID or epoch token is exported
by this collector. Internal file/descriptor maps are mutex-protected and bounded
to 65,536 entries each (a smaller limit is supported). Hitting the bound or failing
observation invalidates accounting and produces null peaks, not fabricated values.
The existing bounded benchmark latency samples remain separate from counters.

Observational code preserves syscall results/errno, retry loops, TLS verification,
timeouts, synchronization ordering, publication, catalog CAS, and same-identity
recovery. Instrumentation adds atomics, clocks, bookkeeping locks and filesystem
observations; it is not free. `--metrics off` provides an explicit equivalent
uninstrumented workload for measuring overhead. This milestone does not claim
that instrumentation itself makes application operations faster.

## Transport layers and boundaries

Per-row counters aggregate attached coordinator and storage threads, not one
connection or one server. Logical object bytes remain the successful workload
bytes in the existing throughput metric. Other layers are distinct:

| Metric | Meaning |
| --- | --- |
| serialized payload sent/received | Full canonical request/response payloads of completed frames, including metadata and epoch envelopes; not just object data |
| framed sent | Completed sends: serialized payload + unchanged 24-byte header per frame |
| framed received | Bytes returned to the parser by the stream, including incomplete/malformed frames |
| TLS application sent/received | Successful `SSL_write_ex`/`SSL_read_ex` byte results; includes protocol framing, excludes TLS records |
| socket sent/received | Positive `send`/`recv` returns in the actual custom OpenSSL socket BIO or plaintext stream |

Socket TX includes TLS handshake and record bytes accepted by the kernel. Socket
RX includes TLS buffering/read-ahead. It excludes TCP/IP headers, retransmissions,
ACKs and NIC traffic; it is **not physical wire capture**. For an entire successful
loopback exchange, aggregate TX already counts each sender once; adding RX would
double-count traffic. Failed/partial exchanges need not have balanced counters.
The difference between socket and TLS application bytes includes handshake,
records and window/read-ahead effects; independent per-record overhead and exact
handshake-byte attribution remain unavailable. No silent wire-byte estimate is
made. BIO attempts and positive short returns have separate call/partial counters.
A short receive means fewer bytes than requested, not a protocol error.

TCP connection timing covers `connect_loopback`; TLS timing covers handshake plus
peer certificate/allowlist validation on both endpoints, excluding context/key
loading. The existing setup row includes context/key load, connect and handshake
end-to-end. RPC timers include request encoding, stream I/O, parsing and matching
response ID; server dispatch is included in end-to-end waiting. RPC failures and
deadline expirations are counted without introducing retries or changing timeout
classification. A transport error can remain a public `result_unknown`.

OpenSSL's custom BIO and buffered-read semantics are documented in
[BIO methods](https://docs.openssl.org/3.3/man3/BIO_meth_new/) and
[SSL reads](https://docs.openssl.org/3.5/man3/SSL_read/).

## Filesystem and transient storage

Wrappers observe actual AtlasFS POSIX reads/writes, `fsync`/`fdatasync`, renames,
unlink and recursive cleanup in object, epoch, membership, coordinator-operation,
repair-plan, remote-node journal and catalog paths. Existing ostream journal
flushes and completely decoded istream records are explicitly observed as file
bytes, but their library-internal syscall counts are unavailable. File read/write
byte totals therefore combine positive wrapped syscall results and observed
successful stream data; they are not block-device write volume. Failed stream
reads/writes may be only partially observed. Chunk/manifest bytes, catalog/journal
write bytes and stream flush counts remain distinguishable. Unclassified state
files fall into the journal-write category. File/directory sync attempts and
latency include retries individually; failed attempts have a separate counter.
Rename latency measures the rename itself, excluding subsequent accounting.

At each quiescent row boundary, `begin_files` inventories the private fixture
root before timing, excluding PEM credentials. Opened/written files, destination
replacement, directory/file rename and removal update the apparent-file gauge.
Files below transactions and `.tmp`, `.atlasfs-tmp-`, `.repair-` names count as
temporary. Moving a transaction generation transfers its files to final storage
without counting new disk allocation; committing a temporary manifest similarly
subtracts temporary bytes while preserving its file bytes. Removing/replacing
files subtracts their tracked bytes. The benchmark compares final accounting to
an independent filesystem traversal and fails if complete accounting disagrees.

`temporary_peak_bytes` is peak simultaneous temporary **regular-file apparent
bytes**, including any temporary baseline. `additional_disk_peak_bytes` is peak
regular-file bytes minus the initial baseline. Neither is allocated disk blocks,
directory metadata, COW overhead, cache memory, or device writes. The accounting
requires a private root and no unobserved external writers.
The gauge follows AtlasFS's close-before-rename/unlink publication discipline;
it is not an inode tracker for arbitrary hard links or writes through renamed
open descriptors. Concurrent updates are safe, but do not constitute a global
filesystem transaction or a physical allocation trace. Failure hooks and
restart cleanup are tested; orphan final generations can remain non-authoritative.
Cleanup counts apply to tracked files, not empty directories.

On Linux, per-row `/proc/self/io` deltas report rchar/wchar, syscall counts,
read_bytes/write_bytes and cancelled_write_bytes. These cover the entire process
and its waited-for children, eligible file/library syscalls and metric reads;
they are not socket-traffic counters and cannot be
attributed exactly to one object or node. rchar/wchar are not physical disk bytes.
Block-layer counters are cache/filesystem dependent; cancelled writes are kept
separate, not silently subtracted. Unsupported/inaccessible proc metrics are null.
See the [Linux I/O counter semantics](https://www.man7.org/linux/man-pages/man5/proc_pid_io.5.html).

Publication, verification (reread and integrity checks), catalog CAS and completion
persistence have separate aggregate call/time counters. These instrument the
normal execution path; post-CAS recovery shortcuts need not traverse every stage.
Startup counters cover object cleanup scans, catalog load, repair-plan load and
coordinator/remote journal scans. Entries count actually visited directory entries
plus catalog-file inspection, not an inferred object count. They do not imply
that every visited entry was valid or decoded successfully.

## Workloads and repeated methodology

The existing seeded real-operation suites are retained. Schema 2 adds explicit
`--warmup N` (default 3), `--metrics on|off`, startup rows, queue wait percentiles,
counter deltas, process I/O and per-row peaks. Warmups run before timing/counter
boundaries. Warmup operation indices and execution IDs are disjoint from measured
ones so repairs/replays are not silently recycled. Distributed initial-write and
repair fixtures seed `objects + warmup` candidates; measured counts still report
only the requested operations. Fault histories can retain warmup outcomes.

The `startup` suite seeds the configured retained object/chunk count, one
coordinator operation per object, per-chunk catalog records and actual generated
repair plans. It separately reopens/verifies storage and reopens the durable
coordinator catalog/journals/plans without executing abandoned historical work.
Each row records the actual retained counts. Seeding and baseline inventory are
excluded from startup timing. OS caches are warm, not purged or device-cold.

Concurrent workers still queue to one coordinator mutex. Queue samples time lock
acquisition; end-to-end samples include that wait. The synchronous client and
sequential connection-serving node remain unchanged. This is supported admission
concurrency, not multiple catalog writers, parallel node dispatch or consensus.

`linux_run.py` records distribution/kernel, CPU/model/affinity, memory capacity,
filesystem mount/options, accessible storage information, compiler/CMake/Ninja/
OpenSSL, governor, Git revision/dirty state, seed/configuration, UTC start time,
unique run ID and exact command with every result. The executable also records
configure-time commit, compiled-source-tree SHA-256, dirty state, build flags and
run start. A revision mismatch fails and requires reconfiguration. File bytes in
the hash reflect the actual checkout (including line endings), not an assumed
source release. Missing environment commands/fields are null.

The characterization profile performs three sequential rounds of: collector
off/on local operations (1 KiB/64 KiB/1 MiB), distributed workers 1/2/4, retained
objects 8/32/128 with four chunks each, chunk size 4 KiB/64 KiB, factors 1/3 and
fault/recovery operations. The off/on order alternates between rounds. Each
workload warms separately. Summary values are medians of per-round metrics;
median p95 is **not a pooled p95**, and no statistical significance is claimed.
The normal shared-runner smoke profile has no speed threshold.

`schema-v2.json` is the machine-readable structural contract. `validate.py`
evaluates its used keywords with the standard library, then checks throughput
arithmetic, counters, framing, peaks and percentile/sample consistency. Legacy
Milestone 20 schema 1 remains supported. Comparison rejects differing configs,
flags, labels and Linux machine fingerprints; instrumented/uninstrumented
comparisons require `--allow-instrumentation-change` and describe overhead, not
an implementation improvement. Do not compare MSYS and Linux as a speedup.

## Reproduction and verification

```sh
export CXX=g++-13
cmake -S . -B build-bench -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DATLASFS_BUILD_BENCHMARKS=ON -DCMAKE_CXX_COMPILER=g++-13
cmake --build build-bench --parallel 4
python3 benchmarks/linux_run.py --executable build-bench/atlasfs_bench \
  --output-dir build-bench/results --runner-class manual-uncontrolled \
  --profile characterize --rounds 3
python3 benchmarks/summarize.py build-bench/results
python3 benchmarks/compare.py --allow-instrumentation-change \
  build-bench/results/local-off-1.json build-bench/results/local-on-1.json

cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure --timeout 60
ctest --test-dir build -R 'Metrics|TlsRepair|EpochRepair' \
  --repeat until-fail:3 --output-on-failure --timeout 60
cppcheck --enable=warning,performance,portability --std=c++20 --inline-suppr \
  --suppress=missingIncludeSystem --error-exitcode=1 -I include src benchmarks
```

Ordinary CI runs normal/ASan+UBSan/TSan, full tests, enabled/disabled 24-row
benchmark smoke, schema/arithmetic validation, comparison, static analysis and
production builds. The separate manually dispatched Linux characterization
workflow uploads all repeated JSON, overhead CSVs, index and compact summaries.
Actual final counts, run links and source revisions are recorded in the delivery
report; workflow configuration is not evidence of success.

The instrumented revision `60a749a36bdb19910c4d71ebc7cc4d4165e07a35` passed
323/323 in [Linux CI](https://github.com/MvsArjun/AtlasFS/actions/runs/38111594966)
(normal 8.70 s, ASan/UBSan 25.79 s, TSan 21.87 s). The ownership correction
`1e92eb37525232ebaa266b1cf658f21ef267ca40` passed 324/324 in
[Linux CI](https://github.com/MvsArjun/AtlasFS/actions/runs/38112095094)
(normal 7.34 s, ASan/UBSan 25.99 s, TSan 21.32 s). All jobs also validated
enabled/disabled smoke reports. A separate fresh clone of the latter revision
passed 324/324 (41.51 s) and 24-row/72-operation smoke, exit 0.

Sixteen new instrumentation regressions cover concurrent totals, saturation,
snapshot/reset/scopes, timer lifetime, disabled I/O, errno, plaintext layer totals,
encrypted BIO traffic, deadlines, successful/failing temporary publication,
destination replacement, restart cleanup, lost-response idempotence, capacity
and concurrent file lifecycles with descriptor reuse, plus TCP_NODELAY on/off.
Ownership review identified
that retiring descriptor tracking after kernel close could erase a newly reused
descriptor's observation. Tracking now retires before close. Five stress rounds
did not reproduce the race before the fix; the ordering argument, not a claimed
observed failure, motivated it. Recursive-cleanup errors invalidate accounting
because deletion may have made partial progress. Six Python tooling regressions
exercise schema, arithmetic, framing, integer types, unavailable accounting and
explicit transport/config comparison requirements.
All original checksum/durability/TLS/epoch/repair/CAS tests remain intact.

## Measured Linux baseline

[Characterization run 38111605139](https://github.com/MvsArjun/AtlasFS/actions/runs/38111605139)
completed successfully at `60a749a36bdb19910c4d71ebc7cc4d4165e07a35` with
39 sequential reports (13 configurations, three rounds), 10,200 measured cycles,
351.36 s summed measured-row wall time, zero unexpected errors and zero residual
temporary bytes. Maximum process-lifetime RSS across these runs was 19,222,528
bytes; it includes fixture setup, not a per-operation allocation measurement.
All reports passed schema and arithmetic validation. The full raw reports/index,
summaries and paired overhead CSVs are in artifact `linux-benchmark-hosted-characterize`
(artifact ID 11691987052). Compact CSV and two representative raw reports are
retained in [benchmarks/results/m21](../benchmarks/results/m21/README.md).

The runner exposed Ubuntu 24.04.5 LTS, kernel 6.17.0-1022-azure, AMD EPYC 7763
with two available logical processors (affinity 0,1), 8,323,969,024 bytes RAM,
GCC 13.3.0, CMake 3.31.6, Ninja 1.13.2 and OpenSSL 3.0.13. Release flags were
`-O3 -DNDEBUG`. Scratch storage was `/dev/sda1`, ext4, a reported 75G virtual
disk; its physical backing and cache behavior are unknown. Mount options were
`rw,relatime,discard,journal_async_commit,nobarrier,errors=remount-ro,commit=30,data=writeback`.
In particular, these observations do **not** establish physical power-loss
durability or controlled-device sync performance. Required application syncs
remain present. No CPU isolation, frequency control or cache purge was performed.
Nodes ran in one process with separate roots and loopback TLS, not on separate
machines or a physical network.

Values below are medians of three per-round measurements, not pooled percentiles.
The baseline uses default TCP packetization (TCP_NODELAY disabled).

| Workload | Samples per round | ops/s | p50 / p95 / p99 ms |
| --- | ---: | ---: | --- |
| Hot local 1 MiB read, metrics on | 60 | 166.33 | 6.01 / 6.10 / 6.15 |
| R2 replicated 64 KiB read, workers 1 | 80 | 9.61 | 84.57 / 124.12 / 125.00 |
| Same, workers 2 | 80 | 9.61 | 123.85 / 124.87 / 8320.51 |
| Same, workers 4 | 80 | 9.61 | 123.91 / 124.17 / 8321.48 |
| R1 replicated read | 40 | 23.80 | 42.00 / 42.13 / 42.37 |
| R3 replicated read | 40 | 6.02 | 166.02 / 166.15 / 166.31 |
| TLS context/TCP/handshake | 80 | 358.63 | 2.78 / 2.84 / 3.02 |
| Established query RPC | 80 | 8111.48 | 0.12 / 0.13 / 0.14 |
| Remote 16 KiB repair, verification and CAS | 20 | 11.25 | 88.93 / 89.86 / 90.84 |

Object/chunk/replication configurations, per-case elapsed seconds, CPU, queue,
I/O, sync, peak-space and startup counters are in the compact CSV. RPC latencies
include framing/parsing; the handshake workload also includes context/key loading.

### Byte and stage evidence

The representative 80-cycle R2 read consumed 8.3196 s, 0.2227 s process CPU
(2.68%), and 640 RPCs with 8.2683 s aggregate RPC time. There were no file writes
or syncs. Logical bytes were 5,242,880; serialized TX 10,587,200; framed/TLS
application TX 10,617,920; actual encrypted socket TX 10,660,160 (RX equal).
The 42,240-byte difference is aggregate TLS/protocol-layer overhead within this
established-connection window, not physical TCP/IP overhead. It is not divided
into independent per-record and handshake byte costs. Wrapped file reads totaled
10,606,720 bytes; `/proc/self/io` reported rchar 10,606,836 (including observation
reads), read_bytes 0 because the working set was hot, and no block writes.

The representative 20-cycle remote-repair batch consumed 1.7763 s. RPC time was
1.6924 s; file syncs were 200 calls/57.75 ms, directory syncs 220 calls/33.86 ms,
and renames 200 calls/12.57 ms. Stage totals were publication 873.80 ms,
verification 818.93 ms, CAS 14.35 ms, completion 12.15 ms. Nested stages and
RPC totals must not be summed as independent wall-time components.
Wrapped writes were 393,667 bytes: chunks 327,680, manifests 2,346, catalog
41,990, journal 21,651. Process block-layer write_bytes was 1,064,960, not the
same quantity as those application writes. Temporary peak was 16,384 bytes;
additional apparent-file disk peak above baseline was 338,855 bytes. Peak
accounting includes publication/rename/replacement and is independently checked
against final filesystem state; no physical block-allocation peak is claimed.

### Startup and serialization

At 8/32/128 retained objects (32/128/512 chunks, corresponding catalog entries,
operation records and repair plans), storage reopen p50 was 0.28/0.87/3.31 ms;
coordinator reopen p50 was 0.45/1.68/6.93 ms. Each point used 20 measured reopens
and three warmups per round. This is observed scaling at these sizes, not proof
of a complexity class. Startup validates retained state without executing old
operations. See raw retained-count fields for each component.

Increasing workers from 1 to 4 did not increase baseline read throughput.
The coordinator-side workload deliberately holds one admission mutex across
the shared, non-thread-safe clients and mutable orchestration. `TcpServer::serve`
also handles an accepted connection synchronously, including its request loop.
These are distinct serialization boundaries, not multiple active coordinators.
In workers-4 round 1, queue p50/p95/p99 were 0.00018/0.00233/8237.56 ms, while
end-to-end p99 was 8321.48 ms: unfair mutex reacquisition can starve one worker
until almost the batch end. Percentiles with 80 samples have coarse tail resolution.

### Collector overhead

Paired local off/on runs alternate order in round two. Hot 1 MiB read throughput
was 166.80 versus 166.33 ops/s (0.28% observed reduction); hot 1 KiB read was
29,297.15 versus 23,810.36 ops/s (18.73% reduction). Local 1 KiB write was
678.58 versus 627.11 ops/s (7.58% reduction). Clocks, atomics and bounded
bookkeeping matter most for very short operations. These are shared-runner
observations, not fixed overhead guarantees or an optimization speedup.

## Findings and remaining scope

### Measured packetization improvement

Low CPU use, zero syncs and about 40 ms latency steps implicated transport waits.
OpenSSL also documents interaction between TLS traffic and Nagle buffering;
see [SSL_connect documentation](https://docs.openssl.org/3.0/man3/SSL_connect/).
Rather than removing verification or syncs, a configurable TCP_NODELAY option
was added only at secure TCP client/server endpoints, before handshake. Direct
TLS ByteStream Unix sockets and plaintext compatibility sockets are unchanged.
No framing, timeout, retry, authentication or durable ordering was modified.

[Paired experiment 38112712691](https://github.com/MvsArjun/AtlasFS/actions/runs/38112712691)
passed at `aacc5ecdb1f97360b8bead71b1c582f3957eed03`: the same Release binary and
Linux runner executed off/on, on/off, off/on rounds sequentially. Each used
seed 42, four nodes, R2, 64 KiB objects, 16 KiB chunks, 16 retained objects,
40 iterations, one worker, four warmups per workload, metrics enabled. Writes
measured 16 cycles per round; reads and other distributed workloads measured 40.
All six reports validated: 1,296 measured cycles, 39.49 s summed measured time,
zero unexpected errors and zero residual temporary bytes. Artifact
`linux-benchmark-hosted-transport` (ID 11692302899) includes exact command,
environment and each paired comparison. The fixture, shared runner and cache
limitations above apply equally to both sides; comparing different jobs' VM
measurements is not used as optimization evidence.

| Workload | Off ops/s | On ops/s | Off p50 / p95 / p99 ms | On p50 / p95 / p99 ms |
| --- | ---: | ---: | --- | --- |
| Replicated read | 9.614 | 451.784 | 84.475 / 124.083 / 124.121 | 2.203 / 2.279 / 2.291 |
| Replicated write | 5.457 | 52.511 | 183.079 / 186.171 / 186.171 | 18.646 / 21.093 / 21.093 |

These are medians of three per-round values. Read throughput improved 46.99x
and write throughput 9.62x in this workload. Individual read improvements ranged
43.45x–47.06x. This intervention supports the packetization hypothesis; packet
traces isolating delayed ACK behavior were not collected. Read encrypted socket
TX stayed 5,330,080 bytes per 40-cycle round on both sides. CPU utilization rose
from 2.76% to 109.43% of one CPU (the process uses coordinator and server threads;
100% is not a multi-core upper bound). CPU seconds decreased from 0.1150 to
0.0969. Process-lifetime RSS median was 19,165,184 bytes on both sides. TLS
setup and tiny established query RPC did not show a comparable improvement:
their per-round variation remains visible in the checked-in CSVs.

Based on this evidence, secure TCP now defaults to TCP_NODELAY enabled.
Applications may set `TlsConfig::tcp_nodelay=false`; the benchmark accepts
`--tcp-nodelay off`. JSON records the policy, and `compare.py` rejects different
policies unless `--allow-transport-change` is supplied. Run the paired workflow
with `profile=transport`, or run `linux_run.py --profile transport --rounds 3`.
The opt-out does not reset externally configured descriptors: it leaves new
TCP sockets at the OS default. The setting is never applied to non-TCP TLS
streams. Extra small packets or TCP/IP headers are a possible tradeoff;
physical packet counts/headers/retransmissions were not measured. No socket
byte reduction or real-network speedup is claimed.

A dedicated Linux baseline is
unavailable without a supplied controlled runner; shared observations must not
be substituted for that missing capacity evidence.

### Final optimized profile and verification

[Final-code verification](https://github.com/MvsArjun/AtlasFS/actions/runs/38112997172)
passed for `ca687d0f5dec3b3b379f664b07992d6006f5683c`: normal 325/325 (5.96 s),
ASan/UBSan 325/325 (16.58 s), TSan 325/325 (18.55 s), all exit 0. Each job
validated 24-workload/72-cycle enabled and disabled smoke, including the six
Python checks. Normal CI also passed Cppcheck, production-only build, seven
TLS-repair tests repeated ten times (70 executions, 1.61 s), and 31 epoch
activation/RPC/repair/catalog tests repeated ten times (310 executions, 6.78 s).

Local optimized MSYS2 verification passed 325/325 (39.57 s), Cppcheck 2.21
over 24 translation units, production-only Release build, enabled/disabled and
TCP_NODELAY opt-out smoke validation and six tooling checks, all exit 0.
Thirty-one Metrics/TLS-repair/epoch-repair cases repeated three times passed
(93 executions, 14.87 s). A newly cloned checkout of the published optimized
revision independently configured/fetched GoogleTest, built, passed 325/325
(40.00 s), and validated 24 workloads/72 cycles and the six tooling checks.
Windows and MSYS Git initially disagreed on CRLF normalization in that clone;
setting that clone's `core.autocrlf=true` aligned them. Reconfigured benchmark
metadata reflects the same source bytes without relabeling an uncommitted edit
as a published clean source. Correctness runtimes are not benchmark speedups.

[Final characterization 38113019605](https://github.com/MvsArjun/AtlasFS/actions/runs/38113019605)
passed at the same optimized revision: 39 reports, 10,200 measured cycles,
32.36 s summed measured-row time, zero unexpected errors and zero residual
temporary bytes. Artifact `linux-benchmark-hosted-characterize` (ID 11692496391)
contains all raw reports and exact environments/commands. Maximum process RSS
was 19,668,992 bytes. [optimized-summary.csv](https://github.com/MvsArjun/AtlasFS/blob/7779ea7c38e2128013e32068bd437eb785ff8915/benchmarks/results/m21/optimized-summary.csv)
retains all 72 repeated workload groups, sample counts, wall times, percentiles,
CPU/RSS, queue, byte, sync and peak-space values.

**This job ran on a different exposed CPU: AMD EPYC 9V45**, two available
processors. Distribution/kernel/RAM and tool versions matched the recorded
baseline, and the virtual ext4 mount had the same unsafe-for-power-loss-evidence
options. Its fingerprint is
`c349ed44a59e634c5ef189b63523f6842ed7b42a50b3a773089412d312933a58`, versus the
baseline/A-B fingerprint
`ec6696a00ab35046df0c1cae4f42e5f3f55c614c45938afd67a24d7eefaa1c7d`.
Do not turn these jobs' runtime difference into an optimization ratio. Only the
same-job TCP_NODELAY pair above supports the claimed improvement.

Final-profile observations (medians of three per-round measurements):

| Workload | Samples/round | ops/s | p50 / p95 / p99 ms | Queue p99 ms |
| --- | ---: | ---: | --- | ---: |
| R2 64 KiB read, workers 1 | 80 | 809.23 | 1.219 / 1.282 / 1.410 | 0.00020 |
| Same, workers 2 | 80 | 810.50 | 1.226 / 1.286 / 98.555 | 97.183 |
| Same, workers 4 | 80 | 788.50 | 1.224 / 1.423 / 101.184 | 99.808 |
| Remote 16 KiB repair | 20 | 158.88 | 6.224 / 6.785 / 6.808 | unavailable |

Read CPU was about 106–107% of one CPU; extra workers still did not increase
throughput, and unfair queue tails remain visible. At 8/32/128 objects, final
storage startup p50 was 0.214/0.659/2.504 ms; coordinator startup p50 was
0.331/1.234/5.095 ms. These are new-environment observations, not startup
optimization comparisons.

In final repair round 1, twenty repairs took 125.88 ms: RPC aggregate 48.67 ms,
file syncs 53.37 ms and directory syncs 31.86 ms. Publication/verification/CAS/
completion stage totals were 45.99/3.00/12.97/12.25 ms. Sync costs are now a
substantial part of this particular workload; they were not removed. Write
byte categories, 200 file syncs, 220 directory syncs, 16,384-byte temporary
peak and 338,855-byte additional apparent-file peak matched the baseline
work volume. Physical storage behavior remains unknown. Longer controlled-host
sync and batching experiments would require a separately reviewed design that
preserves commit acknowledgement and failure semantics.

The serial coordinator queue and sequential server dispatch are architectural
boundaries. Startup scanning and catalog snapshot growth remain investigation
targets; observations at a few retained sizes do not establish an asymptotic
complexity class. Do not remove syncs or freshness/integrity checks to improve a
graph. Consensus, election, multiple active coordinators, automatic replanning,
parallel server redesign, cache purging and power-cut/device testing remain out
of scope. Recommended next step: repeat this profile on an operator-isolated
Linux host/disk and physical network, then evaluate bounded admission scheduling
or an explicitly designed server-dispatch milestone using the queue evidence.
