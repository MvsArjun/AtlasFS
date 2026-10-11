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
requires a private root and no unobserved external writers. Failure hooks and
restart cleanup are tested; orphan final generations can remain non-authoritative.
Cleanup counts apply to tracked files, not empty directories.

On Linux, per-row `/proc/self/io` deltas report rchar/wchar, syscall counts,
read_bytes/write_bytes and cancelled_write_bytes. These cover the entire process
and its waited-for children, sockets, libraries and metric reads; they cannot be
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

Fourteen new instrumentation regressions cover concurrent totals, saturation,
snapshot/reset/scopes, timer lifetime, disabled I/O, errno, plaintext layer totals,
encrypted BIO traffic, deadlines, successful/failing temporary publication,
destination replacement, restart cleanup, lost-response idempotence and capacity.
All original checksum/durability/TLS/epoch/repair/CAS tests remain intact.

## Findings and remaining scope

No application speedup is claimed before equivalent Linux rounds are available.
Instrumentation overhead itself must be reported. A dedicated Linux baseline is
unavailable without a supplied controlled runner; shared observations must not
be substituted for that missing capacity evidence.

The serial coordinator queue and sequential server dispatch are architectural
boundaries. Startup scanning and catalog snapshot growth remain investigation
targets; observations at a few retained sizes do not establish an asymptotic
complexity class. Do not remove syncs or freshness/integrity checks to improve a
graph. Consensus, election, multiple active coordinators, automatic replanning,
parallel server redesign, cache purging and power-cut/device testing remain out
of scope. Recommended next step: repeat this profile on an operator-isolated
Linux host/disk, then select a targeted change using its stage and byte evidence.
