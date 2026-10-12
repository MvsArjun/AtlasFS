# Milestone 23 candidate characterization

These are shared GitHub-hosted Linux, warm-cache, loopback observations, not
production capacity, exclusive hardware, physical-network or power-loss results.
All 39 raw reports, index, full summaries and three instrumentation comparison
CSVs are retained. Earlier M20–22 measurements are unchanged.

| Provenance | Value |
| --- | --- |
| Workflow | [38204468584](https://github.com/MvsArjun/AtlasFS/actions/runs/38204468584), completed success |
| Source | `cfab4ff2035de439ee79ee20c9009deccdef5abd`, clean checkout |
| Artifact | `linux-benchmark-hosted-characterize`, ID `11726677541`, 222,382 ZIP bytes |
| ZIP SHA-256 | `89fcc9485476fc22abf1ad499975a5d257d1ab463d8b2498f80427e6548052ca` |
| Compiled runtime source-tree SHA-256 | `5745b9067b4c15e0c11798f44eb3b412dab8ffd5897e96e3e9389b574f6cf64d` |
| Machine fingerprint | `45ffedee19174ba645d32cde353b2908f17586b6c36ca9254b384ee57db53fa0` |
| Build | GCC 13.3.0, Release `-O3 -DNDEBUG`, CMake 3.31.6, Ninja 1.13.2, OpenSSL 3.0.13 |
| Runner | Ubuntu 24.04.5, kernel 6.17.0-1022-azure, AMD EPYC 9V74, 2 logical CPUs, 8,323,969,024 memory bytes |

The hosted ext4 mount reports `journal_async_commit,nobarrier,data=writeback`
and `commit=30`; passing application fsync tests on this environment is explicitly
not physical-device durability evidence. Exact environment, commands, workload,
source/build metadata and run IDs remain in each JSON and `index.json`.

The existing characterize profile ran three rounds of local instrumentation
on/off, 1/2/4-worker replicated read/mixed/write/RPC workloads, 4/64-KiB chunks,
1/3 replicas, fault recovery and 8/32/128 retained-state startup scenarios.
Validation passed for all 39 reports and 11,160 measured cycles: zero unexpected
errors/timeouts, 960 deliberate capacity-zero overloads, and 180 deliberately
injected unknown outcomes across lost-repair-response, transport-outage and
partial-activation scenarios. Every instrumented report has complete accounting;
the disabled reports correctly have no counters. Unknown is a classification,
not an extra successful operation or an unexpected test error.

| Read workload | Median ops/s | Median queue p99 ms | Median end-to-end p99 ms |
| --- | ---: | ---: | ---: |
| 1 worker | 576.525 | 0.001041 | 1.957979 |
| 2 workers | 559.987 | 1.941204 | 3.797810 |
| 4 workers | 553.372 | 5.721488 | 7.588951 |

Medians summarize per-round metrics; percentiles are not pooled. More workers
still queue behind one durable handler. The fingerprint differs from M22; these
numbers must not be used for a cross-job speedup or regression ratio. M22's FIFO,
small-RPC, repair-sync and sustained-mixed trade-offs remain documented, not
replaced by this sample. The extra node intent synchronization is a correctness
cost; no paired claim of universal throughput non-regression is made.

From the exact source commit, configure production Release with benchmarks ON,
then run on Linux:

```sh
python3 benchmarks/linux_run.py --executable build-production/atlasfs_bench \
  --output-dir build-production/results --runner-class shared --profile characterize --rounds 3
python3 benchmarks/summarize.py build-production/results
```

Use `controlled` only on operator-controlled exclusive hardware and
`manual-uncontrolled` for an ordinary developer machine. To validate/regenerate
the retained dataset, run `python3 benchmarks/validate.py` with all raw JSON
reports (exclude `index.json` and `summary.json`) and
`python3 benchmarks/summarize.py benchmarks/results/m23`. Summary generation
does not rerun workloads. See [release readiness](../../../docs/release-readiness.md)
for correctness gates, audit findings and remaining operational requirements.
