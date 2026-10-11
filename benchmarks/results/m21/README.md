# Milestone 21 result provenance

These are **shared GitHub-hosted Linux observations**, warm filesystem cache,
one process, separate node roots and loopback TLS. They are not production
capacity, physical-network measurements or dedicated-hardware results.

Baseline source: `60a749a36bdb19910c4d71ebc7cc4d4165e07a35`.
[Successful workflow](https://github.com/MvsArjun/AtlasFS/actions/runs/38111605139),
artifact `linux-benchmark-hosted-characterize` (ID 11691987052), contains all
39 reports and exact commands/environments. `baseline-summary.csv` contains
medians of three per-round metrics (not pooled percentiles), total samples and
measured wall time. It was regenerated with the expanded summary columns;
raw inputs are unchanged. `baseline-workers-1.json` and `baseline-fault-1.json`
are unchanged representative reports copied from that artifact. JSON lacking
`tcp_nodelay` predates that option and used Nagle's default behavior.

All nineteen Milestone 20 result files remain unchanged. Large generated datasets
remain workflow artifacts rather than being checked in.

Transport A/B source: `aacc5ecdb1f97360b8bead71b1c582f3957eed03`.
[Successful paired workflow](https://github.com/MvsArjun/AtlasFS/actions/runs/38112712691),
artifact `linux-benchmark-hosted-transport` (ID 11692302899), contains all six
reports from the same runner/job. `transport-summary.csv` and `transport-1..3.csv`
show the three rounds; `transport-off-1.json` / `transport-on-1.json` retain an
unchanged raw pair. Pair order was off/on, on/off, off/on. Use
`python3 benchmarks/compare.py --allow-transport-change transport-off-1.json transport-on-1.json`.

Optimized profile source: `ca687d0f5dec3b3b379f664b07992d6006f5683c`.
[Successful full workflow](https://github.com/MvsArjun/AtlasFS/actions/runs/38113019605),
artifact `linux-benchmark-hosted-characterize` (ID 11692496391), contains the
39 final raw reports. `optimized-summary.csv` is its unchanged summary.
This runner exposed EPYC 9V45 instead of EPYC 7763: **do not compare different
jobs' timings as an optimization ratio**. Use the same-job transport A/B pair
for that claim. Across baseline, paired transport and optimized full profiles,
84 reports validated 21,696 measured cycles with zero unexpected errors and
zero residual temporary bytes.

For methods, metric attribution, mount/durability limitations, verification and
interpretation, see [Milestone 21](../../../docs/milestone-21.md).
