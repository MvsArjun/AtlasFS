# Milestone 22 result provenance

These are repeated **shared GitHub-hosted Linux** observations, not production
capacity, dedicated-hardware results, physical-network measurements or power-loss
guarantees. Each final A/B pair used the same clean Release binary and runner,
identical work volume, seed, resource limits, warmups and instrumentation. Three
rounds alternate pair order; no cross-fingerprint timing ratio is claimed.

See [Milestone 22](../../../docs/milestone-22.md) for interpretation, test results,
design limits and the initial adverse measurements. The original M20/M21 result
files are unchanged.

## Successful workflow artifacts

| Directory | Workflow run | Artifact ID | Source commit | Artifact reports | Retained here |
| --- | --- | ---: | --- | ---: | --- |
| `reproduced-baseline` | [38113891187](https://github.com/MvsArjun/AtlasFS/actions/runs/38113891187) | 11692920915 | `323872f75f5650d56e89bf6f919573cccc4a113d` | 39 | Full summaries |
| `initial-admission` | [38115003308](https://github.com/MvsArjun/AtlasFS/actions/runs/38115003308) | 11692563806 | `020c8fab50ad9e3f1e266e2b2f8c3578efca38fd` | 30 | Full summaries and all six sustained-workload raw reports |
| `initial-dispatch` | [38115005101](https://github.com/MvsArjun/AtlasFS/actions/runs/38115005101) | 11692394259 | `020c8fab50ad9e3f1e266e2b2f8c3578efca38fd` | 36 | Full summaries |
| `admission` | [38162678128](https://github.com/MvsArjun/AtlasFS/actions/runs/38162678128) | 11709928919 | `115415fe1d0c447c2dcdabce98dbb17e284265fa` | 30 | All raw JSON, index, 15 comparison CSVs and full summaries |
| `dispatch` | [38162680828](https://github.com/MvsArjun/AtlasFS/actions/runs/38162680828) | 11711240583 | `115415fe1d0c447c2dcdabce98dbb17e284265fa` | 36 | All raw JSON, index, 18 comparison CSVs and full summaries |
| `characterize` | [38162994312](https://github.com/MvsArjun/AtlasFS/actions/runs/38162994312) | 11711270766 | `3eb679b2cd7b980e86512c91a57c18be337832cc` | 39 | Full summaries and round-one 1/2/4-worker raw reports |

Artifact names are `linux-benchmark-hosted-admission`,
`linux-benchmark-hosted-dispatch`, or `linux-benchmark-hosted-characterize`, matching
the profile. Workflow artifacts contain every report plus exact commands and
environment metadata; artifact retention is finite. Final paired raw datasets
are also checked in here so those comparisons remain independently inspectable.
Partial retained datasets deliberately omit `index.json`: their full summaries
must not be regenerated from only the representative reports.

SHA-256 of the downloaded ZIP artifacts (not of the extracted directory):

| Directory | ZIP bytes | SHA-256 |
| --- | ---: | --- |
| `reproduced-baseline` | 194873 | `998b085cd7631f26045d3f6fb554b104a3800a2b12f855d95ec5dc88cb51b84a` |
| `initial-admission` | 209913 | `7352c727656b0f823f43c052ffbf6bbaddd007ebf28342582665a417dafc7c06` |
| `initial-dispatch` | 236943 | `3dfe123dc2404cc284b18ef4f0a3754e22eaa8c3574897de23f86139c7aa7aab` |
| `admission` | 213166 | `58de6d6ceac5224ee0206902603cef361730a6bb2bb6307815c9ad54fd736ec5` |
| `dispatch` | 241773 | `b6056f8f1ed08f0713178d4bb669bafefc16896de3b9168b11022e2820266579` |
| `characterize` | 221112 | `ee8347680af9f7788b58481f4cc1a2cf64c2663172ac19dc248ef03e6578e232` |

The final runtime source-tree checksum is
`5677661cfb33a60b467a5316a5c28e26eb7a0a0509877465f5716725a8e77ebf`.
The `3eb679b` correction changed a handoff-bound test and documentation, not
runtime code. Measurement source/build metadata is preserved as recorded; later
documentation commits are not retroactively substituted into old reports.

## Machine identities and method

| Datasets | Machine fingerprint | CPU | Memory bytes |
| --- | --- | --- | ---: |
| Reproduced baseline, initial dispatch | `b78ff2ba8010ed1c02bbea6f7573983212914cd35ffd7f4d5a1b4ace9e275a56` | EPYC 7763 | 8323964928 |
| Initial admission | `45ffedee19174ba645d32cde353b2908f17586b6c36ca9254b384ee57db53fa0` | EPYC 9V74 | 8323969024 |
| Final admission, final characterization | `ec6696a00ab35046df0c1cae4f42e5f3f55c614c45938afd67a24d7eefaa1c7d` | EPYC 7763 | 8323969024 |
| Final dispatch | `c349ed44a59e634c5ef189b63523f6842ed7b42a50b3a773089412d312933a58` | EPYC 9V45 | 8323969024 |

All exposed two CPUs with affinity `{0,1}` on Ubuntu 24.04.5,
kernel `6.17.0-1022-azure`, GCC 13.3, CMake 3.31.6, Ninja 1.13.2,
OpenSSL 3.0.13 and Release `-O3 -DNDEBUG`. They used warm filesystem cache
without cache purging and loopback mutual TLS with TCP_NODELAY. Exact VM storage,
mount, compiler and command details are embedded in the JSON environment.
Hosted ext4 settings include `journal_async_commit`, `nobarrier`, `commit=30`
and `data=writeback`; virtual storage and these settings do not establish
power-loss durability or controlled-device sync capacity.

The fairness pairs keep server dispatch serial. Distributed 1/2/4-worker cases
use seed 42, 80 measured cycles, 16 retained 64 KiB objects, 16 KiB chunks, four
nodes, R2 and four separate warmups. The four-worker sustained case uses 400
cycles; the fault suite uses 20. Coordinator waiting capacity is 16 with a
5,000 ms budget. The dispatch pairs hold coordinator admission at FIFO and
compare serial/bounded serving; four configured workers, four waiting request
slots and a 1,000,000-request connection limit stay constant. Independent idle
and partial-frame TLS probes use 12 cycles, two warmups, a 200 ms server deadline
and 1,000 ms client deadline. These probes do not mutate catalog state.

Summaries contain medians of per-round metrics, **not pooled percentiles**.
Maximum wait/latency columns are maxima across all measured rounds. In a
400-operation mutex batch, three starving callers may be invisible to p99;
maximum wait and the deterministic FIFO tests are needed to interpret fairness.
CPU is whole-process utilization; peak RSS is a process high-water mark rather
than a per-operation allocation or physical-disk bound.

Final admission: 30 reports, 24,384 measured cycles. Final dispatch: 36 reports,
24,528 cycles. Final characterization: 39 reports, 11,160 cycles. Each has zero
unexpected errors and zero timeouts. Their 3,840 / 3,840 / 960 capacity-zero
overload attempts are deliberate non-executed rejections with zero RPCs, not
successful storage work. Other expected fault rejections and ambiguous RPC
outcomes remain explicit; no automatic mutation retry is introduced.
`operations_per_second` counts offered cycles; use
`successful_operations_per_second` when discussing successful-work capacity.

## Inspect and reproduce retained comparisons

From the repository root:

```sh
python3 benchmarks/validate.py benchmarks/results/m22/admission/workers-4-fifo-1.json
python3 benchmarks/compare.py --allow-admission-change \
  benchmarks/results/m22/admission/workers-4-mutex-1.json \
  benchmarks/results/m22/admission/workers-4-fifo-1.json
python3 benchmarks/compare.py --allow-dispatch-change \
  benchmarks/results/m22/dispatch/dispatch-serial-1.json \
  benchmarks/results/m22/dispatch/dispatch-bounded-1.json
python3 benchmarks/summarize.py benchmarks/results/m22/admission
python3 benchmarks/summarize.py benchmarks/results/m22/dispatch
```

The comparison flags allow only the selected policy change; limits, deadlines,
work volumes, warmups, instrumentation and fingerprint still must match.
Summarization refuses mixed fingerprints. Initial results, including the
observed 14.30% mixed-throughput regression, are retained separately, not hidden
by a different-runner final measurement. Final small read/RPC costs and variable
repair/mixed sync stalls also remain visible. No universal throughput
non-regression or production tail-latency guarantee is established.
