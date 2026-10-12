# AtlasFS Milestone 23: release readiness

## Recommendation and supported scope

**Conditionally ready for an audited library/source candidate, proposed
`0.1.0-rc.1`; not a general-purpose production storage release.** The final
verification ledger below must be complete before this recommendation is used.
No tag or formal GitHub release is created: the repository has no established
tag, packaging or release policy. CMake's project version remains `0.1.0`.

AtlasFS supports one active coordinator/catalog authority, independently owned
POSIX storage roots, synchronous local or loopback authenticated node RPCs,
explicit recovery and repair, and bounded concurrent connection I/O. Handlers
remain serialized behind one admission gate. Shared clients/control-plane objects
need the same external serialization when called directly. This is reusable
C++20 code plus `atlasfs_bench`, not a coordinator daemon, config-file loader,
service installer or deployable multi-host cluster distribution. No license or
packaged release artifact was present in the audited repository.

Consensus, leader election, automatic coordinator failover, independent catalog
writers, automatic retry/replanning/repair, background recovery, credential
management and parallel durable handlers are intentionally unsupported. Epoch
fencing does not make multiple active coordinators safe. Passing tests is not
proof of every hardware, filesystem, security or failure guarantee.

## Baseline and publication provenance

Audit began from a clean `master` and matching `origin/master` at
`1576ef4517bcf12138c4cb0e78cb721244912268`. Native Git fetched origin and tags;
there were no tags. Milestones 15–22 are ancestors: CAS `1b538b2`, remote repair
`aaf9f33`, crash reconciliation `8010540`/`f8284be`, TLS `56eccf3`, epochs
`729e6aa`, benchmarks/stress `8cd38b4`/`e68c1ec`, instrumentation and Linux
characterization `60a749a`/`aacc5ec`/`ca687d0`/`323872f`, and scheduling/dispatch
`020c8fa`/`115415f`. The handoff-test correction is `3eb679b`; `1576ef4` delivers
the final M22 results. The latest pre-audit runtime revision is
`115415fe1d0c447c2dcdabce98dbb17e284265fa`, not the later report commit.

[Baseline CI 38163952042](https://github.com/MvsArjun/AtlasFS/actions/runs/38163952042)
actually succeeded at `1576ef4`: normal, ASan/UBSan and TSan each passed 365/365
tests (7.15/29.16/22.99 seconds), both benchmark smoke modes and 14 tool tests.
Normal also passed Cppcheck, production-only Release, 70 TLS, 310 epoch and
420 scheduling repeated executions. The local baseline reproduced 365/365 in
42.30 seconds with exit 0 and a production-only Release build.

The audited runtime/test/workflow revision is
`cfab4ff2035de439ee79ee20c9009deccdef5abd`, normally pushed to `origin/master`.
The independent remote clone used that exact clean revision. Later delivery
changes only release evidence/documentation and retained benchmark reports;
runtime and test trees are unchanged. Final delivery SHA/CI are checked again
and reported in the publication handoff; baseline success is not substituted.

## Confirmed defects and focused fixes

| Finding | Correction and regression evidence |
| --- | --- |
| Membership, ACOP and repair stores remained writable from old RAM after candidate rename followed by sync failure | Sticky per-instance `durability_unknown()` rejects every further mutation, including no-ops; recovery/repair does no remote work from poisoned journals. Membership eligibility becomes empty. Three baseline regressions failed; affected persistence gate passed 52/52. |
| Replica commit directory-sync failure could acknowledge a duplicate and discard rollback evidence | Sticky per-replica uncertainty blocks commit, abort and finalize; local query retains UNKNOWN and replication does not blindly roll back. Two ObjectStore tests failed before fixing; local end-to-end unknown-commit regression passes. |
| Replica rollback could restore old bytes, fail synchronization and subsequently report committed | Post-visibility or cleanup uncertainty is sticky; subsequent terminal calls/finalize cannot falsely succeed. Regression covers manifest sync, generation sync and prepared-generation cleanup; failed before fix. |
| Node physical action/terminal RAM state preceded durable journal acknowledgement; lost journal publication became duplicate success | Durable UNKNOWN intent precedes COMMIT/ABORT. Exact same-process journal-only replay can finish a known terminal publication without repeating physical action; surviving UNKNOWN cannot be adopted on restart. Failed baseline commit/abort/prepare publication regressions and counted-action restart regressions cover this. |
| Node operation/repair state was not covered by the optional epoch-token checksum | New RECORD1 wrapper uses existing CRC-32C over all payload fields, strict version/footer parsing, and 4-KiB regular-file reads; state/status corruption and malformed wrappers reject startup. Legacy compatibility is explicit, not equivalent protection. |
| Several startup readers allocated before bounding persistent files | AMBR/ACOP/ARPL and node readers bound regular files before allocation/read, reject symlinks/FIFOs, truncation, read failures and trailing data. Oversize/nonregular regressions cover each store. Trusted exclusive roots are still required. |
| ACOP validation rejected legitimate M19 epoch errors | Accept defined `stale_epoch`/`epoch_unresolved`; reject negative/unknown values without changing format version. |
| Membership version could wrap UINT64_MAX to zero, including fresh-health publication | All real registry mutations fail with appended `version_exhausted` errors before changing evidence; no-ops still succeed. Heartbeat rejects failed health mutation. Four baseline failures; metadata 67/67 and 120 focused repeated executions passed. |
| Repeating durable completion of committed history attempted forbidden COMMITTED→COMMITTING | Preserve committed lifecycle while checking fresh participant evidence. Two regressions failed before fix; repeated completion has no extra commit/finalize actions and unavailable participants are not guessed successful. Affected ACOP/recovery gate passed 18/18. |
| Startup treated data behind a corrupt published manifest as an orphan and reclaimed it | Validate every published logical manifest before generation reclamation; fail startup preserving corrupt manifest and generation evidence. Preservation regression failed on the original cleanup. Nonregular entries must fail without FIFO blocking. |

There are 34 added regression tests and two strengthened corruption-restart tests.
Failure evidence was retained in the ignored
`build-m23/` audit logs, not removed to obtain a pass. No invariant or original
test was weakened. Coordinator operation identity/placement/participants are
immutable; latest observations can change through explicit recovery. COMMITTED
cannot regress. ABORTED describes coordinator attempt knowledge and may be
revised by explicit recovery after an uncertain PREPARE; it is not abort consensus.

## Build and verification ledger

Prerequisites: POSIX, C++20, CMake 3.20+, Ninja, Git, Threads and OpenSSL 3.0+
development files. Tests fetch GoogleTest v1.15.2 (resolved commit
`b514bdc898e2951020cbdca1304b75f5950d1f59`) on first configure. Production builds
with `BUILD_TESTING=OFF` avoid that dependency. Linux verification uses Ubuntu
24.04/GCC 13; local MSYS uses GCC 15.3.0, CMake 4.4.4 and OpenSSL 3.6.5.
MSYS means the POSIX MSYS runtime, not native MinGW. Package-manager versions are
recorded observations, not a bit-for-bit pinned operating-system image.

From the exact desired Git revision (configure again after changing revisions):

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure --timeout 60
cppcheck --enable=warning,performance,portability --std=c++20 --inline-suppr \
  --suppress=missingIncludeSystem --error-exitcode=1 -I include src benchmarks
cmake -S . -B build-production -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build-production --parallel 4
ctest --test-dir build -R TlsRepair --repeat until-fail:10 --output-on-failure --timeout 60
ctest --test-dir build -R 'EpochActivation|EpochRpc|EpochRepair|EpochCatalog' --repeat until-fail:10 --output-on-failure --timeout 60
ctest --test-dir build -R 'AdmissionQueue|Dispatch|SchedulingIntegration' --repeat until-fail:10 --output-on-failure --timeout 60
ctest --test-dir build -R 'CoordinatorOperationRecord|CoordinatorOperationStore|DurableRecoveryCoordinator|DurableMembership|FailureDetector|NodeRegistry|RepairPlanStore|RepairExecutor|StorageNodeProtocol|ObjectStore|StorageIntegration.ReportsPersistedCorruption|ReplicaManager\.LocalCommitDirectorySyncUnknown' --repeat until-fail:10 --output-on-failure --timeout 60
./build-production/atlasfs_bench --iterations 3 --objects 3 --bytes 4096 --chunk 1024 \
  --workers 2 --warmup 1 --output build-production/on.json
./build-production/atlasfs_bench --iterations 3 --objects 3 --bytes 4096 --chunk 1024 \
  --workers 2 --warmup 1 --metrics off --output build-production/off.json
python3 benchmarks/validate.py build-production/on.json build-production/off.json
python3 benchmarks/compare.py build-production/on.json build-production/on.json
python3 benchmarks/tool_test.py build-production/on.json
```

For a true fresh clone: `git clone https://github.com/MvsArjun/AtlasFS.git atlasfs-rc`,
`cd atlasfs-rc`, check `git rev-parse HEAD` against the published SHA, then run the
same configuration/build/test commands in new build directories. Authenticate
through the credential manager/SSH, never through committed credentials.

Linux sanitizer flags and halt-on-error settings are exactly in
[verify.yml](../.github/workflows/verify.yml): ASan/UBSan uses
`-fsanitize=address,undefined -fno-omit-frame-pointer`, TSan uses
`-fsanitize=thread -fno-omit-frame-pointer -fno-pie -no-pie`. All matrices run full
CTest and both validated smoke modes; normal runs static analysis, production
Release and all four repeated integration groups. No native Windows sanitizer
result is claimed.

| Post-fix requirement | Actual evidence/status |
| --- | --- |
| Local Debug/full CTest | Final integrated working tree: 399/399, 43.82 s, exit 0; 34 added tests and two strengthened tests |
| Production Release and Cppcheck | Final runtime build passed, `BUILD_TESTING=OFF`, benchmarks ON; Cppcheck 25 translation units, no diagnostics, exit 0 |
| Local repeated integration/smoke/schema/tool checks | 118 storage/persistence/recovery/repair tests ×10 = 1,180 executions, 86.35 s, exit 0. TLS 7×10 = 70 (9.07 s), epochs 31×10 = 310 (43.12 s), scheduling 42×10 = 420 (35.38 s) also passed during integration; exact final-source repeats run in CI. Both smoke modes: 27 workloads/81 cycles each; 14 tool tests and all 98 retained historical JSON reports validated, exit 0. |
| Linux normal/ASan/UBSan/TSan | [38204466311](https://github.com/MvsArjun/AtlasFS/actions/runs/38204466311), exact `cfab4ff`, all completed success: 399/399 each, normal 8.42 s, ASan/UBSan 31.90 s, TSan 19.28 s; exit 0, no sanitizer diagnostics |
| Linux repeated/static/production/smoke checks | Same run: 70 TLS, 310 epoch, 420 scheduling, 1,180 persistence/storage/repair/recovery repeated executions; normal Cppcheck 25/25 and production Release passed. Every matrix passed both smoke modes (27 workloads/81 cycles each), accounting checks, schema/arithmetic/self-comparison and 14 tool tests, exit 0. |
| Fresh remote clone | `build-m23-fresh/source`, clean exact `cfab4ff`; new configure/dependency checkout/Debug build, 399/399 in 44.58 s; new production-only Release and both 27-workload/81-cycle smoke modes plus 14 tool tests, exit 0; smoke metadata confirms exact SHA and `source_dirty=false` |
| Representative Linux characterization | [38204468584](https://github.com/MvsArjun/AtlasFS/actions/runs/38204468584), exact clean `cfab4ff`, Release; 39 validated reports/11,160 cycles, zero unexpected errors/timeouts, 960 injected overloads and 180 expected injected unknown outcomes; all instrumented accounting complete; exit 0 |
| Final diff/docs/remote/clean tree | Candidate normal push verified by `git ls-remote`; clean tree, diff whitespace check, no tracked certificate/key files. Evidence delivery revalidated all 137 retained raw reports and 28 relative Markdown links; final remote/clean tree/CI are checked again before handoff. |

The earlier failed M22 [run 38162648935](https://github.com/MvsArjun/AtlasFS/actions/runs/38162648935)
remains historical evidence: a test incorrectly bounded handoff peak at three
instead of four; `3eb679b` corrected that accounting expectation. It was not a
TSan race finding. Any M23 failed CI will be recorded here with its resolution.

During the final local gate, 398/399 passed (46.55 seconds, shell invocation exit 1): the existing
logical-object integration fixture still expected startup to accept a truncated
published manifest. The stricter preservation contract now rejects startup;
the test was strengthened to require preserved manifest/generation evidence and
also retains live GET corruption rejection for a damaged chunk. This was an
intentional fail-closed behavior change, not removal of corruption coverage.

## Security and epoch operation

Code enforces TLS 1.3 mutual certificate validation, configured trust roots,
validity/purpose checks, an exact single SAN URI, immutable endpoint identity,
role/target allowlists and separate epoch-activation permission. Secure storage
endpoints reject generic object RPCs and never downgrade to plaintext. Storage
identity must match the node; mutating enrolled RPCs and catalog/repair operations
hold the matching epoch guard through publication. Generic TLS `TcpServer` users
must supply their own application policy; direct local APIs are trusted, not an
authenticated service. CRC/FNV fingerprints are integrity/placement tools, not
cryptographic authenticators or collision-resistant malicious-peer proofs.

Operators must use the explicit TLS constructors on both ends and remove
plaintext listeners, provision each peer with the intended role/node URI and
target permissions, protect private keys and writable roots with owner-only
permissions, maintain reliable clocks, and distribute current trust/CRLs out of
band. The code does not enforce credential file ACLs. Optional configured CRLs
check the complete chain and fail closed for missing required CRLs; without CRLs,
trusted unexpired revoked certificates can remain usable. Configuration is a
connection/context snapshot: drain/close existing sessions and replace endpoints
for rotation/revocation, not just replace a disk file. No resumption or early data
is enabled. An authorized compromised coordinator can still perform permitted
mutations; no Byzantine protection is claimed. Test/benchmark credentials are
ephemeral; no tracked certificate/key files were found in this audit.

Enrollment and replacement are explicit API operations, not CLI commands:

1. Stop/quiesce all old writers. Inventory the complete participant set and
   record trusted independent minimum/high-water checkpoints for every root.
2. Provision never-enrolled coordinator/node epoch stores offline. Do not
   delete or restore enrollment state to bypass provisioning checks.
3. Configure authenticated owners and `may_activate_epoch` permissions. Persist
   the exact successor intent and use `epoch::Coordinator` activation; only an
   exact successor token confirmed by every configured participant enables work.
4. Partial/unknown activation blocks work. Explicit reconcile queries; explicit
   resume retries only that same activation. Neither elects a coordinator.
5. Preserve external high-water checkpoints before restart/restore. A new fenced
   session cannot automatically adopt old epoch-bound repair executions or
   unfinished unfenced plans. Stop old binaries before upgrade; do not downgrade.

Missing/corrupt/rolled-back enrollment or trusted checkpoints requires operator
investigation, not creating a fresh authority. Local cooperative locks do not
merge independently loaded catalogs. A partitioned uncontacted node retains its
old token; complete-inventory activation and quiescence remain essential.

## Persistent compatibility and durability boundaries

| Record/protocol | Supported read/write and bound |
| --- | --- |
| AMNF object/generation manifest | v1 CRC-32C, at most 786,530 bytes; object limit 16 MiB minus 512 bytes, 65,536 chunks, generation ID 64 bytes |
| AMBR membership | v1 CRC-32C, 2 MiB, 4,096 nodes; node ID 64/host 253 bytes; version exhaustion fails closed |
| ACOP operation history | v1 CRC-32C, 8 MiB, 1,024 participants; original identity/placement frozen; observations explicitly refreshed |
| ARPC current catalog | v1 CRC-32C, 64 MiB, 65,536 records, 4,096 replicas per chunk; global-version CAS and exhaustion checks |
| ARPL repair plan | reads v1–v4, writes v3 unfenced/v4 epoch-bound, CRC-32C, 1 MiB; v1/v2 lack catalog evidence and cannot execute |
| Node operation/repair journal | reads legacy text and RECORD1, writes RECORD1 full-payload CRC-32C, 4 KiB; old readers unsupported after upgrade |
| AEPC epoch state | v1 CRC-32C, 1 MiB, 4,096 activation participants; independent checkpoint required |
| Wire/storage/epoch envelope | ATFS v1 framing, default 16 MiB; canonical storage v2 only; epoch mutation envelope v3 (320-byte overhead reserve); read/query v2 and activation/query types 214–218 v3 |

Published logical-object manifests and control records are validated before use;
invalid records stop startup without silently replacing them or reclaiming data
behind corrupt manifests. Chunk/generation corruption is checked on reads, not
by a complete startup scrub. Temporary work is not promoted on startup. Legacy constructors are
unprovisioned development-only; unfenced catalog/server constructors refuse
enrolled roots. Direct ObjectStore/LocalStorageNode APIs do not independently
authenticate/fence a root. Legacy text journals lack whole-record checksums and
cannot retrospectively establish unrecorded integrity; migrate only under
quiescence with verified backups and reject ambiguous evidence rather than
inventing a successful transition. There is no automated bulk migration utility.

File-sync, atomic rename and parent-directory-sync implement publication under
the documented POSIX filesystem assumptions. A post-rename failure poisons the
current coordinator store; reads of old RAM are diagnostic, not permission to
write. Stop the instance, preserve files, reopen and inspect validated surviving
state explicitly. A surviving node UNKNOWN is not success or permission to abort.
Same-process exact journal-only replay is narrower than restart adoption.
The development logical-object PUT path predates replica intent tracking: a
post-rename sync error can return `filesystem_error` with new bytes visible, so
inspect surviving state before any explicit retry; do not treat that error as
proof of no effect. It is not the authenticated storage-node mutation endpoint.
Physical bytes alone never authorize a repaired replica: publication, fresh
verification and exact catalog CAS are all required. Post-CAS missing/corrupt data
leaves completion pending without undoing the recorded catalog transition.

Some constructors create missing directory trees without synchronizing every
new ancestor link. Operators must durably preprovision the entire root/journal
directory hierarchy (and synchronize changed parent links) before use. Hosted
tests do not establish fresh-root creation or real physical power-loss durability.
No guarantee is made for hardware/filesystems that violate fsync/rename behavior.

## Backup, recovery and resource operation

Quiesce the single authority and all node writers; take a coordinated snapshot
of object/chunk roots, node operation/repair journals, AMBR/ACOP/ARPC/ARPL and
epoch/enrollment files. Keep credentials separately protected and external epoch
high-water checkpoints outside restored snapshots. Preserve participant identity
and exact directory ownership; test restores offline with the current binary and
checksums before activation. Missing ordinary stores can mean legitimate first
use, so AtlasFS cannot distinguish trusted deletion/rollback from a fresh empty
root. Compare against an external backup/inventory/checkpoint rather than allowing
startup to recreate an authority. Never edit a journal to resolve an unknown.

After reopening, explicitly inspect original operation ID/fingerprint/participant
evidence through recovery. Complete/rollback only when fresh state authorizes it;
unavailable, corrupt, conflicting and unknown participants block guessing.
Repair needs explicit approval and original evidence; reconcile exact execution
identity only. Stale membership/catalog/epoch evidence requires investigation,
not automatic replan. Do not convert an unknown into success by retrying under
a new operation/execution ID. Retain visible non-authoritative bytes for diagnosis
and back up before any operator-directed cleanup.

Default server bounds are four fixed connection workers, four queued handler
admissions, one active handler, a million requests per connection, and absolute
handshake/frame/admission/response deadlines. FIFO prevents bypass, not durable
handler preemption. Overload closes the connection without automatic retry;
failure after sending a mutation may be unknown. Stop cancels pending work and
unblocks sockets but cannot undo an active durable operation. Callers must stop
and join serve before destroying the server; a trusted handler that never returns
can prevent shutdown indefinitely. Disconnect detection is best effort, not
transaction cancellation. Finite slots can still be occupied by idle/authorized
peers until deadlines; there are no per-principal quotas. Operators must bound
disk/history growth, provision capacity and retention policies, and monitor I/O,
unknown outcomes and overload. Per-record caps do not bound total disk use.

## Performance interpretation

[M20](milestone-20.md), [M21](milestone-21.md), [M22](milestone-22.md) and retained
[M22 provenance](../benchmarks/results/m22/README.md) remain unchanged measurement
history. FIFO reduced four-worker read queue p99 from 180.394 to 10.236 ms while
throughput changed −1.41%; independent bounded dispatch reduced deliberate
200-ms head-of-line stalls to about 2.2 ms. Small established RPC throughput
changed −3.73%, repair −6.21% with visible sync variation, and sustained mixed
maximum queue wait worsened 97.027→568.050 ms. These trade-offs remain, not a
claim of universal throughput non-regression. The added durable node intent sync
is a correctness cost, not an optimization to remove on timing evidence alone.

Representative Linux reproduction uses the existing workflow
[benchmark-linux.yml](../.github/workflows/benchmark-linux.yml), `runner=hosted`,
`profile=characterize`, three rounds, or:

```sh
python3 benchmarks/linux_run.py --executable build-production/atlasfs_bench \
  --output-dir build-production/results --runner-class shared --profile characterize --rounds 3
python3 benchmarks/summarize.py build-production/results
```

The runner rejects stale compiled source metadata; every report preserves source,
build, workload and environment. Comparisons reject incompatible configuration/
machine fingerprints unless a specifically named single experimental factor is
allowed. No cross-job speedup is claimed. Offered/executed/success/rejected/unknown
and expected injected faults are distinct. Metrics are opt-in saturated counters,
with bounded temporary accounting, logical TCP bytes (not NIC traffic), apparent
file bytes (not physical allocation), whole-process CPU/lifetime peak RSS, warm
OS cache, and coarse small-batch tails. Accounting completeness is validated.
Shared hosted loopback results are not production capacity, physical network,
exclusive hardware or physical device durability evidence.

The M23 [retained dataset](../benchmarks/results/m23/README.md) preserves all 39
reports and provenance. On its different shared-runner fingerprint (`45ffedee…`),
1/2/4-worker median read throughput is 576.525/559.987/553.372 ops/s, queue p99
0.001041/1.941204/5.721488 ms and end-to-end p99
1.957979/3.797810/7.588951 ms. This is representative current-source
characterization, not a cross-job performance comparison. Its ext4 mount reports
`journal_async_commit,nobarrier,data=writeback`; physical power-loss claims are
particularly unsupported. No speculative optimization was introduced.

## Release checklist

| Requirement | Classification | Scope/remaining responsibility |
| --- | --- | --- |
| Correctness, bounded parsing and regression audit | Verified with limitations | Full/repeated ledger passed; trusted roots/one authority |
| Published exact-source Debug/Release/static/sanitizer gates | Verified | Actual candidate CI and remote-clone ledger above; final delivery checked separately |
| TLS identity/role/target/epoch checks | Verified with limitations | Code checks tested; not Byzantine security |
| Certificate issuance, ACLs, clocks, trust and CRL freshness/session draining | Operator responsibility | No automatic credential lifecycle or ACL enforcement |
| Epoch inventory, quiescence and independent high-water checkpoint | Operator responsibility | No election, consensus or rollback-proof restored trusted state |
| Consistent backup, restore and durable preprovisioned ancestors | Operator responsibility | Offline restore drill required for the intended environment |
| Physical power-loss and production filesystem behavior | Not verified | Hosted CI/fault injection does not simulate real power loss |
| FIFO, bounded dispatch and shutdown | Verified with limitations | Serialized handlers; active action not preempted; stop/join contract |
| Representative benchmarks/schema/accounting | Verified with limitations | Current-source reproduction and raw reports retained; no capacity/non-regression claim |
| Disk/retention/per-principal abuse controls | Operator responsibility | Record bounds only; no total quota or automatic history GC |
| Production multi-host service/package/license/release artifacts | Not verified | Not delivered by this library/source candidate |
| Formal release/tag | Not verified | Proposed candidate name only; no established publishing policy |

No gate is called Verified solely because its workflow exists. An unavailable
required check is Blocked, and an unperformed deployment guarantee is Not
verified. Operator responsibilities must be fulfilled separately before use.
