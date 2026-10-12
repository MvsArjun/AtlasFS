# AtlasFS changelog

## 0.1.0-rc.1 — library/source release candidate

Release identity: annotated tag `v0.1.0-rc.1`, GitHub prerelease titled
`AtlasFS v0.1.0-rc.1`. CMake's numeric base version remains `0.1.0`.
The [private GitHub release](https://github.com/MvsArjun/AtlasFS/releases/tag/v0.1.0-rc.1)
records the exact verified commit, actual CI runs and SHA-256 asset checksums.

### Delivered capabilities

- C++20 chunked object storage with CRC-32C integrity, synchronized filesystem
  publication, deterministic placement, replication and durable membership.
- Durable coordinator operation history and explicit distributed recovery;
  versioned replica catalog with atomic compare-and-swap.
- Operator-approved local/remote repair, idempotent publication, crash
  reconciliation and recovery evidence fencing.
- TLS 1.3 mutual authentication, peer role/target authorization, optional CRLs
  and durable coordinator-epoch fencing.
- Bounded records/requests, absolute deadlines, fair FIFO admission and bounded
  connection dispatch around serialized durable handlers.
- Deterministic regression/stress tests, Linux normal/ASan/UBSan/TSan workflows,
  benchmark smoke validation and Linux I/O/performance instrumentation.

Milestone 23 hardens uncertain publication: affected stores fail closed after
post-rename durability failures; node commit/abort intent is durable before
physical action. New node journals protect the whole bounded record with CRC-32C.
Startup preserves data behind corrupt published manifests; membership versions
cannot wrap; repeated committed recovery checks fresh evidence without repeating
terminal physical actions. The full suite contains 399 tests, including 34 new
regressions and two strengthened corruption tests.

See [README](README.md), [release readiness](docs/release-readiness.md),
[architecture](docs/architecture.md), and milestones
[16](docs/milestone-16.md), [17](docs/milestone-17.md),
[18](docs/milestone-18.md), [19](docs/milestone-19.md),
[20](docs/milestone-20.md), [21](docs/milestone-21.md),
[22](docs/milestone-22.md) for contracts and historical evidence.

### Source builds and asset scope

The manually attached source ZIP contains the exact tagged tracked source,
headers, tests, benchmark tools, workflows and documentation. Historical
`benchmarks/results/` measurement files are excluded because they contain runner
paths; their README provenance summaries remain included. Full retained datasets
remain in the private Git repository and GitHub-generated full-source archives.
No compiled binaries, credentials, certificates, private keys or transient logs
are attached. There is no service installer or packaging framework.

Use a POSIX environment, C++20 compiler, CMake 3.20+, Ninja, Git, OpenSSL 3.0+
development libraries, Python 3 and Cppcheck. Ubuntu prerequisites:

```sh
sudo apt-get install g++ cmake ninja-build git libssl-dev openssl python3 cppcheck
git clone --branch v0.1.0-rc.1 https://github.com/MvsArjun/AtlasFS.git atlasfs-rc
cd atlasfs-rc
git rev-parse HEAD # compare with the full SHA in the GitHub release
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build --parallel 4
ctest --test-dir build --output-on-failure --timeout 60
cppcheck --enable=warning,performance,portability --std=c++20 --inline-suppr \
  --suppress=missingIncludeSystem --error-exitcode=1 -I include src benchmarks
cmake -S . -B build-production -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF -DATLASFS_BUILD_BENCHMARKS=ON
cmake --build build-production --parallel 4
```

Test builds fetch GoogleTest v1.15.2 on first configure (network required);
production-only builds do not. An extracted source ZIP also supports these build
commands after changing into its top-level directory, without the clone commands.
Git provenance is unavailable in an archive: use an authenticated tag checkout
for `benchmarks/linux_run.py` characterization. Direct benchmark smoke and Python
validation work from the archive. Configure again whenever the Git revision changes.
MSYS2's POSIX MSYS runtime is additionally tested; native MinGW/Windows is not a
supported build target. Final verification results are in the release record.

### Known limitations and operator responsibilities

This is not a general-purpose production storage product. One active
coordinator/catalog authority is required; there is no consensus, automatic
election, failover, replanning or background recovery. Transport listeners are
currently loopback; application handlers remain serialized and shared direct
clients/control-plane objects require external serialization.

There is no license file or unrestricted redistribution grant, packaged multi-host
service, production-capacity guarantee, or established physical power-loss
durability. Hosted loopback measurements and fault injection do not establish
deployment-specific hardware/filesystem guarantees.

Operators must provision TLS identities/trust roots and optional fresh CRLs,
protect keys/roots, maintain clocks, drain sessions during credential rotation,
quiesce writers for upgrades/backup/restore, durably preprovision ancestor links,
and preserve independent epoch high-water checkpoints outside restored snapshots.
No automatic credential lifecycle, total disk quota or Byzantine protection is
provided. Investigate uncertain/corrupt state rather than guessing success,
automatically retrying or downgrading newly written journals.
