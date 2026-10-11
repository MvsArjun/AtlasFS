# Milestone 19: durable coordinator epoch fencing

## Audit and supported authority model (specified before implementation)

Baseline: `56eccf34c158fbf05f32f6382b049bfea97a9a3b`, fetched master, clean tree,
249/249 serial tests reproduced. AtlasFS has library APIs, not a coordinator
daemon or automatic startup election. Membership/health versions, catalog CAS,
execution IDs and CRC fingerprints are evidence/idempotency mechanisms, not
coordinator authorization. A persistent TLS connection can currently submit
mutations after its coordinator has been administratively superseded.

The supported model is operator-controlled activation in one durable authority
namespace. An epoch token contains authority ID, monotonic number, authenticated
coordinator ID and activation ID. The operator provisions persistent epoch stores
explicitly, designates TLS peers that may activate epochs, stops/quiesces the old
coordinator and all catalog writers, and activates the replacement against the
complete explicitly configured participant set. A counter is NOT consensus,
leader election, transparent failover, or proof of global authority under a
partition. Multiple independent authority roots/catalog writers are unsupported.

## Ordering and rejection

The coordinator durably writes a proposed next token and participant set BEFORE
network activation. Pending activation blocks all epoch-protected coordinator
mutations, including old sessions. Each node atomically CASes its prior token to
the exact successor, durably synchronizing the file and parent directory before
acknowledging. Exact duplicates are idempotent; different identities at the same
number conflict; lower epochs are stale; skipped or unexpected predecessor tokens
are rejected. TLS identity must match the proposed coordinator and its policy
must explicitly allow activation for this target.

Only confirmation of the exact token from EVERY configured participant permits
the coordinator's durable active transition. Lost responses, inaccessible nodes,
partial activation, mismatches and uncertain persistence remain unresolved.
Explicit reconciliation queries all participants; it does not silently retry
application work or infer global success from a subset. A rejected activation
never rolls back a node's high-water mark. The operator must resolve outstanding
activation before attempting another epoch.

Every coordinator mutation RPC carries a versioned epoch envelope around the
unchanged canonical storage-v2 payload. The receiving node checks TLS role and
target policy, exact active token, and authenticated owner before dispatch.
Storage reads and operation inspection retain existing authorization rules.
Checks are per request, not per connection. A stable advisory file lock and
in-process recursive mutex serialize validation through publication/journaling
with activation. Catalog mutations and repair execution similarly hold the local
authority guard through their protected transitions. Nested guards do not
release the outer lock. These locks fence epoch transitions; they do not make
independent catalog snapshots safe concurrent writers.

## Persistence, missing records and recovery

Epoch records are versioned, bounded, strictly decoded and checksummed, with
temporary write, fsync, atomic rename and directory fsync. Startup NEVER
auto-creates or resets an epoch. Provisioning is an explicit offline operation
for a new epoch directory and refuses an existing directory. Missing lock/record,
truncation, corruption, authority mismatch or regression below the configured
durable minimum fails closed. A post-rename synchronization failure poisons that
store's handles and remains unknown until all handles are closed and the surviving
record is reopened/reconciled. No credentials or peer error contents are logged.

Deletion of a journal cannot silently enroll a node again. Preserve epoch
directories and independent operator high-water checkpoints during backups and
migration. A privileged operator deleting all state or restoring an older valid
record together with an obsolete checkpoint cannot be detected without external
trusted storage; it is outside this non-consensus model. Never reprovision a
previously enrolled deployment to resolve missing state.

Interrupted repair journals bind the original epoch together with execution ID.
Same-epoch restart/reconciliation retains the original token, canonical payload
and fingerprint. A new epoch does not adopt or replay old executions, even if
physical bytes or an exact catalog transition exist. Old-epoch recovery is
rejected without making bytes authoritative or rewriting history. After an
already committed catalog transition, authority remains committed; rejection of
old-epoch completion does not undo it. Explicit operator inspection is required.

The invariant remains publication -> verification -> catalog CAS -> completion.
Legacy unfenced constructors/formats remain for unprovisioned development roots;
once an epoch directory exists, unfenced storage/catalog opening is refused.
Deployment migration requires explicit provisioning, TLS activation permissions,
epoch-bearing clients and fenced catalog/executor sessions. Old unfinished repair
plans without epoch evidence cannot be automatically adopted by a fenced session.

## Operator procedure and API integration

1. Stop/quiesce every old coordinator, recovery writer and catalog writer; keep
   clients stopped throughout enrollment/activation. Inventory every participating
   storage node, including unavailable ones. An omitted node is not fenced.
2. Back up catalog, operation history, repair journals, epoch state and independent
   trusted minimum-token checkpoints together. Prepare the replacement's TLS
   coordinator identity and node-target allowlists. Explicitly grant
   `PeerPolicy::may_activate_epoch` only to approved activating coordinator peers.
3. Create parent directories explicitly. For a never-enrolled authority, provision
   `root/coordinator/epoch` and each `node-journal/epoch` using the SAME namespace
   and initial token. Prefer `{authority, 0, "", ""}` for first enrollment, then
   activate epoch 1 against all nodes before constructing mutation sessions.
   Existing deployed token enrollment is an offline administrative operation,
   not evidence of distributed agreement. Adjacent `epoch.enrolled` markers are
   synchronized BEFORE directory creation; failed partial enrollment remains
   fail-closed and requires operator inspection, never delete-and-retry startup.
4. Open each `epoch::Store(directory, minimum_token)` from the trusted minimum
   checkpoint. Pass the node store to the TLS `StorageNodeServer` overload.
   Plaintext and unfenced TLS constructors refuse enrolled journals. Running
   unfenced storage/catalog instances also reject work after enrollment is seen.
5. Construct activation clients using authenticated TLS coordinator configuration,
   without mutation tokens, and pass the complete participant set to
   `epoch::Coordinator`. Call `activate(coordinator_id, unique_activation_id)`.
   Activation is synchronous; lifecycle APIs and individual network clients must
   not be called concurrently. Check both status and returned EXACT token.
6. If unresolved, keep old and new application work stopped. Reopen poisoned
   stores only after closing every handle. Reconstruct the exact persisted
   participant set, then `reconcile()` to query all nodes. If some retain the
   predecessor, explicitly `resume_activation()` with the original intent. A
   different same-number identity, a later node epoch, missing record or persistent
   partition requires operator investigation; there is no rollback or automatic
   adoption. `reconcile()` with no pending intent does not invent a successful
   activation. Inspect surviving local state/checkpoints explicitly.
7. Only after accepted completion, save independent monotonic checkpoints and
   create `RemoteStorageNodeClient(id, port, tls_config, active_token)` and
   `DurableReplicaCatalog(root, coordinator_store, active_token)`. Use that catalog
   for `RepairExecutor`. Keep token-bearing clients immutable for their session;
   replacement creates fresh sessions. Same-epoch restart opens the surviving
   state, retaining original token and original repair execution evidence.

The APIs do not manage certificate issuance, root ownership, checkpoint storage,
participant inventory or writer shutdown. Protect the durable directories from
untrusted filesystem writes. Storage-role identities cannot activate or mutate;
TLS authentication alone does not grant epoch activation.

## Safety boundaries and compatibility

During partial activation, a contacted node rejects the old token while an
uncontacted node still knows only its predecessor. Pending coordinator state
rejects all protected local mutations. This does NOT prevent an independently
rooted old coordinator from talking to an uncontacted old-epoch node; stopping
old writers and using the single shared authority are required. No partition-safe
leader election, multi-coordinator safety, transparent failover or global
transaction is claimed. Cross-process epoch locks do not implement cross-process
catalog snapshot serialization. POSIX durability guarantees depend on a filesystem
that honors fsync, atomic same-directory rename and advisory locks.

Source compatibility is maintained through appended enums, optional epoch fields,
and explicit constructor overloads. Unenrolled development roots retain v2 RPCs,
legacy text records and ARPL v1–v3 support. Enrolled roots require the TLS/epoch
overloads, explicit minimum checkpoint, v3 mutation envelopes and v4 executing
repair journals. Existing unfinished unfenced operations/repairs require operator
inspection; a new epoch cannot commit an old prepared operation or relabel an old
execution. Old binaries are unsupported on enrolled roots. ACOP history, ARPC
catalog, membership/health versions and canonical RPC fingerprints are unchanged.

## Deterministic verification coverage

New regressions cover strict bounded codecs; stale/conflicting/skipped/duplicate
activation; counter exhaustion; restart; missing, truncated, corrupt and foreign
records; minimum-checkpoint regression; replaced locks and rolled-back pending
intent; every persistence fault; post-rename shared poison; lost responses;
participant omission, disagreement and partitions; explicit reconciliation and
resume; real two-node mutual-TLS activation; per-request rejection on persistent
old sockets for all five mutation RPCs; role/owner/activation policy; filesystem
publication versus concurrent activation; stale catalog upsert/erase/CAS and an
obsolete separate process; pending catalog gating; migration bypass; four-node
repair/historical-operation isolation; and same/new-epoch recovery at three crash
windows (before publication, after publication, after catalog CAS).

The baseline passed 249/249 tests (exit 0, 16.31 s). The final local MSYS2 suite
passed 296/296 (47 additions; exit 0, 21.18 s), using GCC 15.3 and OpenSSL 3.6.5.
A new production-only Release build passed, and Cppcheck 2.21 checked all 22
implementation files with no warning/performance/portability findings (exit 0).
Actual remote fresh-clone and Linux build/CI results are recorded in the delivery
report; workflow configuration alone is not evidence of passing sanitizers.
CI runs serial CTest in normal, ASan/UBSan and TSan Linux jobs,
plus normal production build, Cppcheck and ten repetitions of authenticated repair
and epoch activation/recovery integration tests.
