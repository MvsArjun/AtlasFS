# Milestone 16: distributed repair reconciliation

## Gap analysis

Milestone 15 made remote repair publication durable and idempotent, but a
coordinator could still stop after persisting `EXECUTING`, after publishing and
verifying target bytes, or after committing the replica-catalog CAS but before
persisting `COMPLETED`. Restart loaded the evidence but offered no safe way to
resolve those states. Retrying as a new repair could duplicate work or overwrite
new control-plane decisions; declaring success from target bytes alone could make
non-authoritative data authoritative.

The recovery boundary is therefore the existing ARPL plan, target repair journal,
and ARPC catalog snapshot. Historical ACOP operation records, membership state,
placement policy, and storage paths remain outside the mutation boundary.

## Behavior and invariant

Recovery is explicit and operator-invoked through `RepairExecutor::reconcile`;
startup remains read-only. It accepts durable `EXECUTING` plans and FAILED plans
with only `remote_result_unknown`, `catalog_durability_unknown`, or
`metadata_commit_failure`. Other states return `not_reconcilable`.

The invariant is: reconciliation may complete only the exact previously approved
repair against the exact evidence it captured. It reuses the durable execution
ID, source, and target. Changed membership, health, placement, or any catalog
state other than the original evidence or the plan's exact single next-version
transition becomes `STALE`. It never silently replans or removes a physical
replica.

For an unchanged catalog, reconciliation verifies source and target data, replays
an unknown remote publication using the same identity, persists verification,
and performs the original catalog CAS. For the exact next catalog state, it
recognizes that the pre-crash CAS committed, updates the caller's placement view,
and persists `COMPLETED` without another transfer.

## Verification and compatibility

Deterministic fault injection covers interruption after `EXECUTING`, after target
publication evidence, and after the catalog commit. Tests also cover restart,
lost response with durable duplicate replay, concurrent catalog mutation, changed
membership, and rejection of non-ambiguous states. Existing end-to-end repair,
catalog-conflict, durability-unknown, and historical-operation immutability tests
remain active.

ARPL stays at version 3 and the storage-node protocol stays at version 2. No wire
or durable layout migration is required: the implementation adds executor policy
and two in-memory result codes, while retaining compatibility with all existing
valid records. Legacy ARPL v1/v2 records remain readable and remain ineligible for
execution because they lack catalog evidence.

Acceptance requires a warning-clean-enough C++20 build under the documented
warning flags, a complete serial CTest pass, a fresh-checkout build/test pass,
and factual reporting of unavailable sanitizer or static-analysis coverage.
