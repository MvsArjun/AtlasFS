# Milestone 17: recovery evidence fencing

## Verified failure and scope

The Milestone 16 audit identified a durability defect more immediate than adding
TLS. FAILED/REMOTE_RESULT_UNKNOWN was persisted as EXECUTING with its error
cleared before replay. If another crash occurred there, the next reconciliation
could skip the publisher when bytes were present, bypass a target journal's
unknown outcome, and commit catalog authority. TLS would not fix this defect.

The post-CAS completion path also accepted a visible catalog snapshot without
checking the catalog's poisoned durability state, target availability/integrity,
or the plan's durable verification flags. A coincidentally matching catalog
mutation could not prove this plan had completed publication.

## Invariant and design

Physical presence does not establish confirmed publication. Every interrupted
pre-CAS execution now replays the exact publisher identity, including after
repeated crashes. Unknown target journal results cannot be promoted to success.
A poisoned catalog rejects recovery without changing the plan. After restart,
the exact next catalog transition may finish the plan only with durable step
evidence and a fresh intact target read. A missing or corrupt target remains
pending without recopying, undoing authority, or substituting another node.
Catalog comparison uses one snapshot and rechecks it after the target read.

The wire schemas and ARPL/ARPC layouts are unchanged; no migration is needed.
Historical operation records remain separate. One active coordinator is still
required; the final catalog check is not a cross-process transaction or lease.

## Tests and acceptance

Regression coverage exercises a second coordinator crash during unknown replay,
same-process poisoned-catalog recovery followed by restart, missing/corrupt and
unavailable post-CAS targets, matching catalog state without step evidence, and a
successful target journal with a missing generation after restart. Existing
duplicate-ID, mismatched-fingerprint, malformed journal, membership change,
catalog conflict, response-loss, four-node, and historical-isolation tests remain.
Acceptance requires the full serial suite, static analysis, and fresh checkout
verification. Sanitizer status is recorded separately from ordinary test success.

## Security investigation and next dependency

The current loopback transport authenticates neither peer and exposes plaintext
framed traffic. Claimed node identity and CRC fingerprints provide neither peer
authentication nor cryptographic integrity. A malicious trusted-network peer can
forge operations; replay journal retention does not fence obsolete requests from
a previously authorized coordinator.

OpenSSL is the preferred candidate for a subsequent transport milestone: mutual
certificate verification, trust roots, certificate identity bound to node/role,
operation authorization, explicit secure endpoints without fallback, bounded
handshake/read/write deadlines, and a rotation/revocation policy are required.
The existing frames can run unchanged inside TLS. TLS alone would not fence stale
application execution IDs or authorize operation roles. Peer verification must
fail closed; servers must also require client certificates. See the official
[OpenSSL verification documentation](https://docs.openssl.org/3.3/man3/SSL_CTX_set_verify/).
No TLS dependency or security placeholder is introduced in this durability milestone.

## Independent verification environment

The new GitHub Actions workflow defines Ubuntu 24.04/GCC 13 normal, ASan/UBSan,
and TSan jobs. Local MSYS GCC lacks all three sanitizer runtimes, WSL is not
installed, and no Docker/Podman command is available. CI runs require an
authenticated remote push and GitHub Actions availability. Workflow creation does
not establish a sanitizer pass. Parallel baseline CTest with four workers passed
210/210 without reproducing the previously reported process-launch timeout;
its cause remains unconfirmed.
