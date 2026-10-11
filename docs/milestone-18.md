# Milestone 18: authenticated transport and peer authorization

## Threat model and invariant

Untrusted network peers can observe or modify plaintext, impersonate a node,
submit mutations, replay messages, or occupy the sequential server. A socket and
a claimed node ID establish no trust. OpenSSL mutual TLS must validate chains,
validity periods, and configured revocation information before framing or storage
dispatch. Authorization additionally requires the configured role and target.
Authentication/authorization failure must not mutate storage, repair journals,
historical records, or catalog authority. Transport loss after sending a mutation
remains unknown. Existing execution IDs and fingerprints remain unchanged.

## Selected design

TLS 1.3 wraps existing ATFS v1/storage v2 frames without negotiation or downgrade.
A byte-stream abstraction supports explicit plaintext and TLS endpoints. Secure
configuration supplies a per-peer certificate/key, explicit CA file, optional
CRL file, local role/ID, peer allowlist, and operation deadlines. Certificate SAN
URI identity is exactly `urn:atlasfs:coordinator:<id>` or
`urn:atlasfs:storage:<id>`; CN and payload claims are ignored. The allowlist binds
that exact URI to a role/ID and a set of permitted storage targets. Clients require
the certificate identity of the intended storage node. Servers require client
certificates, then permit coordinator storage operations only for an authorized
target; storage-role peers may read chunks only for permitted targets. Generic
object requests are denied on the authenticated storage-node endpoint.

Nonblocking connect, handshake, read, and write use absolute monotonic deadlines.
Each frame has an overall deadline, preventing indefinite partial-frame trickle.
TLS sessions/early data are disabled so reconnect authenticates credentials anew.
Configuration is immutable for an endpoint; rotation requires replacing/restarting
the endpoint and closing old connections. Optional CRL checking requires current
CRLs for the complete chain and fails closed if missing or expired. Removing a
peer from the allowlist also requires endpoint restart; there is no live reload.

## Replay and limits

TLS protects records in a connection; durable journals retain application duplicate
semantics across reconnect. This milestone does not add coordinator epochs or
fence a stale request issued by an otherwise currently authorized coordinator.
Authorization is by role and storage target, not object ACL. Compromised keys,
trust roots, or an authorized coordinator remain trusted until configuration and
credentials are replaced. CRC fingerprints retain their accidental-integrity role
and are not cryptographic authenticators. One coordinator and sequential loopback
servers remain the deployment model; TLS does not introduce distributed leases.

## Verification strategy

Generate temporary CA/key/certificate/CRL fixtures at test runtime with
OpenSSL; no private key or production credentials are committed. Cover mutual
TLS, missing/untrusted/expired/revoked certificates, identity mismatch, role and
target denial, plaintext downgrade, malformed frames, handshake/frame timeout,
rotation, repair response loss, duplicate replay after restart, and catalog
conflict after authenticated publication. Retain all prior tests, run serial
CTest, Cppcheck, Linux ASan/UBSan and TSan, and fresh remote clone verification.

## Dependency and configuration

The inspected baseline is `f8284be59be4ff42a0c6f34c3d7d926d5392d382`, matching
fetched `origin/master`, with a clean tree and 214/214 serial tests reproduced.
There is no separate Milestone 15 document in that baseline; its remote-repair
design is recorded in README, architecture/protocol documentation, and the
Milestone 16 gap analysis. Network entrypoints are the generic TCP client/server
and their remote-storage wrappers; heartbeat is an injected in-process interface,
not another concrete socket implementation.

OpenSSL 3.0+ development libraries are mandatory, located through CMake's
`find_package(OpenSSL 3.0 REQUIRED)` and linked as `OpenSSL::SSL`/`OpenSSL::Crypto`.
Use a vendor-supported, security-patched release; the minimum API version is not
a promise that every 3.0 release is currently supported. Linux CI installs
`libssl-dev` and `openssl` and reports the actual version. MSYS2 MSYS requires
`openssl-devel` (not the native MinGW package).

Configure each coordinator and storage endpoint with its own leaf certificate and
protected unencrypted PEM private key. Chain files contain the leaf first and any
intermediates; `trust_file` contains only explicitly selected trust anchors. Keys
with interactive passwords are rejected rather than prompting. Protect key files
and configuration with owner-only filesystem permissions; AtlasFS does not manage
secret storage or require a specific filesystem ACL implementation.

Example server and coordinator client (paths are deployment-specific):

```cpp
net::TlsConfig server;
server.certificate_file = "/etc/atlasfs/d-chain.pem";
server.private_key_file = "/etc/atlasfs/d-key.pem";
server.trust_file = "/etc/atlasfs/ca.pem";
server.crl_file = "/etc/atlasfs/crls.pem"; // optional; full-chain CRLs required
server.local_identity = {"d", net::PeerRole::storage};
net::PeerIdentity coordinator{"primary", net::PeerRole::coordinator};
server.peers.emplace(net::certificate_identity(coordinator),
                     net::PeerPolicy{coordinator, {"d"}});
server.timeout = std::chrono::seconds(5);
replication::StorageNodeServer endpoint(node_d, journal_root, server);
// Start endpoint.serve() before constructing a synchronous TLS client.
net::TlsConfig client;
client.certificate_file = "/etc/atlasfs/primary-chain.pem";
client.private_key_file = "/etc/atlasfs/primary-key.pem";
client.trust_file = "/etc/atlasfs/ca.pem";
client.crl_file = "/etc/atlasfs/crls.pem";
client.local_identity = coordinator;
net::PeerIdentity storage_d{"d", net::PeerRole::storage};
client.peers.emplace(net::certificate_identity(storage_d),
                     net::PeerPolicy{storage_d, {"d"}});
replication::RemoteStorageNodeClient remote("d", endpoint.port(), client);
```

IDs accept ASCII letters/digits/`.`/`_`/`-`, at most 255 bytes, excluding `.` and
`..`. Timeout is positive and at most five minutes; default is five seconds.
Peer policies and target sets are capped at 4096 entries. A client policy's target
set does not confer server permissions: the receiving server's policy controls
authorization. An empty target set permits authentication but no storage requests.

## Issuance and trust administration

Use an offline or tightly controlled CA service, issue one certificate per role/ID,
and prohibit arbitrary SAN issuance. Leaf certificates require `CA:FALSE`, digital
signature key usage, and the appropriate TLS purpose (`clientAuth` for a
coordinator, `serverAuth` for a storage service; both for bidirectional storage
clients). The single canonical URI SAN is mandatory; hostname validation is not
used because expected AtlasFS role/ID binding is the service-identity mechanism.
Do not reuse production roots or keys in tests. For a test-only CSR:

```sh
umask 077
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -nodes -keyout d-key.pem -out d.csr -subj /CN=d \
  -addext 'subjectAltName=URI:urn:atlasfs:storage:d'
```

The CA must inspect the requested role/ID and issue with approved SAN, key usage,
validity, and purpose extensions; blindly copying CSR extensions can grant an
attacker a coordinator identity. Distribution of roots and allowlists is an
out-of-band administrative responsibility. A compromised root can issue a
certificate for any allowlisted identity; TLS cannot remedy a compromised trust
administrator. No automatic root download, TOFU, online OCSP, or default system
trust-store fallback exists.

If revocation is required, configure current PEM CRLs from every issuer in the
chain. OpenSSL's full-chain CRL check rejects revoked certificates, expired CRLs,
missing issuer CRLs, or invalid signatures. Without `crl_file`, expiration and
allowlist checks still apply but individual certificate revocation is not checked.

## Rotation and migration

Issue replacement certificates before expiry. Prepare new root/CRL bundles and
peer policies, quiesce application work, close clients, stop and join the server,
and construct replacement endpoints. To rotate roots across multiple nodes,
temporarily distribute an approved old+new root bundle, migrate certificates,
then remove the old root and restart again. Reconcile any ambiguous operations
explicitly with their original durable identity after reconnect. No connection
is silently retried or assumed successful during rotation.

Existing streams retain the policy and credentials loaded at construction;
changing a PEM file does not revoke an established session. Close all old streams
to enforce revocation immediately. Session tickets, session caching, and TLS
early data are disabled. There is no hot reload or background renewal.

Legacy constructors intentionally remain plaintext for existing local tests and
development callers. Production migration must update **both** remote client and
storage-server constructors to supply `TlsConfig`, remove plaintext listeners,
and audit generic TLS handlers for policy enforcement. No process-wide secure
default or runtime daemon/config-file loader is introduced. Current listeners
remain sequential and loopback-only. Handshake deadlines limit stalled peers,
but do not provide rate limiting or protection against aggregate connection DoS.

## Failure and authority

Configuration failure throws before serving. Handshake/identity failure closes
the connection with no storage dispatch; application logs receive only generic
errors, never private keys or certificate contents. Valid-but-disallowed storage
requests return `error_response` without journal/data changes. A client failure
closes its stream; a new endpoint/client is required for explicit replay. Repair
clients retain conservative unknown outcomes for errors after an attempted send.
Physical publication, verification, catalog CAS, and completion remain separate.
Authentication does not grant catalog-writing authority or bypass stale evidence.

TLS deadlines bound networking, not an application handler's filesystem calls.
Local disk stalls, authorized requests that replay stale application actions,
compromised trusted hosts/keys, and single-coordinator availability are residual
risks. TLS has no object-level access control or cryptographic strengthening of
existing durable CRC fingerprints. Existing loopback plaintext object-storage
handlers are outside the authenticated storage-node endpoint.

## Local verification

The MSYS2 GCC 15.3.0/OpenSSL 3.6.5 build discovers and executes 249 tests:
all 214 prior tests plus 35 TLS/authorization/repair regressions. Serial CTest
passes 249/249. The seven authenticated repair integration tests pass ten
consecutive repetitions each (70 executions). Production-only Release compilation
with `BUILD_TESTING=OFF` passes. Cppcheck 2.21 passes with warning, performance,
and portability checks and an error exit code for findings; `git diff --check`
is clean. No certificate or private-key fixture is tracked.

The published-commit report must separately confirm fresh-remote-clone tests and
the actual Linux normal/ASan+UBSan/TSan run; the workflow's existence or local
MSYS test success does not establish Linux sanitizer success.
