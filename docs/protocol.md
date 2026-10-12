# AtlasFS wire protocol v1

Every integer is unsigned and encoded in network byte order (big endian). Each
connection carries a sequence of frames and may remain open for multiple requests.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `ATFS` (`0x41544653`) |
| 4 | 2 | version | Protocol version, currently `1` |
| 6 | 2 | message type | Operation or response discriminator |
| 8 | 4 | flags | Reserved operation flags; currently operation-defined |
| 12 | 8 | request ID | Client-selected response-correlation identifier |
| 20 | 4 | payload length | Number of payload bytes following the header |

The fixed header is 24 bytes. Operation payload schemas are defined below; the
framing layer continues to treat their bytes as opaque.

## Message types

| Value | Name |
|---:|---|
| 1 | PUT request |
| 2 | GET request |
| 3 | DELETE request |
| 4 | metadata request |
| 10 | PUT response |
| 11 | GET response |
| 12 | DELETE response |
| 100 | successful response |
| 101 | error response |

## Parsing and connection behavior

- Receivers must accept fragmented headers and payloads.
- Receivers must process multiple frames delivered in one read.
- A declaration above the configured payload limit is rejected immediately.
- Invalid magic, unsupported version, or unknown message type is fatal for the
  stream. The connection handler should send an error when safe, then close.
- EOF with buffered bytes is a truncated frame. The connection handler detects
  this through `has_partial_message()` and treats the request as incomplete.
- Request IDs are correlation values, not deduplication guarantees. Retry and
  idempotency semantics will be defined with storage operations.

## Current transport behavior

`TcpServer` is handler-driven. Its default echo handler accepts request types 1
through 4 and returns type 100, which remains useful for transport tests. A storage
server installs `StorageRequestHandler`, which implements types 1 through 3 and
returns their corresponding operation-specific response type. Malformed framing,
oversized frames, or truncated input closes the connection without stream
resynchronization.

## Object operation payloads

All lengths and checksums use big-endian byte order. Object IDs are byte strings
that must satisfy the storage layer's documented ASCII allowlist.

PUT request:

```text
uint16  object ID length
uint32  object data length
bytes   object ID
bytes   object data
uint32  CRC-32C of object data
```

GET and DELETE request:

```text
uint16  object ID length
bytes   object ID
```

PUT and DELETE response payloads contain one `uint16` status. GET failures use
the same two-byte representation. A successful GET response is:

```text
uint16  status = 0
uint32  object data length
bytes   object data
uint32  CRC-32C of object data
```

Decoders require exact payload lengths and reject truncation, trailing bytes,
overlong IDs, and object lengths above `16 MiB - 512 bytes`. The margin ensures
the complete operation payload remains below the framing parser's 16 MiB limit.

Status values are: `0 success`, `1 not_found`, `2 invalid_request`,
`3 invalid_object_id`, `4 checksum_mismatch`, `5 corrupt_object`,
`6 filesystem_error`, `7 payload_too_large`, and `8 malformed_request`.

CRC-32C uses the Castagnoli polynomial and is stored as four big-endian bytes. It
detects accidental corruption but is not an authentication mechanism and no
cryptographic security is claimed.

Filesystem chunking does not change this wire protocol. PUT and GET continue to
represent one bounded logical object; manifests, generation IDs, chunk indices,
and per-chunk checksums are internal storage metadata. A future streaming protocol
will be required before objects can exceed the current single-frame bound.

## Compatibility

Changing the fixed header or existing field meaning requires a new protocol
version. New message types may be added within version 1 when old peers can reject
them cleanly. Reserved flag bits must be zero until their semantics are specified.

## Storage-node protocol

Replica traffic uses the same frame with distinct message types 200–213. Each
payload begins with a two-byte big-endian storage protocol version. Milestone 8
uses version 2. Unknown versions, malformed lengths, unsafe identifiers, and
out-of-bound chunk counts are rejected.

PREPARE carries a bounded operation ID, object ID, protected manifest, chunk
indexes, and object bytes. The server fingerprints the canonical request. An
exact repeated PREPARE succeeds while PREPARED; different contents under the same
ID return `operation_conflict`. COMMIT and ABORT carry operation, object, and
generation IDs. Repeated COMMIT after COMMITTED and ABORT after ABORTED succeed;
cross-terminal transitions return `invalid_operation_state`.

GET_CHUNK carries object ID, generation ID, and chunk index and returns manifest,
chunk metadata, and bytes. DELETE is idempotent. QUERY_OPERATION reports UNKNOWN,
PREPARED, COMMITTED, or ABORTED. Frame request IDs correlate responses and
mutating responses echo the operation ID.

There are no automatic retries. Loss of a COMMIT response is `result_unknown`
because the server may already have committed. The caller must not blindly abort;
QUERY_OPERATION on a new connection can resolve the durable state.

QUERY_OPERATION responses include the durable PREPARE fingerprint in addition to
the echoed operation ID and state. Recovery clients must compare both values with
the expected operation identity. Absent operations have no stored fingerprint;
an unresolved operation with durable intent retains its original fingerprint.
This version-2 field is required by Milestone 8 recovery clients. Version-1
Milestone 7 payloads are rejected rather than being misinterpreted.

### Node-journal publication (Milestone 23)

This is a persistence change, not a new wire version. New operation and repair
records use `RECORD1\n`, the existing single payload line (including its newline),
then `CRC32C <decimal checksum>\n`. CRC-32C covers the complete payload, including
state, identity, fingerprint and optional epoch. Readers bound records to 4 KiB,
require regular non-symlink files, and reject unknown wrapper versions, malformed
footers, corruption and trailing fields. Legacy unwrapped text remains readable
but lacks whole-record protection. Older binaries are unsupported on upgraded
journals; some old unfenced readers skip unsupported records, so downgrade is
not a safe recovery mechanism.

COMMIT/ABORT persist UNKNOWN intent before physical action. Intent publication
failure causes no physical mutation. If physical success is known but terminal
journal publication fails, QUERY and the original response remain unknown;
explicit matching replay in the same process may publish only the terminal
journal, not repeat the mutation. Opposite transitions are rejected. A surviving
UNKNOWN record after restart remains unknown and explicit recovery refuses both
completion and rollback. A surviving valid terminal candidate can resolve an
after-rename journal ambiguity on reopening; this is surviving-filesystem
evidence, not proof of physical power-loss durability. No automatic retry occurs.

### Remote repair publication

`REPAIR_CHUNK` uses request type 212 and response type 213. The request contains
the storage protocol version, a zero reserved field, bounded repair-plan and
execution IDs, the intended target node ID, object ID, complete encoded manifest,
explicit chunk index/length/checksum, and bounded chunk bytes. It never contains
a destination path. The node validates every identifier and its own target
identity before allowing `ObjectStore` to derive a storage location.

The target validates manifest/chunk consistency and the transferred checksum,
then uses synchronized temporary files, reread verification, atomic rename, and
directory synchronization. A valid existing identical replica returns
`already_present_valid`; conflicting, corrupt, or unrelated existing content is
not overwritten.

Responses echo the plan and execution IDs plus the CRC-32C fingerprint of the
complete canonical request. Result values distinguish published,
already-present-valid, identity mismatch, target rejection, invalid manifest,
checksum mismatch, publication failure, and result unknown. The node journals
the fingerprint and result by validated execution ID, so exact duplicates are
idempotent across restart and conflicting reuse is rejected.

TCP send and receive waits are bounded to five seconds by default. Once a
mutating request may have been sent, a timeout, disconnect, malformed response,
or lost response is conservatively reported as an unknown result. The client
does not retry automatically. An explicit coordinator reconciliation may replay
the byte-equivalent request with the same plan and execution IDs; the durable
target journal then returns the recorded result or rejects a conflicting reuse.
This recovery behavior adds no wire fields or message types. A duplicate whose
generation directory disappeared is rejected despite its successful journal
entry. A journaled unknown result remains unknown on replay. All interrupted
pre-CAS executions pass through this boundary, even when GET_CHUNK returns
identical physical bytes. A successful
physical response still does not make the replica authoritative; only the
subsequent coordinator catalog CAS does.

### Authenticated transport envelope (Milestone 18)

Explicit TLS endpoints exchange TLS 1.3 records from the first byte, with mutual
certificate authentication and no plaintext negotiation or downgrade. Within the
authenticated stream, ATFS framing and storage protocol version 2 are unchanged.
Plaintext peers and TLS versions below 1.3 are rejected. No new application fields
or security claims in payloads are trusted as authentication.

Certificates must contain exactly one URI SAN, either
`urn:atlasfs:coordinator:<id>` or `urn:atlasfs:storage:<id>`. Chain validation,
validity periods, TLS certificate purposes, and configured CRLs are checked by
OpenSSL. CN and DNS SANs do not establish AtlasFS identity. URI-to-role/ID mappings
must be canonical, explicitly allowlisted, and bound to the expected storage ID
on clients. A TLS storage endpoint must match its underlying storage-node ID.

Before storage request decoding, the server requires the authenticated peer's
target allowlist to contain its local node ID. Coordinators may PREPARE, COMMIT,
ABORT, GET_CHUNK, DELETE, QUERY_OPERATION, and REPAIR_CHUNK. Storage-role peers may
only GET_CHUNK. Other types, including generic object operations and response
frames used as requests, are denied. REPAIR_CHUNK's claimed target must also match
the server; successful authentication cannot override that check.

Policy denial returns a bounded empty `error_response` with the original request
ID and does not touch application journals or data. Certificate or identity
failure closes the transport before dispatch. Existing repair clients conservatively
classify an unexpected response as `result_unknown`, never as publication success.
Deadlines bound connect, handshake, complete-frame reads, and writes. Certificate
chains are limited to depth eight and handshake certificate lists to 64 KiB;
application payloads retain the existing bounded parser limits. No automatic
retry occurs. Reconnect authenticates current credentials, and duplicate replay
uses the original execution ID and fingerprint, not a new operation identity.

Legacy plaintext endpoints remain explicitly available for local development and
are not secure deployments. See [Milestone 18](milestone-18.md) for migration,
configuration, revocation, and residual application-level replay risks.

### Epoch envelope and activation (Milestone 19)

ATFS framing and TLS 1.3 are unchanged. Enrolled secure storage endpoints require
the following version-3 payload envelope for PREPARE, COMMIT, ABORT, DELETE and
REPAIR_CHUNK; their existing message type numbers are unchanged. All integers are
unsigned big-endian, strings are length-prefixed ASCII, and no trailing bytes or
nonzero reserved fields are accepted:

```text
u16 version=3 | u16 reserved=0 | u16 token_bytes | token
u32 canonical_payload_bytes | unchanged storage-v2 canonical payload
token := u16 authority_bytes | authority | u64 epoch
         u16 coordinator_bytes | coordinator | u16 activation_bytes | activation
```

Authority, coordinator and activation IDs each contain 1–96 ASCII letters,
digits, `_` or `-`; the encoded token is at most 302 bytes. Epoch zero is allowed
only as a provisioning/activation predecessor with empty owner and activation;
it never authorizes a mutation. Request frames remain bounded to 16 MiB, and
outbound canonical payloads reserve 320 bytes for envelope overhead. Fingerprints
are still CRC-32C of the original canonical v2 payload, not the envelope.
GET_CHUNK and QUERY_OPERATION retain v2 payloads and existing role authorization.

| Type | Number | Payload after version/reserved prefix |
| --- | ---: | --- |
| epoch_activate_request | 214 | length-prefixed expected token, desired token |
| epoch_activate_response | 215 | status and optional token |
| epoch_query_request | 216 | empty |
| epoch_query_response | 217 | status and optional token |
| epoch_error_response | 218 | status and optional token |

Each token above has a u16 byte length. Results contain a u16 status followed by
the u16 token length (zero means absent). Status values 0–5 are accepted, stale,
conflict, unauthorized, unresolved and invalid. Accepted results always include
a token. No response implies global authority: activation must confirm the
exact desired token at every configured participant and durably commit local
coordinator intent. Wrong, malformed, lost or uncertain responses remain unknown;
clients never automatically retry application work.

Activation requires an authenticated coordinator, target allowlist membership,
explicit `may_activate_epoch=true`, and a desired owner matching the TLS identity.
Nodes durably CAS the exact predecessor to its immediate successor, acknowledge
only after synchronization, and return idempotent success only for the exact
token. Query requires coordinator role and target authorization but not activation
permission. Mutation authorization matches the full active token and TLS owner
per request; failures produce a bounded epoch_error_response before dispatch.
Local mutation APIs expose stale_epoch/epoch_unresolved; catalog APIs reject
epoch-unavailable mutations without advancing versions.

Epoch state uses `AEPC` binary version 1: magic u32, version u16, reserved u16,
current token, pending flag u16, optional proposed token, participant count u32,
sorted u16-length-prefixed participant IDs, then CRC-32C u32 over preceding bytes.
Records are bounded to 1 MiB and 4096 participants. The synchronized enrollment
marker contains an initial AEPC record; the stable lock file is never replaced.
Storage operation/repair records append `EPOCH3 authority number coordinator
activation token_crc32c` to their existing text fields, retaining canonical request
fingerprints and original execution identity. Fenced malformed records fail startup.
Legacy records remain readable but cannot be mutated/replayed as a new epoch's
operations. ARPL v4 extends v3 with u16 token length and token before its existing
record checksum. ARPL v1–v3 stay readable; older binaries must not open enrolled
roots or v4 journals. There is no negotiation or downgrade to unfenced mutations.
