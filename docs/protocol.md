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
the expected operation identity. UNKNOWN operations have no stored fingerprint.
This version-2 field is required by Milestone 8 recovery clients. Version-1
Milestone 7 payloads are rejected rather than being misinterpreted.

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
This recovery behavior adds no wire fields or message types. A successful
physical response still does not make the replica authoritative; only the
subsequent coordinator catalog CAS does.

The current trust boundary is the existing coordinator-to-storage-node channel
and validated node identity. AtlasFS does not yet provide cryptographic peer
authentication or encrypted transport.
