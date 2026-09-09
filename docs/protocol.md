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

## Compatibility

Changing the fixed header or existing field meaning requires a new protocol
version. New message types may be added within version 1 when old peers can reject
them cleanly. Reserved flag bits must be zero until their semantics are specified.
