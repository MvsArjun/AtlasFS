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

The fixed header is 24 bytes. Payloads are opaque at this milestone. Their schemas
will be specified alongside the operations that need them, rather than guessed in
advance.

## Message types

| Value | Name |
|---:|---|
| 1 | PUT request |
| 2 | GET request |
| 3 | DELETE request |
| 4 | metadata request |
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

## Compatibility

Changing the fixed header or existing field meaning requires a new protocol
version. New message types may be added within version 1 when old peers can reject
them cleanly. Reserved flag bits must be zero until their semantics are specified.
