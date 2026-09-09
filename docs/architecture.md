# AtlasFS architecture

## Current milestone

AtlasFS currently contains a dependency-free protocol library and a Linux/POSIX
TCP transport library, plus a persistent `ObjectStore`. `TcpClient` encodes
requests, sends complete frames, and uses `StreamParser` to receive responses.
`TcpServer` listens only on loopback,
accepts connections sequentially, parses arbitrary receive chunks, invokes a
request handler, and reliably sends encoded responses. `StorageRequestHandler`
decodes operation payloads and delegates filesystem work to `ObjectStore`. There
is no replication or coordinator yet.

The parser consumes arbitrary byte fragments because TCP preserves byte order but
not application message boundaries. It may emit zero, one, or many messages per
call. Incomplete frames remain buffered. A malformed header is a connection-level
protocol error: the parser enters a failed state until reset, preventing unsafe
attempts to resynchronize within an untrusted byte stream.

## Decisions

### Explicit binary framing

AtlasFS uses a fixed-width, big-endian header followed by an opaque payload. This
is compact, deterministic, and exercises the actual stream-framing problem. HTTP
was rejected as the internal protocol because it adds semantics AtlasFS does not
need and obscures this foundational systems concern. A schema framework such as
Protocol Buffers remains an option for future payloads, but is unnecessary for
the envelope.

### Bounded payloads

The parser rejects declared payloads larger than a configurable limit before
waiting for their bytes. This bounds memory exposure from malformed or hostile
peers. The initial default is 16 MiB; large objects will later be split into
bounded chunks rather than increasing frame sizes without limit.

### Single-threaded parser ownership

`StreamParser` has no internal locking. One connection handler owns one parser.
This keeps synchronization outside a low-level value parser and makes ownership
clear.

### Blocking sequential transport

The server uses blocking POSIX sockets and handles one connection at a time. This
is the smallest execution model that proves real TCP framing and lifecycle
correctness. `recv()` output is passed directly to `StreamParser`; the transport
does not duplicate framing logic. `send_all()` retries interrupted calls and
continues after short writes. A move-only `Socket` closes its descriptor exactly
once.

The listening socket binds to `127.0.0.1`; port zero is supported and the chosen
port is obtained with `getsockname()`. `stop()` shuts down the listener and any
active connection so a test or owner can deterministically unblock the server.
Malformed, oversized, and incomplete frames close only their connection. The
server continues accepting later connections.

The default transport handler remains storage-free. For request message types it
returns a `response` with the same request ID, flags, and payload. Receiving a
response-type message where a request is expected produces an `error_response`.
Storage servers instead install `StorageRequestHandler`.

### Persistent object store

The configured root contains an `objects/` directory and one file per object ID.
Identifiers use a strict ASCII allowlist and cannot contain path separators,
absolute-path syntax, traversal components, or the internal temporary-file prefix.
GET opens files with `O_NOFOLLOW` and accepts only regular files.

Each file is:

```text
4 bytes  magic "AOBJ"
2 bytes  format version (1)
2 bytes  reserved zero
8 bytes  object length, big endian
4 bytes  CRC-32C, big endian
N bytes  object data
```

PUT first verifies the client checksum. It then creates a private, exclusive
temporary file in `objects/`, writes the complete header and data, calls
`fdatasync()`, closes the file, atomically renames it over the destination, and
calls `fsync()` on the directory. The temporary path is removed on pre-rename
failure. Thus readers never observe a partially written final object. A directory
sync failure can be reported after the rename has become visible because the
system cannot safely roll that operation back.

GET validates file type, magic, version, reserved bits, declared length, exact
file length, configured bounds, and CRC-32C before returning bytes. DELETE uses
`unlink()` and directory sync. DELETE is idempotent with respect to final state,
while its result distinguishes the first successful removal from later
`not_found` attempts.

## Intended evolution

The next dependency is large-object chunking with per-chunk integrity and partial
transfer handling. Production concurrency, distributed placement, and replication
remain deferred.
