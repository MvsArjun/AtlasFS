# AtlasFS architecture

## Current milestone

AtlasFS currently contains a dependency-free protocol library and a Linux/POSIX
TCP transport library. `TcpClient` encodes requests, sends complete frames, and
uses `StreamParser` to receive responses. `TcpServer` listens only on loopback,
accepts connections sequentially, parses arbitrary receive chunks, invokes a
request handler, and reliably sends encoded responses. There is no persistence,
replication, or coordinator yet.

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

The default handler is deliberately storage-free. For request message types it
returns a `response` with the same request ID, flags, and payload. Receiving a
response-type message where a request is expected produces an `error_response`.

## Intended evolution

The next layer is single-node filesystem-backed PUT, GET, and DELETE behavior with
real integrity hashes, while retaining the current transport/handler boundary.
Production concurrency, distributed placement, and replication remain deferred.
