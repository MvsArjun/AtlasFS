# AtlasFS architecture

## Current milestone

AtlasFS currently contains one dependency-free library, `AtlasFS::protocol`. It
defines the message envelope shared by future clients, the metadata coordinator,
and storage nodes. No network listener, persistence, replication, or coordinator
is present yet.

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
clear. Concurrency belongs in the future server layer.

## Intended evolution

The next layer is a single-node Linux TCP server and client using POSIX sockets,
reliable short-read/short-write loops, filesystem-backed object storage, and real
integrity hashes. Distributed placement and replication follow only after that
single-node path is correct and tested.
