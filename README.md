# AtlasFS

AtlasFS is a C++20/Linux object-storage systems project. The current milestone
provides a documented binary wire format, incremental stream parser, blocking
POSIX TCP transport, and persistent single-node PUT, GET, and DELETE operations.
Objects survive server restarts and are verified with CRC-32C before being
returned. Storage splits every logical object into configurable chunks (1 MiB by
default), protects each chunk and the whole object, and commits through a durable
binary manifest.

## Build and test

Prerequisites: CMake 3.20+, a C++20 compiler, Git, and network access on the first
configure (CMake fetches the pinned GoogleTest release).

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

The project enables `-Wall -Wextra -Wpedantic` on GCC and Clang. See
[`docs/architecture.md`](docs/architecture.md) and
[`docs/protocol.md`](docs/protocol.md) for the current design and its boundaries.

## Repository layout

```text
include/atlasfs/  Public C++ interfaces
src/              Implementations
tests/            Unit and loopback socket integration tests
docs/             Engineering decisions and protocol specification
```

## Current scope

Object identifiers contain only ASCII letters, digits, `.`, `_`, and `-`, are at
most 255 bytes, and cannot be `.` or `..`. Objects are limited to slightly below
16 MiB so complete requests and responses remain inside the framing limit. The
internal `.atlasfs-tmp-` prefix is reserved. This milestone is single-node and
sequential: it does not implement replication, a coordinator, or production
concurrency. Chunking is a filesystem detail; the network API still transfers one
bounded logical object per PUT or GET.
