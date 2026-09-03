# AtlasFS

AtlasFS is a C++20/Linux object-storage systems project. The current milestone
provides a documented binary wire format, incremental stream parser, and a
blocking POSIX TCP client/server transport over loopback. The server currently
returns deterministic echo responses; it does not store objects yet.

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
