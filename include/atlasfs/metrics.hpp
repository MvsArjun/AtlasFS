#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <sys/types.h>

namespace atlasfs::metrics {
// Bounded, opt-in observations. No payloads, identifiers or credentials are exported.
enum class Counter : std::size_t {
    payload_sent, payload_received, frames_sent, frames_received, framed_sent, framed_received,
    tls_sent, tls_received, socket_sent, socket_received, socket_read_calls,
    socket_write_calls, socket_partial_reads, socket_partial_writes, socket_errors,
    stream_read_calls, stream_write_calls, tls_handshakes, tls_handshake_ns,
    tcp_connects, tcp_connect_ns, connection_failures, deadlines,
    file_read_calls, file_write_calls, file_read_bytes, file_write_bytes,
    chunk_read_bytes, chunk_write_bytes, manifest_read_bytes, manifest_write_bytes,
    catalog_write_bytes, journal_write_bytes, stream_file_flushes,
    file_sync_calls, file_sync_ns, directory_sync_calls, directory_sync_ns,
    sync_failures, rename_calls, rename_ns, rename_failures,
    temporary_created, temporary_committed, temporary_cleaned,
    startup_calls, startup_ns, startup_entries,
    publication_calls, publication_ns, verification_calls, verification_ns,
    cas_calls, cas_ns, completion_calls, completion_ns,
    queue_calls, queue_ns, rpc_calls, rpc_ns, rpc_failures, nodelay_connections, count
};
inline constexpr std::size_t counter_count = static_cast<std::size_t>(Counter::count);
[[nodiscard]] std::string_view name(Counter counter) noexcept;
struct Snapshot {
    std::array<std::uint64_t, counter_count> values{};
    bool saturated{}, accounting_complete{true};
    std::uint64_t disk_baseline{}, disk_current{}, disk_peak{}, temporary_current{}, temporary_peak{};
    std::size_t tracked_paths{};
    [[nodiscard]] std::uint64_t get(Counter counter) const noexcept { return values[static_cast<std::size_t>(counter)]; }
};
class Collector {
public:
    static constexpr std::size_t capacity = 65536;
    explicit Collector(std::size_t path_limit = capacity);
    void add(Counter counter, std::uint64_t value = 1) noexcept;
    [[nodiscard]] Snapshot snapshot() const noexcept;
    // Reset counters only with producers quiescent; snapshots are not transactions.
    void reset() noexcept;
    // Rebase apparent-file accounting from a private workload root, outside timing.
    void begin_files(const std::filesystem::path& root) noexcept;
    void opened(int fd, const char* path, int flags) noexcept;
    void closed(int fd) noexcept;
    void transferred(int fd, std::uint64_t bytes, bool writing) noexcept;
    void observed_file(const std::filesystem::path& path, std::uint64_t written) noexcept;
    void renamed(const std::filesystem::path& from, const std::filesystem::path& to) noexcept;
    void removed(const std::filesystem::path& path) noexcept;
    void invalidate_accounting() noexcept;
private:
    void account(const std::string& path, std::uint64_t size);
    void erase_tree(const std::string& path);
    void totals() noexcept;
    std::array<std::atomic<std::uint64_t>, counter_count> counters_{};
    std::atomic_bool saturated_{};
    mutable std::mutex mutex_;
    std::map<std::string, std::uint64_t, std::less<>> files_;
    std::map<int, std::string> descriptors_;
    std::string root_;
    const std::size_t limit_;
    bool complete_{true};
    std::uint64_t baseline_{}, disk_{}, peak_{}, temporary_{}, temporary_peak_{};
};
[[nodiscard]] std::shared_ptr<Collector> current_owner() noexcept;
[[nodiscard]] Collector* current() noexcept;
class Scope {
public:
    explicit Scope(std::shared_ptr<Collector> collector) noexcept;
    ~Scope();
    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;
private:
    std::shared_ptr<Collector> previous_;
};
inline void add(Counter counter, std::uint64_t value = 1) noexcept {
    if (auto* collector = current()) collector->add(counter, value);
}
class Timer {
public:
    Timer(Counter calls, Counter nanoseconds) noexcept;
    ~Timer();
    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;
private:
    std::shared_ptr<Collector> collector_;
    Counter nanoseconds_;
    std::chrono::steady_clock::time_point start_{};
};
// Preserve syscall return values, errno, retries and publication order.
namespace io {
int open(const char* path, int flags, mode_t mode = 0666) noexcept;
int close(int fd) noexcept;
ssize_t read(int fd, void* bytes, std::size_t size) noexcept;
ssize_t write(int fd, const void* bytes, std::size_t size) noexcept;
int fsync(int fd) noexcept;
int fdatasync(int fd) noexcept;
void rename(const std::filesystem::path& from, const std::filesystem::path& to, std::error_code& error);
bool remove(const std::filesystem::path& path, std::error_code& error);
std::uintmax_t remove_all(const std::filesystem::path& path, std::error_code& error);
int unlink(const char* path) noexcept;
// Observe a successful ostream flush, whose internal syscall counts are unavailable.
void observed_write(const std::filesystem::path& path, std::uint64_t size) noexcept;
void observed_read(std::uint64_t size) noexcept;
void observed_record_read(const std::filesystem::path& path) noexcept;
}
} // namespace atlasfs::metrics
