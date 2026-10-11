#include "atlasfs/metrics.hpp"
#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

namespace atlasfs::metrics {
namespace {
thread_local std::shared_ptr<Collector> active;
constexpr std::array<std::string_view, counter_count> names{
    "serialized_payload_sent_bytes", "serialized_payload_received_bytes", "frames_sent", "frames_received", "framed_sent_bytes", "framed_received_bytes",
    "tls_application_sent_bytes", "tls_application_received_bytes", "socket_sent_bytes", "socket_received_bytes", "socket_read_calls",
    "socket_write_calls", "socket_partial_reads", "socket_partial_writes", "socket_errors",
    "stream_read_calls", "stream_write_calls", "tls_handshakes", "tls_handshake_ns",
    "tcp_connects", "tcp_connect_ns", "connection_failures", "deadline_expirations",
    "file_read_calls", "file_write_calls", "file_read_bytes", "file_write_bytes",
    "chunk_read_bytes", "chunk_write_bytes", "manifest_read_bytes", "manifest_write_bytes",
    "catalog_write_bytes", "journal_write_bytes", "stream_file_flushes",
    "file_sync_calls", "file_sync_ns", "directory_sync_calls", "directory_sync_ns",
    "sync_failures", "rename_calls", "rename_ns", "rename_failures",
    "temporary_created", "temporary_committed", "temporary_cleaned",
    "startup_calls", "startup_ns", "startup_entries",
    "publication_calls", "publication_ns", "verification_calls", "verification_ns",
    "cas_calls", "cas_ns", "completion_calls", "completion_ns",
    "queue_calls", "queue_ns", "rpc_calls", "rpc_ns", "rpc_failures", "tcp_nodelay_connections"
};
bool within(std::string_view path, std::string_view root) noexcept {
    return path == root || (path.starts_with(root) && path.size() > root.size() && path[root.size()] == '/');
}
bool temporary(std::string_view path) noexcept {
    const auto file = path.substr(path.find_last_of('/') + 1);
    return path.find("/transactions/") != std::string_view::npos || file.ends_with(".tmp") ||
        file.starts_with(".atlasfs-tmp-") || file.starts_with(".repair-");
}
Counter category(std::string_view path, bool writing) noexcept {
    const auto file = path.substr(path.find_last_of('/') + 1);
    if (file.find("chunk_") != std::string_view::npos) return writing ? Counter::chunk_write_bytes : Counter::chunk_read_bytes;
    if (file == "manifest" || file.find("manifest") != std::string_view::npos || path.find("/committed/") != std::string_view::npos)
        return writing ? Counter::manifest_write_bytes : Counter::manifest_read_bytes;
    if (file == "catalog" || file.starts_with("catalog.")) return Counter::catalog_write_bytes;
    return Counter::journal_write_bytes;
}
void file_bytes(Collector& collector, std::string_view path, std::uint64_t bytes, bool writing) noexcept {
    collector.add(writing ? Counter::file_write_bytes : Counter::file_read_bytes, bytes);
    const auto type = category(path, writing);
    if (writing || type == Counter::chunk_read_bytes || type == Counter::manifest_read_bytes) collector.add(type, bytes);
}
int sync(int fd, bool data) noexcept {
    if (!current()) return data ? ::fdatasync(fd) : ::fsync(fd);
    struct stat info{};
    const bool directory = ::fstat(fd, &info) == 0 && S_ISDIR(info.st_mode);
    Timer timer(directory ? Counter::directory_sync_calls : Counter::file_sync_calls,
                directory ? Counter::directory_sync_ns : Counter::file_sync_ns);
    const int result = data ? ::fdatasync(fd) : ::fsync(fd);
    const int saved = errno;
    if (result < 0) add(Counter::sync_failures);
    errno = saved; return result;
}
}
std::string_view name(Counter counter) noexcept { return names[static_cast<std::size_t>(counter)]; }
Collector::Collector(std::size_t path_limit) : limit_(path_limit) {
    if (!limit_ || limit_ > capacity) throw std::invalid_argument("invalid metric path bound");
}
void Collector::add(Counter counter, std::uint64_t value) noexcept {
    auto& cell = counters_[static_cast<std::size_t>(counter)];
    auto old = cell.load(std::memory_order_relaxed);
    for (;;) {
        const bool overflow = value > std::numeric_limits<std::uint64_t>::max() - old;
        const auto next = overflow ? std::numeric_limits<std::uint64_t>::max() : old + value;
        if (cell.compare_exchange_weak(old, next, std::memory_order_relaxed)) {
            if (overflow) saturated_.store(true, std::memory_order_relaxed);
            return;
        }
    }
}
Snapshot Collector::snapshot() const noexcept {
    Snapshot result;
    for (std::size_t n = 0; n < counter_count; ++n) result.values[n] = counters_[n].load(std::memory_order_relaxed);
    result.saturated = saturated_.load(std::memory_order_relaxed);
    std::lock_guard lock(mutex_);
    result.accounting_complete = complete_; result.disk_baseline = baseline_; result.disk_current = disk_;
    result.disk_peak = peak_; result.temporary_current = temporary_; result.temporary_peak = temporary_peak_;
    result.tracked_paths = files_.size(); return result;
}
void Collector::reset() noexcept {
    for (auto& value : counters_) value.store(0, std::memory_order_relaxed);
    saturated_.store(false, std::memory_order_relaxed);
}
void Collector::totals() noexcept {
    disk_ = temporary_ = 0;
    for (const auto& [path, size] : files_) {
        if (size > std::numeric_limits<std::uint64_t>::max() - disk_) { complete_ = false; return; }
        disk_ += size; if (temporary(path)) temporary_ += size;
    }
    peak_ = std::max(peak_, disk_); temporary_peak_ = std::max(temporary_peak_, temporary_);
}
void Collector::account(const std::string& path, std::uint64_t size) {
    if (root_.empty() || !within(path, root_)) return;
    if (!files_.contains(path) && files_.size() >= limit_) { complete_ = false; return; }
    const auto found = files_.find(path);
    const auto before = found == files_.end() ? 0 : found->second;
    if (size > std::numeric_limits<std::uint64_t>::max() - (disk_ - before)) { complete_ = false; return; }
    files_[path] = size; disk_ = disk_ - before + size;
    if (temporary(path)) temporary_ = temporary_ - before + size;
    peak_ = std::max(peak_, disk_); temporary_peak_ = std::max(temporary_peak_, temporary_);
}
void Collector::begin_files(const std::filesystem::path& root) noexcept {
    std::lock_guard lock(mutex_);
    try {
        root_ = std::filesystem::absolute(root).lexically_normal().generic_string();
        files_.clear(); descriptors_.clear(); complete_ = true;
        baseline_ = disk_ = peak_ = temporary_ = temporary_peak_ = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
            if (entry.is_regular_file() && entry.path().extension() != ".pem") {
                if (files_.size() >= limit_) { complete_ = false; break; }
                files_.emplace(std::filesystem::absolute(entry.path()).lexically_normal().generic_string(), entry.file_size());
            }
        }
        totals(); baseline_ = disk_;
    } catch (...) { complete_ = false; }
}
void Collector::opened(int fd, const char* path, int flags) noexcept {
    if (fd < 0) return;
    std::lock_guard lock(mutex_);
    try {
        const auto key = std::filesystem::absolute(path).lexically_normal().generic_string();
        struct stat info{};
        if (::fstat(fd, &info) != 0 || !S_ISREG(info.st_mode)) return;
        if (descriptors_.size() >= limit_ && !descriptors_.contains(fd)) { complete_ = false; return; }
        descriptors_[fd] = key; account(key, static_cast<std::uint64_t>(info.st_size));
        if ((flags & O_CREAT) && temporary(key)) add(Counter::temporary_created);
    } catch (...) { complete_ = false; }
}
void Collector::closed(int fd) noexcept { std::lock_guard lock(mutex_); descriptors_.erase(fd); }
void Collector::invalidate_accounting() noexcept { std::lock_guard lock(mutex_); complete_ = false; }
void Collector::transferred(int fd, std::uint64_t bytes, bool writing) noexcept {
    std::lock_guard lock(mutex_);
    try {
        const auto found = descriptors_.find(fd);
        if (found == descriptors_.end()) { add(writing ? Counter::file_write_bytes : Counter::file_read_bytes, bytes); return; }
        file_bytes(*this, found->second, bytes, writing);
        if (writing) { struct stat info{}; if (::fstat(fd, &info) == 0) account(found->second, static_cast<std::uint64_t>(info.st_size)); else complete_ = false; }
    } catch (...) { complete_ = false; }
}
void Collector::observed_file(const std::filesystem::path& path, std::uint64_t written) noexcept {
    std::lock_guard lock(mutex_);
    try {
        const auto key = std::filesystem::absolute(path).lexically_normal().generic_string();
        file_bytes(*this, key, written, true); add(Counter::stream_file_flushes);
        if (temporary(key)) add(Counter::temporary_created);
        account(key, std::filesystem::file_size(path));
    } catch (...) { complete_ = false; }
}
void Collector::erase_tree(const std::string& path) {
    for (auto it = files_.begin(); it != files_.end();) {
        if (within(it->first, path)) { if (temporary(it->first)) add(Counter::temporary_cleaned); it = files_.erase(it); }
        else ++it;
    }
}
void Collector::removed(const std::filesystem::path& path) noexcept {
    std::lock_guard lock(mutex_);
    try { erase_tree(std::filesystem::absolute(path).lexically_normal().generic_string()); totals(); }
    catch (...) { complete_ = false; }
}
void Collector::renamed(const std::filesystem::path& from, const std::filesystem::path& to) noexcept {
    std::lock_guard lock(mutex_);
    try {
        const auto old = std::filesystem::absolute(from).lexically_normal().generic_string();
        const auto target = std::filesystem::absolute(to).lexically_normal().generic_string();
        erase_tree(target);
        std::map<std::string, std::uint64_t, std::less<>> moved;
        for (auto it = files_.begin(); it != files_.end();) {
            if (within(it->first, old)) {
                const auto key = target + it->first.substr(old.size());
                if (temporary(it->first) && !temporary(key)) add(Counter::temporary_committed);
                moved.emplace(key, it->second); it = files_.erase(it);
            } else ++it;
        }
        files_.merge(moved); totals();
    } catch (...) { complete_ = false; }
}
std::shared_ptr<Collector> current_owner() noexcept { return active; }
Collector* current() noexcept { return active.get(); }
Scope::Scope(std::shared_ptr<Collector> collector) noexcept : previous_(std::move(active)) { active = std::move(collector); }
Scope::~Scope() { active = std::move(previous_); }
Timer::Timer(Counter calls, Counter nanoseconds) noexcept : collector_(current_owner()), nanoseconds_(nanoseconds) {
    if (collector_) { collector_->add(calls); start_ = std::chrono::steady_clock::now(); }
}
Timer::~Timer() {
    if (collector_) collector_->add(nanoseconds_, static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count()));
}
namespace io {
int open(const char* path, int flags, mode_t mode) noexcept {
    const int fd = ::open(path, flags, mode); const int saved = errno;
    if (auto* collector = current()) collector->opened(fd, path, flags);
    errno = saved; return fd;
}
int close(int fd) noexcept {
    // Retire the observation before releasing the kernel descriptor. Another
    // attached thread may reuse the same integer as soon as ::close returns.
    if (auto* collector = current()) collector->closed(fd);
    const int result = ::close(fd); const int saved = errno;
    errno = saved; return result;
}
ssize_t read(int fd, void* bytes, std::size_t size) noexcept {
    const auto result = ::read(fd, bytes, size); const int saved = errno;
    if (auto* collector = current()) { collector->add(Counter::file_read_calls); if (result > 0) collector->transferred(fd, result, false); }
    errno = saved; return result;
}
ssize_t write(int fd, const void* bytes, std::size_t size) noexcept {
    const auto result = ::write(fd, bytes, size); const int saved = errno;
    if (auto* collector = current()) { collector->add(Counter::file_write_calls); if (result > 0) collector->transferred(fd, result, true); }
    errno = saved; return result;
}
int fsync(int fd) noexcept { return sync(fd, false); }
int fdatasync(int fd) noexcept { return sync(fd, true); }
void rename(const std::filesystem::path& from, const std::filesystem::path& to, std::error_code& error) {
    { Timer timer(Counter::rename_calls, Counter::rename_ns);
      std::filesystem::rename(from, to, error); }
    if (error) add(Counter::rename_failures); else if (auto* collector = current()) collector->renamed(from, to);
}
bool remove(const std::filesystem::path& path, std::error_code& error) {
    const auto result = std::filesystem::remove(path, error);
    if (!error) if (auto* collector = current()) collector->removed(path);
    return result;
}
std::uintmax_t remove_all(const std::filesystem::path& path, std::error_code& error) {
    const auto result = std::filesystem::remove_all(path, error);
    if (auto* collector = current()) {
        if (!error) collector->removed(path);
        else collector->invalidate_accounting(); // Recursive deletion can fail after partial progress.
    }
    return result;
}
int unlink(const char* path) noexcept {
    const int result = ::unlink(path); const int saved = errno;
    if (result == 0) if (auto* collector = current()) {
        try { collector->removed(path); } catch (...) { collector->invalidate_accounting(); }
    }
    errno = saved; return result;
}
void observed_write(const std::filesystem::path& path, std::uint64_t size) noexcept {
    if (auto* collector = current()) collector->observed_file(path, size);
}
void observed_read(std::uint64_t size) noexcept { add(Counter::file_read_bytes, size); }
void observed_record_read(const std::filesystem::path& path) noexcept {
    if (auto* collector = current()) {
        try { collector->add(Counter::file_read_bytes, std::filesystem::file_size(path)); }
        catch (...) { collector->invalidate_accounting(); }
    }
}
}
} // namespace atlasfs::metrics
