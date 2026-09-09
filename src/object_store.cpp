#include "atlasfs/object_store.hpp"
#include "atlasfs/checksum.hpp"
#include "atlasfs/manifest.hpp"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <iomanip>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace atlasfs::storage {
namespace {

constexpr std::size_t kMaximumManifestSize =
    30U + kMaxGenerationIdSize + static_cast<std::size_t>(kMaxChunkCount) * 12U + 4U;

class FileDescriptor {
public:
    explicit FileDescriptor(int descriptor = -1) noexcept : descriptor_(descriptor) {}
    ~FileDescriptor() { reset(); }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    [[nodiscard]] int get() const noexcept { return descriptor_; }
    [[nodiscard]] bool valid() const noexcept { return descriptor_ >= 0; }
    int release() noexcept { const int value = descriptor_; descriptor_ = -1; return value; }
    int close_now() noexcept { const int value = release(); return value < 0 ? 0 : ::close(value); }
    void reset() noexcept { const int value = release(); if (value >= 0) { ::close(value); } }
private:
    int descriptor_;
};

class RemoveOnFailure {
public:
    explicit RemoveOnFailure(std::filesystem::path path) : path_(std::move(path)) {}
    ~RemoveOnFailure() { if (active_) { std::error_code ignored; std::filesystem::remove_all(path_, ignored); } }
    RemoveOnFailure(const RemoveOnFailure&) = delete;
    RemoveOnFailure& operator=(const RemoveOnFailure&) = delete;
    void release() noexcept { active_ = false; }
private:
    std::filesystem::path path_;
    bool active_{true};
};

bool write_all(int fd, std::span<const std::byte> data) noexcept {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const auto result = ::write(fd, data.data() + offset, data.size() - offset);
        if (result > 0) { offset += static_cast<std::size_t>(result); }
        else if (result < 0 && errno == EINTR) { continue; }
        else { return false; }
    }
    return true;
}

bool read_all(int fd, std::span<std::byte> data) noexcept {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const auto result = ::read(fd, data.data() + offset, data.size() - offset);
        if (result > 0) { offset += static_cast<std::size_t>(result); }
        else if (result < 0 && errno == EINTR) { continue; }
        else { return false; }
    }
    return true;
}

bool at_eof(int fd) noexcept {
    std::byte extra{};
    ssize_t result;
    do { result = ::read(fd, &extra, 1); } while (result < 0 && errno == EINTR);
    return result == 0;
}

bool sync_file(int fd) noexcept {
    int result;
    do { result = ::fdatasync(fd); } while (result < 0 && errno == EINTR);
    return result == 0;
}

std::string chunk_filename(std::uint32_t index) {
    std::ostringstream name;
    name << "chunk_" << std::setw(6) << std::setfill('0') << index;
    return name.str();
}

std::optional<std::vector<std::byte>> read_regular_file(const std::filesystem::path& path,
                                                        std::size_t maximum_size) {
    FileDescriptor file(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (!file.valid()) { return std::nullopt; }
    struct stat metadata {};
    if (::fstat(file.get(), &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
        metadata.st_size < 0 || static_cast<std::uint64_t>(metadata.st_size) > maximum_size) {
        return std::nullopt;
    }
    std::vector<std::byte> data(static_cast<std::size_t>(metadata.st_size));
    if (!read_all(file.get(), data) || !at_eof(file.get())) { return std::nullopt; }
    return data;
}

bool write_synced_file(const std::filesystem::path& path, std::span<const std::byte> data,
                       bool fail_write, bool fail_sync) {
    FileDescriptor file(::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
    return file.valid() && !fail_write && write_all(file.get(), data) && !fail_sync &&
           sync_file(file.get()) && file.close_now() == 0;
}

}  // namespace

ObjectStore::ObjectStore(std::filesystem::path root, std::size_t max_object_size,
                         std::uint32_t chunk_size, PutFaultInjector fault_injector)
    : max_object_size_(max_object_size), chunk_size_(chunk_size),
      fault_injector_(std::move(fault_injector)) {
    if (max_object_size > object_protocol::kMaxObjectDataSize || chunk_size == 0 ||
        chunk_size > max_object_size) {
        throw std::invalid_argument("invalid AtlasFS object or chunk size limit");
    }
    std::error_code error;
    const auto storage_root = root / "objects";
    for (const auto* name : {"committed", "chunks", "transactions"}) {
        std::filesystem::create_directories(storage_root / name, error);
        if (error) { throw std::system_error(error, "create AtlasFS storage directories"); }
    }
    objects_directory_ = std::filesystem::canonical(storage_root / "committed", error);
    if (!error) { chunks_directory_ = std::filesystem::canonical(storage_root / "chunks", error); }
    if (!error) { transactions_directory_ = std::filesystem::canonical(storage_root / "transactions", error); }
    if (error) { throw std::system_error(error, "resolve AtlasFS storage directories"); }
    clean_incomplete_state();
}

StoreError ObjectStore::put(std::string_view object_id, std::span<const std::byte> data,
                            std::uint32_t supplied_checksum) {
    if (!valid_object_id(object_id)) { return StoreError::invalid_object_id; }
    if (data.size() > max_object_size_) { return StoreError::payload_too_large; }
    if (should_fail(PutFaultPoint::checksum_calculation)) { return StoreError::filesystem_error; }
    const auto object_checksum = integrity::crc32c(data);
    if (object_checksum != supplied_checksum) { return StoreError::checksum_mismatch; }
    const auto chunk_count = data.empty() ? 0U : 1U + (data.size() - 1U) / chunk_size_;
    if (chunk_count > kMaxChunkCount) { return StoreError::payload_too_large; }

    std::optional<ObjectManifest> previous_manifest;
    if (const auto previous = read_regular_file(object_path(object_id), kMaximumManifestSize)) {
        previous_manifest = decode_manifest(*previous, max_object_size_);
    }

    std::filesystem::path transaction_path;
    std::string generation_id;
    bool transaction_created = false;
    std::error_code error;
    for (unsigned attempt = 0; attempt < 100; ++attempt) {
        transaction_path = next_temporary_path();
        generation_id = transaction_path.filename().string();
        if (std::filesystem::exists(chunks_directory_ / generation_id, error)) { error.clear(); continue; }
        if (std::filesystem::create_directory(transaction_path, error)) {
            transaction_created = true;
            break;
        }
        if (error) { error.clear(); }
        transaction_path.clear();
    }
    if (!transaction_created) {
        return StoreError::filesystem_error;
    }
    RemoveOnFailure transaction_cleanup(transaction_path);
    ObjectManifest manifest{static_cast<std::uint64_t>(data.size()), chunk_size_,
                            object_checksum, generation_id, {}};
    manifest.chunks.reserve(chunk_count);
    for (std::uint32_t index = 0; index < chunk_count; ++index) {
        const auto offset = static_cast<std::size_t>(index) * chunk_size_;
        const auto length = std::min<std::size_t>(chunk_size_, data.size() - offset);
        const auto chunk = data.subspan(offset, length);
        if (should_fail(PutFaultPoint::chunk_creation) ||
            !write_synced_file(transaction_path / chunk_filename(index), chunk,
                               should_fail(PutFaultPoint::chunk_write),
                               should_fail(PutFaultPoint::chunk_sync))) {
            return StoreError::filesystem_error;
        }
        manifest.chunks.push_back({index, static_cast<std::uint32_t>(length),
                                   integrity::crc32c(chunk)});
    }
    if (should_fail(PutFaultPoint::manifest_creation)) { return StoreError::filesystem_error; }
    const auto encoded_manifest = encode_manifest(manifest);
    if (!write_synced_file(transaction_path / "manifest", encoded_manifest,
                           should_fail(PutFaultPoint::manifest_write),
                           should_fail(PutFaultPoint::manifest_sync)) ||
        sync_directory(transaction_path) != StoreError::none ||
        should_fail(PutFaultPoint::generation_rename)) {
        return StoreError::filesystem_error;
    }

    const auto generation_path = chunks_directory_ / generation_id;
    std::filesystem::rename(transaction_path, generation_path, error);
    if (error) { return StoreError::filesystem_error; }
    transaction_cleanup.release();
    RemoveOnFailure generation_cleanup(generation_path);
    if (sync_directory(chunks_directory_) != StoreError::none) { return StoreError::filesystem_error; }

    const auto manifest_temporary = objects_directory_ / (".atlasfs-tmp-" + generation_id);
    RemoveOnFailure manifest_cleanup(manifest_temporary);
    if (!write_synced_file(manifest_temporary, encoded_manifest, false, false) ||
        should_fail(PutFaultPoint::commit_rename)) {
        return StoreError::filesystem_error;
    }
    std::filesystem::rename(manifest_temporary, object_path(object_id), error);
    if (error) { return StoreError::filesystem_error; }
    manifest_cleanup.release();
    generation_cleanup.release();
    if (sync_directory(objects_directory_) != StoreError::none) { return StoreError::filesystem_error; }
    if (previous_manifest && previous_manifest->generation_id != generation_id) {
        std::filesystem::remove_all(chunks_directory_ / previous_manifest->generation_id, error);
    }
    return StoreError::none;
}

GetResult ObjectStore::get(std::string_view object_id) const {
    if (!valid_object_id(object_id)) { return {StoreError::invalid_object_id, {}, 0}; }
    const auto manifest_bytes = read_regular_file(object_path(object_id), kMaximumManifestSize);
    if (!manifest_bytes) {
        return {std::filesystem::exists(object_path(object_id)) ? StoreError::corrupt_object
                                                                : StoreError::not_found, {}, 0};
    }
    const auto manifest = decode_manifest(*manifest_bytes, max_object_size_);
    if (!manifest) { return {StoreError::corrupt_object, {}, 0}; }
    const auto generation_path = chunks_directory_ / manifest->generation_id;
    const auto generation_manifest = read_regular_file(generation_path / "manifest", kMaximumManifestSize);
    if (!generation_manifest || *generation_manifest != *manifest_bytes) {
        return {StoreError::corrupt_object, {}, 0};
    }
    std::error_code error;
    std::size_t entry_count = 0;
    for (std::filesystem::directory_iterator it(generation_path, error), end;
         !error && it != end && entry_count <= manifest->chunks.size() + 1U;
         it.increment(error)) { ++entry_count; }
    if (error || entry_count != manifest->chunks.size() + 1U) {
        return {StoreError::corrupt_object, {}, 0};
    }
    std::vector<std::byte> object;
    object.reserve(static_cast<std::size_t>(manifest->object_size));
    for (const auto& metadata : manifest->chunks) {
        const auto chunk = read_regular_file(generation_path / chunk_filename(metadata.index), metadata.length);
        if (!chunk || chunk->size() != metadata.length || integrity::crc32c(*chunk) != metadata.checksum) {
            return {StoreError::corrupt_object, {}, 0};
        }
        object.insert(object.end(), chunk->begin(), chunk->end());
    }
    if (object.size() != manifest->object_size || integrity::crc32c(object) != manifest->object_checksum) {
        return {StoreError::corrupt_object, {}, 0};
    }
    return {StoreError::none, std::move(object), manifest->object_checksum};
}

StoreError ObjectStore::remove(std::string_view object_id) {
    if (!valid_object_id(object_id)) { return StoreError::invalid_object_id; }
    const auto manifest_bytes = read_regular_file(object_path(object_id), kMaximumManifestSize);
    if (!manifest_bytes) {
        return std::filesystem::exists(object_path(object_id)) ? StoreError::corrupt_object
                                                               : StoreError::not_found;
    }
    const auto manifest = decode_manifest(*manifest_bytes, max_object_size_);
    if (!manifest) { return StoreError::corrupt_object; }
    if (::unlink(object_path(object_id).c_str()) != 0) {
        return errno == ENOENT ? StoreError::not_found : StoreError::filesystem_error;
    }
    if (sync_directory(objects_directory_) != StoreError::none) { return StoreError::filesystem_error; }
    std::error_code error;
    std::filesystem::remove_all(chunks_directory_ / manifest->generation_id, error);
    if (error || sync_directory(chunks_directory_) != StoreError::none) {
        return StoreError::filesystem_error;
    }
    return StoreError::none;
}

const std::filesystem::path& ObjectStore::objects_directory() const noexcept { return objects_directory_; }
const std::filesystem::path& ObjectStore::chunks_directory() const noexcept { return chunks_directory_; }
const std::filesystem::path& ObjectStore::transactions_directory() const noexcept { return transactions_directory_; }
std::uint32_t ObjectStore::chunk_size() const noexcept { return chunk_size_; }

bool ObjectStore::valid_object_id(std::string_view object_id) noexcept {
    if (object_id.empty() || object_id.size() > object_protocol::kMaxObjectIdSize ||
        object_id == "." || object_id == ".." || object_id.starts_with(".atlasfs-tmp-")) {
        return false;
    }
    for (const unsigned char character : object_id) {
        const bool allowed = (character >= 'a' && character <= 'z') ||
                             (character >= 'A' && character <= 'Z') ||
                             (character >= '0' && character <= '9') || character == '-' ||
                             character == '_' || character == '.';
        if (!allowed) { return false; }
    }
    return true;
}

std::filesystem::path ObjectStore::object_path(std::string_view object_id) const {
    return objects_directory_ / std::string(object_id);
}

std::filesystem::path ObjectStore::next_temporary_path() {
    const auto sequence = temporary_counter_.fetch_add(1);
    return transactions_directory_ / ("generation-" +
        std::to_string(static_cast<unsigned long long>(::getpid())) + "-" + std::to_string(sequence));
}

StoreError ObjectStore::sync_directory(const std::filesystem::path& path) const noexcept {
    FileDescriptor directory(::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (!directory.valid()) { return StoreError::filesystem_error; }
    int result;
    do { result = ::fsync(directory.get()); } while (result < 0 && errno == EINTR);
    return result == 0 ? StoreError::none : StoreError::filesystem_error;
}

void ObjectStore::clean_incomplete_state() {
    std::error_code error;
    std::vector<std::filesystem::path> removals;
    for (std::filesystem::directory_iterator it(transactions_directory_, error), end;
         !error && it != end; it.increment(error)) { removals.push_back(it->path()); }
    for (const auto& path : removals) {
        std::filesystem::remove_all(path, error);
        if (error) { break; }
    }
    if (error) { throw std::system_error(error, "clean AtlasFS transactions"); }
    std::set<std::string> referenced;
    removals.clear();
    for (std::filesystem::directory_iterator it(objects_directory_, error), end;
         !error && it != end; it.increment(error)) {
        if (it->path().filename().string().starts_with(".atlasfs-tmp-")) {
            removals.push_back(it->path());
            continue;
        }
        if (const auto encoded = read_regular_file(it->path(), kMaximumManifestSize)) {
            if (const auto manifest = decode_manifest(*encoded, max_object_size_)) {
                referenced.insert(manifest->generation_id);
            }
        }
    }
    for (const auto& path : removals) {
        std::filesystem::remove_all(path, error);
        if (error) { break; }
    }
    if (error) { throw std::system_error(error, "scan AtlasFS manifests"); }
    removals.clear();
    for (std::filesystem::directory_iterator it(chunks_directory_, error), end;
         !error && it != end; it.increment(error)) {
        if (!referenced.contains(it->path().filename().string())) {
            removals.push_back(it->path());
        }
    }
    for (const auto& path : removals) {
        std::filesystem::remove_all(path, error);
        if (error) { break; }
    }
    if (error) { throw std::system_error(error, "clean AtlasFS orphan generations"); }
}

bool ObjectStore::should_fail(PutFaultPoint point) const {
    return fault_injector_ && fault_injector_(point);
}

}  // namespace atlasfs::storage
