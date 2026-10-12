#include "atlasfs/object_store.hpp"
#include "atlasfs/metrics.hpp"
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
    int close_now() noexcept { const int value = release(); return value < 0 ? 0 : metrics::io::close(value); }
    void reset() noexcept { const int value = release(); if (value >= 0) { metrics::io::close(value); } }
private:
    int descriptor_;
};

class RemoveOnFailure {
public:
    explicit RemoveOnFailure(std::filesystem::path path) : path_(std::move(path)) {}
    ~RemoveOnFailure() { if (active_) { std::error_code ignored; metrics::io::remove_all(path_, ignored); } }
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
        const auto result = metrics::io::write(fd, data.data() + offset, data.size() - offset);
        if (result > 0) { offset += static_cast<std::size_t>(result); }
        else if (result < 0 && errno == EINTR) { continue; }
        else { return false; }
    }
    return true;
}

bool read_all(int fd, std::span<std::byte> data) noexcept {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const auto result = metrics::io::read(fd, data.data() + offset, data.size() - offset);
        if (result > 0) { offset += static_cast<std::size_t>(result); }
        else if (result < 0 && errno == EINTR) { continue; }
        else { return false; }
    }
    return true;
}

bool at_eof(int fd) noexcept {
    std::byte extra{};
    ssize_t result;
    do { result = metrics::io::read(fd, &extra, 1); } while (result < 0 && errno == EINTR);
    return result == 0;
}

bool sync_file(int fd) noexcept {
    int result;
    do { result = metrics::io::fdatasync(fd); } while (result < 0 && errno == EINTR);
    return result == 0;
}

std::string chunk_filename(std::uint32_t index) {
    std::ostringstream name;
    name << "chunk_" << std::setw(6) << std::setfill('0') << index;
    return name.str();
}

std::optional<std::vector<std::byte>> read_regular_file(const std::filesystem::path& path,
                                                        std::size_t maximum_size) {
    FileDescriptor file(metrics::io::open(path.c_str(),
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
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
    FileDescriptor file(metrics::io::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
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
    metrics::io::rename(transaction_path, generation_path, error);
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
    metrics::io::rename(manifest_temporary, object_path(object_id), error);
    if (error) { return StoreError::filesystem_error; }
    manifest_cleanup.release();
    generation_cleanup.release();
    if (sync_directory(objects_directory_) != StoreError::none) { return StoreError::filesystem_error; }
    if (previous_manifest && previous_manifest->generation_id != generation_id) {
        metrics::io::remove_all(chunks_directory_ / previous_manifest->generation_id, error);
    }
    return StoreError::none;
}

StoreError ObjectStore::prepare_replica(
    std::string_view object_id, const ObjectManifest& manifest,
    std::span<const std::uint32_t> chunk_indices,
    std::span<const std::byte> object_data) {
    if (!valid_object_id(object_id)) { return StoreError::invalid_object_id; }
    if (object_data.size() > max_object_size_ || object_data.size() != manifest.object_size ||
        manifest.chunk_size != chunk_size_) {
        return StoreError::payload_too_large;
    }
    std::vector<std::byte> encoded;
    try { encoded = encode_manifest(manifest); }
    catch (const std::invalid_argument&) { return StoreError::corrupt_object; }
    if (integrity::crc32c(object_data) != manifest.object_checksum) {
        return StoreError::checksum_mismatch;
    }
    const auto key = pending_key(object_id, manifest.generation_id);
    if (pending_replicas_.contains(key)) { return StoreError::filesystem_error; }

    std::set<std::uint32_t> selected;
    for (const auto index : chunk_indices) {
        if (!selected.insert(index).second || index >= manifest.chunks.size() ||
            manifest.chunks[index].index != index) {
            return StoreError::corrupt_object;
        }
    }
    std::error_code error;
    const auto generation_path = chunks_directory_ / manifest.generation_id;
    if (std::filesystem::exists(generation_path, error) || error) {
        return StoreError::filesystem_error;
    }
    if (should_fail(PutFaultPoint::transaction_creation)) {
        return StoreError::filesystem_error;
    }
    const auto transaction_path = next_temporary_path();
    if (!std::filesystem::create_directory(transaction_path, error) || error) {
        return StoreError::filesystem_error;
    }
    RemoveOnFailure transaction_cleanup(transaction_path);
    for (const auto index : chunk_indices) {
        const auto& metadata = manifest.chunks[index];
        const auto offset = static_cast<std::size_t>(index) * manifest.chunk_size;
        if (offset > object_data.size() || metadata.length > object_data.size() - offset) {
            return StoreError::corrupt_object;
        }
        const auto chunk = object_data.subspan(offset, metadata.length);
        if (integrity::crc32c(chunk) != metadata.checksum ||
            should_fail(PutFaultPoint::chunk_creation) ||
            !write_synced_file(transaction_path / chunk_filename(index), chunk,
                               should_fail(PutFaultPoint::chunk_write),
                               should_fail(PutFaultPoint::chunk_sync))) {
            return StoreError::filesystem_error;
        }
    }
    if (should_fail(PutFaultPoint::manifest_creation) ||
        !write_synced_file(transaction_path / "manifest", encoded,
                           should_fail(PutFaultPoint::manifest_write),
                           should_fail(PutFaultPoint::manifest_sync)) ||
        sync_directory(transaction_path) != StoreError::none ||
        should_fail(PutFaultPoint::generation_rename)) {
        return StoreError::filesystem_error;
    }
    metrics::io::rename(transaction_path, generation_path, error);
    if (error) { return StoreError::filesystem_error; }
    transaction_cleanup.release();
    if (sync_directory(chunks_directory_) != StoreError::none) {
        metrics::io::remove_all(generation_path, error);
        return StoreError::filesystem_error;
    }
    std::optional<std::vector<std::byte>> previous;
    if (const auto bytes = read_regular_file(object_path(object_id), kMaximumManifestSize)) {
        previous = *bytes;
    }
    pending_replicas_.emplace(key, PendingReplica{std::move(previous), std::move(encoded), false});
    return StoreError::none;
}

StoreError ObjectStore::commit_replica(std::string_view object_id,
                                       std::string_view generation_id) {
    const auto key = pending_key(object_id, generation_id);
    const auto pending = pending_replicas_.find(key);
    if (pending == pending_replicas_.end()) { return StoreError::not_found; }
    if (pending->second.durability_unknown) { return StoreError::result_unknown; }
    if (pending->second.committed) { return StoreError::none; }
    const auto temporary = objects_directory_ / (".atlasfs-tmp-" + std::string(generation_id));
    std::error_code error;
    metrics::io::remove(temporary, error);
    error.clear();
    RemoveOnFailure cleanup(temporary);
    if (!write_synced_file(temporary, pending->second.manifest, false, false) ||
        should_fail(PutFaultPoint::commit_rename)) {
        return StoreError::filesystem_error;
    }
    metrics::io::rename(temporary, object_path(object_id), error);
    if (error) { return StoreError::filesystem_error; }
    cleanup.release();
    pending->second.committed = true;
    pending->second.durability_unknown = true;
    if (sync_directory(objects_directory_) != StoreError::none) {
        return StoreError::result_unknown;
    }
    pending->second.durability_unknown = false;
    return StoreError::none;
}

StoreError ObjectStore::verify_prepared_replica(
    std::string_view object_id, std::string_view generation_id,
    std::span<const std::uint32_t> chunk_indices) const {
    const auto pending = pending_replicas_.find(pending_key(object_id, generation_id));
    if (pending == pending_replicas_.end()) { return StoreError::not_found; }
    const auto manifest = decode_manifest(pending->second.manifest, max_object_size_);
    if (!manifest || manifest->generation_id != generation_id) {
        return StoreError::corrupt_object;
    }
    const auto generation_path = chunks_directory_ / std::string(generation_id);
    const auto durable_manifest = read_regular_file(generation_path / "manifest", kMaximumManifestSize);
    if (!durable_manifest || *durable_manifest != pending->second.manifest) {
        return StoreError::corrupt_object;
    }
    for (const auto index : chunk_indices) {
        if (index >= manifest->chunks.size() || manifest->chunks[index].index != index) {
            return StoreError::corrupt_object;
        }
        const auto& metadata = manifest->chunks[index];
        const auto chunk = read_regular_file(generation_path / chunk_filename(index), metadata.length);
        if (!chunk || chunk->size() != metadata.length ||
            integrity::crc32c(*chunk) != metadata.checksum) {
            return StoreError::corrupt_object;
        }
    }
    return StoreError::none;
}

StoreError ObjectStore::abort_replica(std::string_view object_id,
                                      std::string_view generation_id) {
    const auto key = pending_key(object_id, generation_id);
    const auto pending = pending_replicas_.find(key);
    if (pending == pending_replicas_.end()) { return StoreError::not_found; }
    if (pending->second.durability_unknown) { return StoreError::result_unknown; }
    if (should_fail(PutFaultPoint::rollback_deletion)) {
        return StoreError::filesystem_error;
    }
    std::error_code error;
    if (pending->second.committed) {
        if (pending->second.previous_manifest) {
            const auto temporary = objects_directory_ /
                (".atlasfs-tmp-rollback-" + std::string(generation_id));
            metrics::io::remove(temporary, error);
            error.clear();
            RemoveOnFailure cleanup(temporary);
            if (!write_synced_file(temporary, *pending->second.previous_manifest, false, false)) {
                return StoreError::filesystem_error;
            }
            metrics::io::rename(temporary, object_path(object_id), error);
            if (error) { return StoreError::filesystem_error; }
            cleanup.release();
        } else if (metrics::io::unlink(object_path(object_id).c_str()) != 0 && errno != ENOENT) {
            return StoreError::filesystem_error;
        }
        pending->second.durability_unknown = true;
        if (sync_directory(objects_directory_) != StoreError::none) {
            return StoreError::result_unknown;
        }
    }
    pending->second.durability_unknown = true;
    metrics::io::remove_all(chunks_directory_ / std::string(generation_id), error);
    if (error || sync_directory(chunks_directory_) != StoreError::none) {
        return StoreError::result_unknown;
    }
    pending_replicas_.erase(pending);
    return StoreError::none;
}

StoreError ObjectStore::finalize_replica(std::string_view object_id,
                                         std::string_view generation_id) {
    const auto pending = pending_replicas_.find(pending_key(object_id, generation_id));
    if (pending != pending_replicas_.end() && pending->second.durability_unknown) {
        return StoreError::result_unknown;
    }
    if (pending == pending_replicas_.end() || !pending->second.committed) {
        return StoreError::not_found;
    }
    if (pending->second.previous_manifest) {
        const auto previous = decode_manifest(*pending->second.previous_manifest, max_object_size_);
        if (previous && previous->generation_id != generation_id) {
            std::error_code error;
            metrics::io::remove_all(chunks_directory_ / previous->generation_id, error);
            if (error || sync_directory(chunks_directory_) != StoreError::none) {
                return StoreError::filesystem_error;
            }
        }
    }
    pending_replicas_.erase(pending);
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

ChunkResult ObjectStore::get_replica_chunk(std::string_view object_id,
                                           std::string_view generation_id,
                                           std::uint32_t chunk_index) const {
    if (!valid_object_id(object_id)) {
        return {StoreError::invalid_object_id, {}, {}, {}};
    }
    const auto manifest_bytes = read_regular_file(object_path(object_id), kMaximumManifestSize);
    if (!manifest_bytes) {
        return {std::filesystem::exists(object_path(object_id)) ? StoreError::corrupt_object
                                                                : StoreError::not_found,
                {}, {}, {}};
    }
    const auto manifest = decode_manifest(*manifest_bytes, max_object_size_);
    if (!manifest || manifest->generation_id != generation_id ||
        chunk_index >= manifest->chunks.size() || manifest->chunks[chunk_index].index != chunk_index) {
        return {StoreError::corrupt_object, {}, {}, {}};
    }
    const auto generation_path = chunks_directory_ / manifest->generation_id;
    const auto generation_manifest = read_regular_file(generation_path / "manifest", kMaximumManifestSize);
    if (!generation_manifest || *generation_manifest != *manifest_bytes) {
        return {StoreError::corrupt_object, {}, {}, {}};
    }
    const auto metadata = manifest->chunks[chunk_index];
    auto chunk = read_regular_file(generation_path / chunk_filename(chunk_index), metadata.length);
    if (!chunk) {
        return {std::filesystem::exists(generation_path / chunk_filename(chunk_index))
                    ? StoreError::corrupt_object : StoreError::not_found,
                *manifest, metadata, {}};
    }
    if (chunk->size() != metadata.length || integrity::crc32c(*chunk) != metadata.checksum) {
        return {StoreError::corrupt_object, *manifest, metadata, {}};
    }
    return {StoreError::none, *manifest, metadata, std::move(*chunk)};
}

StoreError ObjectStore::publish_repair_chunk(
    std::string_view object_id, const ObjectManifest& manifest,
    const ChunkMetadata& chunk, std::span<const std::byte> data) {
    if (!valid_object_id(object_id) || chunk.index >= manifest.chunks.size() ||
        manifest.chunks[chunk.index] != chunk || data.size() != chunk.length ||
        integrity::crc32c(data) != chunk.checksum) return StoreError::checksum_mismatch;
    std::vector<std::byte> encoded;
    try { encoded = encode_manifest(manifest); }
    catch (const std::invalid_argument&) { return StoreError::corrupt_object; }

    const auto committed = read_regular_file(object_path(object_id), kMaximumManifestSize);
    if (!committed && std::filesystem::exists(object_path(object_id)))
        return StoreError::corrupt_object;
    if (committed) {
        const auto current = decode_manifest(*committed, max_object_size_);
        if (!current || current->generation_id != manifest.generation_id || *committed != encoded)
            return StoreError::operation_conflict;
    }
    const auto generation = chunks_directory_ / manifest.generation_id;
    std::error_code error;
    std::filesystem::create_directories(generation, error);
    if (error || sync_directory(chunks_directory_) != StoreError::none)
        return StoreError::filesystem_error;
    const auto generation_manifest = generation / "manifest";
    if (const auto existing = read_regular_file(generation_manifest, kMaximumManifestSize)) {
        if (*existing != encoded) return StoreError::operation_conflict;
    } else {
        if (std::filesystem::exists(generation_manifest)) return StoreError::corrupt_object;
        const auto temporary = generation / ".repair-manifest.tmp";
        metrics::io::remove(temporary, error); error.clear();
        if (!write_synced_file(temporary, encoded, should_fail(PutFaultPoint::manifest_write),
                               should_fail(PutFaultPoint::manifest_sync))) {
            metrics::io::remove(temporary, error);
            return StoreError::filesystem_error;
        }
        const auto verified = read_regular_file(temporary, kMaximumManifestSize);
        if (!verified || *verified != encoded) {
            metrics::io::remove(temporary, error);
            return StoreError::corrupt_object;
        }
        if (should_fail(PutFaultPoint::generation_rename)) {
            metrics::io::remove(temporary, error);
            return StoreError::filesystem_error;
        }
        metrics::io::rename(temporary, generation_manifest, error);
        if (error) return StoreError::filesystem_error;
        if (sync_directory(generation) != StoreError::none)
            return StoreError::result_unknown;
    }
    const auto final_chunk = generation / chunk_filename(chunk.index);
    if (const auto existing = read_regular_file(final_chunk, chunk.length)) {
        if (existing->size() != chunk.length || integrity::crc32c(*existing) != chunk.checksum)
            return StoreError::operation_conflict;
    } else {
        if (std::filesystem::exists(final_chunk)) return StoreError::corrupt_object;
        const auto temporary_chunk = generation / (".repair-" + chunk_filename(chunk.index) + ".tmp");
        metrics::io::remove(temporary_chunk, error); error.clear();
        if (!write_synced_file(temporary_chunk, data, should_fail(PutFaultPoint::chunk_write),
                               should_fail(PutFaultPoint::chunk_sync))) {
            metrics::io::remove(temporary_chunk, error);
            return StoreError::filesystem_error;
        }
        const auto verified = read_regular_file(temporary_chunk, chunk.length);
        if (!verified || verified->size() != chunk.length ||
            integrity::crc32c(*verified) != chunk.checksum) {
            metrics::io::remove(temporary_chunk, error);
            return StoreError::corrupt_object;
        }
        if (should_fail(PutFaultPoint::generation_rename)) {
            metrics::io::remove(temporary_chunk, error);
            return StoreError::filesystem_error;
        }
        metrics::io::rename(temporary_chunk, final_chunk, error);
        if (error) return StoreError::filesystem_error;
        if (sync_directory(generation) != StoreError::none)
            return StoreError::result_unknown;
    }

    if (!committed) {
        const auto temporary_manifest = objects_directory_ /
            (".repair-" + std::string(object_id) + ".tmp");
        metrics::io::remove(temporary_manifest, error); error.clear();
        if (!write_synced_file(temporary_manifest, encoded,
                should_fail(PutFaultPoint::manifest_write),
                should_fail(PutFaultPoint::manifest_sync))) {
            metrics::io::remove(temporary_manifest, error);
            return StoreError::filesystem_error;
        }
        const auto verified = read_regular_file(temporary_manifest, kMaximumManifestSize);
        if (!verified || *verified != encoded || should_fail(PutFaultPoint::commit_rename)) {
            metrics::io::remove(temporary_manifest, error);
            return StoreError::filesystem_error;
        }
        metrics::io::rename(temporary_manifest, object_path(object_id), error);
        if (error) return StoreError::filesystem_error;
        if (sync_directory(objects_directory_) != StoreError::none)
            return StoreError::result_unknown;
    }
    return StoreError::none;
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
    if (metrics::io::unlink(object_path(object_id).c_str()) != 0) {
        return errno == ENOENT ? StoreError::not_found : StoreError::filesystem_error;
    }
    if (sync_directory(objects_directory_) != StoreError::none) { return StoreError::filesystem_error; }
    std::error_code error;
    metrics::io::remove_all(chunks_directory_ / manifest->generation_id, error);
    if (error || sync_directory(chunks_directory_) != StoreError::none) {
        return StoreError::filesystem_error;
    }
    return StoreError::none;
}

StoreError ObjectStore::remove_generation(std::string_view object_id,
                                          std::string_view generation_id) {
    const auto manifest_bytes = read_regular_file(object_path(object_id), kMaximumManifestSize);
    if (!manifest_bytes) {
        return std::filesystem::exists(object_path(object_id)) ? StoreError::corrupt_object
                                                               : StoreError::not_found;
    }
    const auto manifest = decode_manifest(*manifest_bytes, max_object_size_);
    if (!manifest) return StoreError::corrupt_object;
    if (manifest->generation_id != generation_id) return StoreError::not_found;
    return remove(object_id);
}

const std::filesystem::path& ObjectStore::objects_directory() const noexcept { return objects_directory_; }
const std::filesystem::path& ObjectStore::chunks_directory() const noexcept { return chunks_directory_; }
const std::filesystem::path& ObjectStore::transactions_directory() const noexcept { return transactions_directory_; }
std::uint32_t ObjectStore::chunk_size() const noexcept { return chunk_size_; }

bool ObjectStore::valid_object_id(std::string_view object_id) noexcept {
    return object_protocol::valid_object_id(object_id);
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
    if (should_fail(PutFaultPoint::directory_sync)) return StoreError::filesystem_error;
    FileDescriptor directory(metrics::io::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (!directory.valid()) { return StoreError::filesystem_error; }
    int result;
    do { result = metrics::io::fsync(directory.get()); } while (result < 0 && errno == EINTR);
    return result == 0 ? StoreError::none : StoreError::filesystem_error;
}

void ObjectStore::clean_incomplete_state() {
    metrics::Timer startup(metrics::Counter::startup_calls, metrics::Counter::startup_ns);
    std::error_code error;
    std::vector<std::filesystem::path> removals;
    for (std::filesystem::directory_iterator it(transactions_directory_, error), end;
         !error && it != end; it.increment(error)) { metrics::add(metrics::Counter::startup_entries); removals.push_back(it->path()); }
    for (const auto& path : removals) {
        metrics::io::remove_all(path, error);
        if (error) { break; }
    }
    if (error) { throw std::system_error(error, "clean AtlasFS transactions"); }
    std::set<std::string> referenced;
    removals.clear();
    for (std::filesystem::directory_iterator it(objects_directory_, error), end;
         !error && it != end; it.increment(error)) {
        metrics::add(metrics::Counter::startup_entries);
        if (it->path().filename().string().starts_with(".atlasfs-tmp-") ||
            it->path().filename().string().starts_with(".repair-")) {
            removals.push_back(it->path());
            continue;
        }
        const auto encoded = read_regular_file(it->path(), kMaximumManifestSize);
        const auto manifest = encoded ? decode_manifest(*encoded, max_object_size_) : std::nullopt;
        // An unreadable reference is not evidence that its generation is orphaned.
        // Validate every published manifest before reclaiming any generation data.
        if (!manifest) { throw std::runtime_error("invalid published AtlasFS manifest"); }
        referenced.insert(manifest->generation_id);
    }
    if (error) { throw std::system_error(error, "scan AtlasFS manifests"); }
    for (const auto& path : removals) {
        metrics::io::remove_all(path, error);
        if (error) { break; }
    }
    if (error) { throw std::system_error(error, "scan AtlasFS manifests"); }
    removals.clear();
    for (std::filesystem::directory_iterator it(chunks_directory_, error), end;
         !error && it != end; it.increment(error)) {
        metrics::add(metrics::Counter::startup_entries);
        if (!referenced.contains(it->path().filename().string())) {
            removals.push_back(it->path());
        }
    }
    for (const auto& path : removals) {
        metrics::io::remove_all(path, error);
        if (error) { break; }
    }
    if (error) { throw std::system_error(error, "clean AtlasFS orphan generations"); }
    for (const auto& generation : referenced) {
        removals.clear();
        const auto directory = chunks_directory_ / generation;
        if (!std::filesystem::exists(directory, error)) {
            if (error) break;
            continue;
        }
        for (std::filesystem::directory_iterator it(directory, error), end;
             !error && it != end; it.increment(error)) {
            metrics::add(metrics::Counter::startup_entries);
            if (it->path().filename().string().starts_with(".repair-"))
                removals.push_back(it->path());
        }
        for (const auto& path : removals) {
            metrics::io::remove_all(path, error);
            if (error) break;
        }
        if (error) break;
    }
    if (error) { throw std::system_error(error, "clean AtlasFS repair transactions"); }
}

bool ObjectStore::should_fail(PutFaultPoint point) const {
    return fault_injector_ && fault_injector_(point);
}

std::string ObjectStore::pending_key(std::string_view object_id,
                                     std::string_view generation_id) {
    std::string key(object_id);
    key.push_back('\0');
    key.append(generation_id);
    return key;
}

}  // namespace atlasfs::storage
