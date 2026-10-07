#include "atlasfs/replica_catalog.hpp"

#include "atlasfs/checksum.hpp"
#include "atlasfs/object_protocol.hpp"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <set>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>

namespace atlasfs::metadata {
namespace {

constexpr std::uint32_t kMagic = 0x41525043U;  // ARPC
constexpr std::uint16_t kFormatVersion = 1;
constexpr std::size_t kHeaderSize = 24;
constexpr std::size_t kChecksumSize = 4;

template <class T>
void append(std::vector<std::byte>& output, T value) {
    static_assert(std::is_unsigned_v<T>);
    for (std::size_t index = sizeof(T); index > 0; --index) {
        output.push_back(static_cast<std::byte>(
            (value >> ((index - 1U) * 8U)) & 0xffU));
    }
}

template <class T>
bool take(std::span<const std::byte> input, std::size_t& offset, T& value) {
    static_assert(std::is_unsigned_v<T>);
    if (offset > input.size() || input.size() - offset < sizeof(T)) {
        return false;
    }
    value = 0;
    for (std::size_t index = 0; index < sizeof(T); ++index) {
        value = static_cast<T>((value << 8U) |
                               std::to_integer<unsigned>(input[offset++]));
    }
    return true;
}

void append_string(std::vector<std::byte>& output, std::string_view value) {
    append(output, static_cast<std::uint16_t>(value.size()));
    const auto* begin = reinterpret_cast<const std::byte*>(value.data());
    output.insert(output.end(), begin, begin + value.size());
}

bool take_string(std::span<const std::byte> input, std::size_t& offset,
                 std::size_t limit, std::string& value) {
    std::uint16_t size{};
    if (!take(input, offset, size) || size == 0 || size > limit ||
        offset > input.size() || input.size() - offset < size) {
        return false;
    }
    value.assign(reinterpret_cast<const char*>(input.data() + offset), size);
    offset += size;
    return true;
}

bool identity_less(const ChunkIdentity& left, const ChunkIdentity& right) {
    if (left.object_id != right.object_id) {
        return left.object_id < right.object_id;
    }
    if (left.generation_id != right.generation_id) {
        return left.generation_id < right.generation_id;
    }
    return left.chunk_index < right.chunk_index;
}

bool valid_identity(const ChunkIdentity& identity) {
    return object_protocol::valid_object_id(identity.object_id) &&
           storage::valid_generation_id(identity.generation_id) &&
           identity.chunk_index < storage::kMaxChunkCount;
}

bool valid_state(CatalogReplicaState state) {
    return state == CatalogReplicaState::present ||
           state == CatalogReplicaState::missing ||
           state == CatalogReplicaState::corrupt ||
           state == CatalogReplicaState::unavailable;
}

bool valid_replica(const CatalogReplica& replica,
                   const storage::ChunkMetadata& chunk) {
    if (!NodeRegistry::valid_node_id(replica.node_id) ||
        !valid_state(replica.state) || replica.evidence_version == 0) {
        return false;
    }
    if (replica.state == CatalogReplicaState::present) {
        return replica.integrity_checksum == chunk.checksum;
    }
    if (replica.state == CatalogReplicaState::missing ||
        replica.state == CatalogReplicaState::unavailable) {
        return replica.integrity_checksum == 0;
    }
    return true;
}

bool valid_payload(const ChunkIdentity& identity,
                   const storage::ChunkMetadata& chunk,
                   const std::vector<CatalogReplica>& replicas) {
    if (!valid_identity(identity) || chunk.index != identity.chunk_index ||
        chunk.length == 0 || chunk.length > object_protocol::kMaxObjectDataSize ||
        replicas.size() > kMaxCatalogReplicasPerChunk) {
        return false;
    }
    std::string previous;
    for (const auto& replica : replicas) {
        if (!valid_replica(replica, chunk) ||
            (!previous.empty() && replica.node_id <= previous)) {
            return false;
        }
        previous = replica.node_id;
    }
    return true;
}

bool add_size(std::size_t& total, std::size_t value) {
    if (value > kMaxReplicaCatalogSize - total) {
        return false;
    }
    total += value;
    return true;
}

bool valid_snapshot(const ReplicaCatalogSnapshot& snapshot) {
    if (snapshot.records.size() > kMaxReplicaCatalogRecords) {
        return false;
    }
    std::size_t encoded_size = kHeaderSize + kChecksumSize;
    std::set<std::uint64_t> live_versions;
    const ReplicaCatalogRecord* previous = nullptr;
    for (const auto& record : snapshot.records) {
        if (!valid_payload(record.identity, record.chunk, record.replicas) ||
            record.catalog_version == 0 ||
            record.catalog_version > snapshot.catalog_version ||
            !live_versions.insert(record.catalog_version).second ||
            (previous != nullptr &&
             !identity_less(previous->identity, record.identity))) {
            return false;
        }
        previous = &record;
        if (!add_size(encoded_size, 28U + record.identity.object_id.size() +
                                       record.identity.generation_id.size())) {
            return false;
        }
        for (const auto& replica : record.replicas) {
            if (!add_size(encoded_size, 18U + replica.node_id.size())) {
                return false;
            }
        }
    }
    return snapshot.records.empty() || snapshot.catalog_version != 0;
}

bool sync_fd(int descriptor) {
    int result;
    do {
        result = ::fsync(descriptor);
    } while (result < 0 && errno == EINTR);
    return result == 0;
}

bool sync_directory(const std::filesystem::path& path) {
    const int descriptor =
        ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (descriptor < 0) {
        return false;
    }
    const bool synced = sync_fd(descriptor);
    const bool closed = ::close(descriptor) == 0;
    return synced && closed;
}

auto find_record(std::vector<ReplicaCatalogRecord>& records,
                 const ChunkIdentity& identity) {
    return std::lower_bound(
        records.begin(), records.end(), identity,
        [](const ReplicaCatalogRecord& record, const ChunkIdentity& key) {
            return identity_less(record.identity, key);
        });
}

auto find_record(const std::vector<ReplicaCatalogRecord>& records,
                 const ChunkIdentity& identity) {
    return std::lower_bound(
        records.begin(), records.end(), identity,
        [](const ReplicaCatalogRecord& record, const ChunkIdentity& key) {
            return identity_less(record.identity, key);
        });
}

}  // namespace

std::vector<std::byte> encode_replica_catalog(
    const ReplicaCatalogSnapshot& snapshot) {
    if (!valid_snapshot(snapshot)) {
        throw std::invalid_argument("invalid replica catalog snapshot");
    }
    std::vector<std::byte> output;
    output.reserve(kHeaderSize + kChecksumSize);
    append(output, kMagic);
    append(output, kFormatVersion);
    append(output, std::uint16_t{0});
    append(output, snapshot.catalog_version);
    append(output, static_cast<std::uint32_t>(snapshot.records.size()));
    append(output, std::uint32_t{0});
    for (const auto& record : snapshot.records) {
        append_string(output, record.identity.object_id);
        append_string(output, record.identity.generation_id);
        append(output, record.identity.chunk_index);
        append(output, record.chunk.length);
        append(output, record.chunk.checksum);
        append(output, record.catalog_version);
        append(output, static_cast<std::uint32_t>(record.replicas.size()));
        for (const auto& replica : record.replicas) {
            append_string(output, replica.node_id);
            append(output, static_cast<std::uint8_t>(replica.state));
            append(output, std::uint8_t{0});
            append(output, std::uint16_t{0});
            append(output, replica.integrity_checksum);
            append(output, replica.evidence_version);
        }
    }
    append(output, integrity::crc32c(output));
    return output;
}

std::optional<ReplicaCatalogSnapshot> decode_replica_catalog(
    std::span<const std::byte> encoded) {
    if (encoded.size() < kHeaderSize + kChecksumSize ||
        encoded.size() > kMaxReplicaCatalogSize) {
        return std::nullopt;
    }
    const auto body = encoded.first(encoded.size() - kChecksumSize);
    std::size_t checksum_offset = body.size();
    std::uint32_t stored_checksum{};
    if (!take(encoded, checksum_offset, stored_checksum) ||
        stored_checksum != integrity::crc32c(body)) {
        return std::nullopt;
    }

    ReplicaCatalogSnapshot snapshot;
    std::size_t offset = 0;
    std::uint32_t magic{};
    std::uint16_t version{};
    std::uint16_t reserved16{};
    std::uint32_t count{};
    std::uint32_t reserved32{};
    if (!take(body, offset, magic) || magic != kMagic ||
        !take(body, offset, version) || version != kFormatVersion ||
        !take(body, offset, reserved16) || reserved16 != 0 ||
        !take(body, offset, snapshot.catalog_version) ||
        !take(body, offset, count) || count > kMaxReplicaCatalogRecords ||
        !take(body, offset, reserved32) || reserved32 != 0) {
        return std::nullopt;
    }
    snapshot.records.reserve(count);
    for (std::uint32_t record_index = 0; record_index < count;
         ++record_index) {
        ReplicaCatalogRecord record;
        std::uint32_t replica_count{};
        if (!take_string(body, offset, object_protocol::kMaxObjectIdSize,
                         record.identity.object_id) ||
            !take_string(body, offset, storage::kMaxGenerationIdSize,
                         record.identity.generation_id) ||
            !take(body, offset, record.identity.chunk_index) ||
            !take(body, offset, record.chunk.length) ||
            !take(body, offset, record.chunk.checksum) ||
            !take(body, offset, record.catalog_version) ||
            !take(body, offset, replica_count) ||
            replica_count > kMaxCatalogReplicasPerChunk) {
            return std::nullopt;
        }
        record.chunk.index = record.identity.chunk_index;
        record.replicas.reserve(replica_count);
        for (std::uint32_t replica_index = 0; replica_index < replica_count;
             ++replica_index) {
            CatalogReplica replica;
            std::uint8_t state{};
            std::uint8_t reserved8{};
            if (!take_string(body, offset, kMaxNodeIdSize, replica.node_id) ||
                !take(body, offset, state) ||
                !take(body, offset, reserved8) || reserved8 != 0 ||
                !take(body, offset, reserved16) || reserved16 != 0 ||
                !take(body, offset, replica.integrity_checksum) ||
                !take(body, offset, replica.evidence_version)) {
                return std::nullopt;
            }
            replica.state = static_cast<CatalogReplicaState>(state);
            record.replicas.push_back(std::move(replica));
        }
        snapshot.records.push_back(std::move(record));
    }
    if (offset != body.size() || !valid_snapshot(snapshot)) {
        return std::nullopt;
    }
    return snapshot;
}

DurableReplicaCatalog::DurableReplicaCatalog(
    std::filesystem::path root,
    ReplicaCatalogFaultInjector fault_injector)
    : directory_(std::move(root) / "coordinator" / "replicas"),
      record_path_(directory_ / "catalog"),
      fault_injector_(std::move(fault_injector)) {
    std::error_code error;
    std::vector<std::filesystem::path> missing_directories;
    for (auto path = directory_; !path.empty();) {
        const bool exists = std::filesystem::exists(path, error);
        if (error) {
            throw std::system_error(error, "inspect replica catalog directory");
        }
        if (exists) {
            break;
        }
        missing_directories.push_back(path);
        const auto parent = path.parent_path();
        if (parent == path) {
            break;
        }
        path = parent;
    }
    std::filesystem::create_directories(directory_, error);
    if (error) {
        throw std::system_error(error, "create replica catalog directory");
    }
    for (const auto& path : missing_directories) {
        if (!sync_directory(path)) {
            throw std::runtime_error("synchronize replica catalog directory");
        }
        const auto parent = path.parent_path();
        if (!parent.empty() && !sync_directory(parent)) {
            throw std::runtime_error("synchronize replica catalog parent directory");
        }
    }
    const bool exists = std::filesystem::exists(record_path_, error);
    if (error) {
        throw std::system_error(error, "inspect replica catalog");
    }
    if (!exists) {
        return;
    }
    const auto size = std::filesystem::file_size(record_path_, error);
    if (error) {
        throw std::system_error(error, "size replica catalog");
    }
    if (size > kMaxReplicaCatalogSize) {
        throw std::runtime_error("invalid durable replica catalog");
    }
    std::ifstream input(record_path_, std::ios::binary);
    if (!input) {
        throw std::runtime_error("read durable replica catalog");
    }
    std::vector<std::byte> bytes(static_cast<std::size_t>(size));
    if (!bytes.empty()) {
        input.read(reinterpret_cast<char*>(bytes.data()),
                   static_cast<std::streamsize>(bytes.size()));
    }
    if (!input || input.peek() != std::char_traits<char>::eof()) {
        throw std::runtime_error("read durable replica catalog");
    }
    const auto decoded = decode_replica_catalog(bytes);
    if (!decoded) {
        throw std::runtime_error("invalid durable replica catalog");
    }
    snapshot_ = *decoded;
}

ReplicaCatalogError DurableReplicaCatalog::upsert(
    ChunkIdentity identity,
    storage::ChunkMetadata chunk,
    std::vector<CatalogReplica> replicas) {
    if (!valid_payload(identity, chunk, replicas)) {
        return ReplicaCatalogError::invalid_record;
    }
    const std::scoped_lock lock(mutex_);
    if (durability_unknown_) {
        return ReplicaCatalogError::durability_unknown;
    }
    const auto found = find_record(snapshot_.records, identity);
    if (found != snapshot_.records.end() && found->identity == identity) {
        if (found->chunk != chunk) {
            return ReplicaCatalogError::conflict;
        }
        if (found->replicas == replicas) {
            return ReplicaCatalogError::none;
        }
    }
    if (snapshot_.catalog_version == std::numeric_limits<std::uint64_t>::max()) {
        return ReplicaCatalogError::version_exhausted;
    }

    ReplicaCatalogSnapshot candidate = snapshot_;
    const auto next_version = snapshot_.catalog_version + 1U;
    const auto candidate_found = find_record(candidate.records, identity);
    ReplicaCatalogRecord record{std::move(identity), chunk, next_version,
                                std::move(replicas)};
    if (candidate_found != candidate.records.end() &&
        candidate_found->identity == record.identity) {
        *candidate_found = std::move(record);
    } else {
        candidate.records.insert(candidate_found, std::move(record));
    }
    candidate.catalog_version = next_version;
    return publish(std::move(candidate));
}

ReplicaCatalogError DurableReplicaCatalog::erase(const ChunkIdentity& identity) {
    if (!valid_identity(identity)) {
        return ReplicaCatalogError::invalid_record;
    }
    const std::scoped_lock lock(mutex_);
    if (durability_unknown_) {
        return ReplicaCatalogError::durability_unknown;
    }
    const auto found = find_record(snapshot_.records, identity);
    if (found == snapshot_.records.end() || found->identity != identity) {
        return ReplicaCatalogError::not_found;
    }
    if (snapshot_.catalog_version == std::numeric_limits<std::uint64_t>::max()) {
        return ReplicaCatalogError::version_exhausted;
    }
    ReplicaCatalogSnapshot candidate = snapshot_;
    const auto candidate_found = find_record(candidate.records, identity);
    candidate.records.erase(candidate_found);
    ++candidate.catalog_version;
    return publish(std::move(candidate));
}

ReplicaCatalogCasResult DurableReplicaCatalog::compare_exchange(
    ReplicaCatalogCompareExchange request) {
    if (!valid_identity(request.identity) ||
        request.expected_catalog_version == 0) {
        return {ReplicaCatalogCasStatus::invalid_request, 0, std::nullopt};
    }
    const std::scoped_lock lock(mutex_);
    auto result = [&](ReplicaCatalogCasStatus status) {
        const auto current = find_record(snapshot_.records, request.identity);
        std::optional<ReplicaCatalogRecord> record;
        if (current != snapshot_.records.end() &&
            current->identity == request.identity) {
            record = *current;
        }
        return ReplicaCatalogCasResult{status, snapshot_.catalog_version,
                                       std::move(record)};
    };
    if (durability_unknown_) {
        return result(ReplicaCatalogCasStatus::durability_unknown);
    }
    const auto found = find_record(snapshot_.records, request.identity);
    if (found == snapshot_.records.end() || found->identity != request.identity) {
        return result(ReplicaCatalogCasStatus::not_found);
    }
    if (!valid_payload(request.identity, found->chunk,
                       request.expected_replicas) ||
        !valid_payload(request.identity, found->chunk,
                       request.desired_replicas)) {
        return result(ReplicaCatalogCasStatus::invalid_request);
    }
    if (snapshot_.catalog_version != request.expected_catalog_version ||
        found->replicas != request.expected_replicas) {
        return result(ReplicaCatalogCasStatus::conflict);
    }
    if (found->replicas == request.desired_replicas) {
        return result(ReplicaCatalogCasStatus::no_change);
    }
    if (snapshot_.catalog_version == std::numeric_limits<std::uint64_t>::max()) {
        return result(ReplicaCatalogCasStatus::version_exhausted);
    }

    ReplicaCatalogSnapshot candidate = snapshot_;
    const auto next_version = candidate.catalog_version + 1U;
    const auto candidate_found = find_record(candidate.records, request.identity);
    candidate_found->replicas = std::move(request.desired_replicas);
    candidate_found->catalog_version = next_version;
    candidate.catalog_version = next_version;
    switch (publish(std::move(candidate))) {
        case ReplicaCatalogError::none:
            return result(ReplicaCatalogCasStatus::committed);
        case ReplicaCatalogError::durability_unknown:
            return result(ReplicaCatalogCasStatus::durability_unknown);
        case ReplicaCatalogError::version_exhausted:
            return result(ReplicaCatalogCasStatus::version_exhausted);
        case ReplicaCatalogError::invalid_record:
            return result(ReplicaCatalogCasStatus::invalid_request);
        case ReplicaCatalogError::persistence_error:
        case ReplicaCatalogError::conflict:
        case ReplicaCatalogError::not_found:
            return result(ReplicaCatalogCasStatus::persistence_error);
    }
    return result(ReplicaCatalogCasStatus::persistence_error);
}

std::optional<ReplicaCatalogRecord> DurableReplicaCatalog::get(
    const ChunkIdentity& identity) const {
    const std::scoped_lock lock(mutex_);
    const auto found = find_record(snapshot_.records, identity);
    return found == snapshot_.records.end() || found->identity != identity
               ? std::optional<ReplicaCatalogRecord>{}
               : std::optional<ReplicaCatalogRecord>{*found};
}

std::vector<ReplicaCatalogRecord> DurableReplicaCatalog::list() const {
    const std::scoped_lock lock(mutex_);
    return snapshot_.records;
}

ReplicaCatalogSnapshot DurableReplicaCatalog::snapshot() const {
    const std::scoped_lock lock(mutex_);
    return snapshot_;
}

std::uint64_t DurableReplicaCatalog::version() const {
    const std::scoped_lock lock(mutex_);
    return snapshot_.catalog_version;
}

bool DurableReplicaCatalog::durability_unknown() const {
    const std::scoped_lock lock(mutex_);
    return durability_unknown_;
}

const std::filesystem::path& DurableReplicaCatalog::record_path() const noexcept {
    return record_path_;
}

ReplicaCatalogError DurableReplicaCatalog::publish(
    ReplicaCatalogSnapshot candidate) {
    std::vector<std::byte> bytes;
    try {
        bytes = encode_replica_catalog(candidate);
    } catch (...) {
        return ReplicaCatalogError::invalid_record;
    }
    if (fault_injector_ &&
        fault_injector_(ReplicaCatalogFaultPoint::record_creation)) {
        return ReplicaCatalogError::persistence_error;
    }
    const auto temporary = directory_ / "catalog.tmp";
    const int descriptor = ::open(
        temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (descriptor < 0) {
        return ReplicaCatalogError::persistence_error;
    }
    bool okay = !(fault_injector_ &&
                  fault_injector_(ReplicaCatalogFaultPoint::record_write));
    std::size_t offset = 0;
    while (okay && offset < bytes.size()) {
        const auto written =
            ::write(descriptor, bytes.data() + offset, bytes.size() - offset);
        if (written > 0) {
            offset += static_cast<std::size_t>(written);
        } else if (written < 0 && errno == EINTR) {
            continue;
        } else {
            okay = false;
        }
    }
    if (fault_injector_ &&
        fault_injector_(ReplicaCatalogFaultPoint::record_sync)) {
        okay = false;
    }
    if (okay) {
        okay = sync_fd(descriptor);
    }
    if (::close(descriptor) != 0) {
        okay = false;
    }
    if (!okay) {
        return ReplicaCatalogError::persistence_error;
    }
    if (fault_injector_ &&
        fault_injector_(ReplicaCatalogFaultPoint::record_rename)) {
        return ReplicaCatalogError::persistence_error;
    }
    std::error_code error;
    std::filesystem::rename(temporary, record_path_, error);
    if (error) {
        return ReplicaCatalogError::persistence_error;
    }

    snapshot_ = std::move(candidate);
    if (fault_injector_ &&
        fault_injector_(ReplicaCatalogFaultPoint::directory_sync)) {
        durability_unknown_ = true;
        return ReplicaCatalogError::durability_unknown;
    }
    if (!sync_directory(directory_)) {
        durability_unknown_ = true;
        return ReplicaCatalogError::durability_unknown;
    }
    durability_unknown_ = false;
    return ReplicaCatalogError::none;
}

}  // namespace atlasfs::metadata
