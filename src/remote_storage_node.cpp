#include "atlasfs/remote_storage_node.hpp"

#include "atlasfs/checksum.hpp"

#include <fstream>
#include <fcntl.h>
#include <stdexcept>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace atlasfs::replication {
namespace snp = storage_node_protocol;
namespace {

net::TlsConfig storage_tls(const std::shared_ptr<StorageNode>& node, net::TlsConfig&& config) {
    if (!node || config.local_identity != net::PeerIdentity{std::string(node->id()), net::PeerRole::storage})
        throw std::invalid_argument("TLS storage endpoint identity mismatch");
    return std::move(config);
}

snp::RepairResponseStatus wire_status(RepairPublishStatus status) {
    switch (status) {
        case RepairPublishStatus::published:
            return snp::RepairResponseStatus::published;
        case RepairPublishStatus::already_present_valid:
            return snp::RepairResponseStatus::already_present_valid;
        case RepairPublishStatus::identity_mismatch:
            return snp::RepairResponseStatus::identity_mismatch;
        case RepairPublishStatus::invalid_manifest:
            return snp::RepairResponseStatus::invalid_manifest;
        case RepairPublishStatus::checksum_mismatch:
            return snp::RepairResponseStatus::checksum_mismatch;
        case RepairPublishStatus::publication_failure:
            return snp::RepairResponseStatus::publication_failure;
        case RepairPublishStatus::result_unknown:
            return snp::RepairResponseStatus::result_unknown;
        case RepairPublishStatus::connection_failure:
        case RepairPublishStatus::timeout:
        case RepairPublishStatus::malformed_response:
        case RepairPublishStatus::target_rejected:
            return snp::RepairResponseStatus::target_rejected;
    }
    return snp::RepairResponseStatus::target_rejected;
}

RepairPublishResult client_status(snp::RepairResponseStatus status) {
    switch (status) {
        case snp::RepairResponseStatus::published:
            return {RepairPublishStatus::published, storage::StoreError::none};
        case snp::RepairResponseStatus::already_present_valid:
            return {RepairPublishStatus::already_present_valid, storage::StoreError::none};
        case snp::RepairResponseStatus::identity_mismatch:
            return {RepairPublishStatus::identity_mismatch,
                    storage::StoreError::operation_conflict};
        case snp::RepairResponseStatus::target_rejected:
            return {RepairPublishStatus::target_rejected,
                    storage::StoreError::operation_conflict};
        case snp::RepairResponseStatus::invalid_manifest:
            return {RepairPublishStatus::invalid_manifest,
                    storage::StoreError::corrupt_object};
        case snp::RepairResponseStatus::checksum_mismatch:
            return {RepairPublishStatus::checksum_mismatch,
                    storage::StoreError::checksum_mismatch};
        case snp::RepairResponseStatus::publication_failure:
            return {RepairPublishStatus::publication_failure,
                    storage::StoreError::filesystem_error};
        case snp::RepairResponseStatus::result_unknown:
            return {RepairPublishStatus::result_unknown,
                    storage::StoreError::result_unknown};
    }
    return {RepairPublishStatus::malformed_response,
            storage::StoreError::corrupt_object};
}

}  // namespace

RemoteStorageNodeClient::RemoteStorageNodeClient(std::string node_id, std::uint16_t port)
    : node_id_(std::move(node_id)), client_(port) {
    if (!metadata::NodeRegistry::valid_node_id(node_id_))
        throw std::invalid_argument("remote storage node requires valid id");
}

std::string_view RemoteStorageNodeClient::id() const noexcept { return node_id_; }

RemoteStorageNodeClient::RemoteStorageNodeClient(std::string node_id, std::uint16_t port,
                                               net::TlsConfig config)
    : node_id_(std::move(node_id)),
      client_(port, std::move(config), {node_id_, net::PeerRole::storage}) {
    if (!metadata::NodeRegistry::valid_node_id(node_id_))
        throw std::invalid_argument("remote storage node requires valid id");
}

snp::Response RemoteStorageNodeClient::exchange(protocol::MessageType request_type,
    protocol::MessageType response_type, std::vector<std::byte> payload, bool commit) const {
    const auto request_id = next_request_id_++;
    try {
        const auto response = client_.request({request_type, 0, request_id, std::move(payload)});
        if (response.type != response_type) throw std::runtime_error("wrong storage response type");
        const auto decoded = snp::decode_response(response.payload);
        if (!decoded) throw std::runtime_error("malformed storage response");
        return *decoded;
    } catch (...) {
        if (commit) return {storage::StoreError::result_unknown, "", snp::OperationState::unknown, 0, {}};
        return {storage::StoreError::filesystem_error, "", snp::OperationState::unknown, 0, {}};
    }
}

storage::StoreError RemoteStorageNodeClient::prepare(std::string_view operation_id,
    std::string_view object_id,
    const storage::ObjectManifest& manifest, std::span<const std::uint32_t> indices,
    std::span<const std::byte> data) {
    snp::PrepareRequest request{std::string(operation_id), std::string(object_id),
        manifest, {indices.begin(), indices.end()}, {data.begin(), data.end()}};
    const auto response = exchange(protocol::MessageType::storage_prepare_request,
        protocol::MessageType::storage_prepare_response, snp::encode_prepare(request));
    if (response.error == storage::StoreError::none && response.operation_id != request.operation_id)
        return storage::StoreError::operation_conflict;
    return response.error;
}

storage::StoreError RemoteStorageNodeClient::verify_prepared(std::string_view operation_id,
    std::string_view, std::string_view, std::span<const std::uint32_t>) const {
    auto* self = const_cast<RemoteStorageNodeClient*>(this);
    return self->query(operation_id) == snp::OperationState::prepared
        ? storage::StoreError::none : storage::StoreError::invalid_operation_state;
}

storage::StoreError RemoteStorageNodeClient::commit(std::string_view operation_id,
    std::string_view object_id,
    std::string_view generation_id) {
    const auto op = std::string(operation_id);
    auto response = exchange(protocol::MessageType::storage_commit_request,
        protocol::MessageType::storage_commit_response,
        snp::encode_identity({op, std::string(object_id), std::string(generation_id)}), true);
    if (response.error == storage::StoreError::none && response.operation_id != op)
        return storage::StoreError::operation_conflict;
    return response.error;
}

storage::StoreError RemoteStorageNodeClient::abort(std::string_view operation_id,
    std::string_view object_id,
    std::string_view generation_id) {
    const auto op = std::string(operation_id);
    const auto response = exchange(protocol::MessageType::storage_abort_request,
        protocol::MessageType::storage_abort_response,
        snp::encode_identity({op, std::string(object_id), std::string(generation_id)}));
    if (response.error == storage::StoreError::none && response.operation_id != op)
        return storage::StoreError::operation_conflict;
    return response.error;
}

storage::StoreError RemoteStorageNodeClient::finalize(std::string_view, std::string_view,
                                                       std::string_view) {
    return storage::StoreError::none;
}

storage::ChunkResult RemoteStorageNodeClient::get_chunk(std::string_view object_id,
    std::string_view generation_id, std::uint32_t index) const {
    auto response = exchange(protocol::MessageType::storage_get_chunk_request,
        protocol::MessageType::storage_get_chunk_response,
        snp::encode_get_chunk({std::string(object_id), std::string(generation_id), index}));
    response.chunk.error = response.error;
    return std::move(response.chunk);
}

storage::StoreError RemoteStorageNodeClient::remove(std::string_view object_id,
                                                    std::string_view generation_id) {
    std::string op = "delete-" + std::string(object_id);
    if (op.size() > snp::kMaxOperationIdSize) op = "delete-request";
    return exchange(protocol::MessageType::storage_delete_request,
        protocol::MessageType::storage_delete_response,
        snp::encode_identity({op, std::string(object_id), std::string(generation_id)})).error;
}

snp::OperationState RemoteStorageNodeClient::query(std::string_view operation_id_value) {
    return query_operation(operation_id_value).state;
}

OperationQueryResult RemoteStorageNodeClient::query_operation(
    std::string_view operation_id_value) const {
    const auto response = exchange(protocol::MessageType::storage_query_request,
        protocol::MessageType::storage_query_response,
        snp::encode_identity({std::string(operation_id_value), "query", "generation"}));
    if (response.error != storage::StoreError::none)
        return {response.error, std::string(operation_id_value), OperationState::unknown, 0};
    if (response.operation_id != operation_id_value)
        return {storage::StoreError::operation_conflict, response.operation_id,
                OperationState::unknown, response.fingerprint};
    return {storage::StoreError::none, response.operation_id, response.state,
            response.fingerprint};
}

RepairPublishResult RemoteStorageNodeClient::publish_repair(
    std::string_view plan_id, std::string_view execution_id,
    std::string_view target_node_id, std::string_view object_id,
    const storage::ObjectManifest& manifest, const storage::ChunkMetadata& chunk,
    std::span<const std::byte> data) {
    snp::RepairChunkRequest request{std::string(plan_id), std::string(execution_id),
        std::string(target_node_id), std::string(object_id), manifest, chunk,
        {data.begin(), data.end()}};
    std::vector<std::byte> payload;
    try {
        payload = snp::encode_repair_chunk(request);
    } catch (...) {
        return {RepairPublishStatus::target_rejected,
                storage::StoreError::corrupt_object};
    }
    const auto fingerprint = integrity::crc32c(payload);
    const auto request_id = next_request_id_++;
    try {
        const auto response = client_.request({
            protocol::MessageType::storage_repair_chunk_request, 0, request_id,
            std::move(payload)});
        if (response.type != protocol::MessageType::storage_repair_chunk_response) {
            return {RepairPublishStatus::result_unknown,
                    storage::StoreError::result_unknown};
        }
        const auto decoded = snp::decode_repair_response(response.payload);
        if (!decoded || decoded->plan_id != plan_id ||
            decoded->execution_id != execution_id ||
            decoded->request_fingerprint != fingerprint) {
            return {RepairPublishStatus::result_unknown,
                    storage::StoreError::result_unknown};
        }
        return client_status(decoded->status);
    } catch (...) {
        // The request may have reached the node, so transport failure after send
        // is deliberately ambiguous and is never retried here.
        return {RepairPublishStatus::result_unknown,
                storage::StoreError::result_unknown};
    }
}

StorageNodeServer::StorageNodeServer(std::shared_ptr<StorageNode> node,
    std::filesystem::path journal_root, std::uint16_t port,
    bool drop_commit_response_once, bool drop_repair_response_once)
    : node_(std::move(node)), operations_directory_(journal_root / "operations"),
      repairs_directory_(std::move(journal_root) / "repairs"),
      drop_commit_response_once_(drop_commit_response_once),
      drop_repair_response_once_(drop_repair_response_once),
      server_(port, [this](const protocol::Message& request) { return handle(request); }) {
    if (!node_) throw std::invalid_argument("storage server requires node");
    std::filesystem::create_directories(operations_directory_);
    std::filesystem::create_directories(repairs_directory_);
    load_records();
    load_repair_records();
}

std::uint16_t StorageNodeServer::port() const noexcept { return server_.port(); }
StorageNodeServer::StorageNodeServer(std::shared_ptr<StorageNode> node,
    std::filesystem::path journal_root, net::TlsConfig config, std::uint16_t port,
    bool drop_commit_response_once, bool drop_repair_response_once)
    : node_(std::move(node)), operations_directory_(journal_root / "operations"),
      repairs_directory_(std::move(journal_root) / "repairs"),
      drop_commit_response_once_(drop_commit_response_once),
      drop_repair_response_once_(drop_repair_response_once),
      server_(storage_tls(node_, std::move(config)), port,
          [this](const protocol::Message& request, const net::PeerPolicy& peer) {
              if (!net::authorize_storage(peer, node_->id(), request.type))
                  return protocol::Message{protocol::MessageType::error_response, 0,
                      request.request_id, {}};
              return handle(request);
          }) {
    std::filesystem::create_directories(operations_directory_);
    std::filesystem::create_directories(repairs_directory_);
    load_records();
    load_repair_records();
}
void StorageNodeServer::serve() { server_.serve(); }
void StorageNodeServer::stop() noexcept { server_.stop(); }

void StorageNodeServer::load_records() {
    std::error_code error;
    for (const auto& entry : std::filesystem::directory_iterator(operations_directory_, error)) {
        std::ifstream input(entry.path()); unsigned state{}; Record record{};
        if (input >> state >> record.fingerprint >> record.probe_chunk >>
                record.object_id >> record.generation_id &&
            state <= static_cast<unsigned>(snp::OperationState::aborted)) {
            record.state = static_cast<snp::OperationState>(state);
            if (record.state == snp::OperationState::prepared) {
                const auto visible = node_->get_chunk(record.object_id,
                                                       record.generation_id,
                                                       record.probe_chunk);
                record.state = visible.error == storage::StoreError::not_found
                    ? snp::OperationState::aborted : snp::OperationState::committed;
                static_cast<void>(persist(entry.path().filename().string(), record));
            }
            records_[entry.path().filename().string()] = std::move(record);
        }
    }
}

void StorageNodeServer::load_repair_records() {
    std::error_code error;
    for (const auto& entry : std::filesystem::directory_iterator(repairs_directory_, error)) {
        if (entry.path().extension() == ".tmp") continue;
        std::ifstream input(entry.path());
        unsigned status{};
        RepairRecord record{};
        const auto execution_id = entry.path().filename().string();
        const bool parsed = static_cast<bool>(input >> status >> record.fingerprint >>
            record.chunk_index >> record.plan_id >> record.object_id >>
            record.generation_id);
        input >> std::ws;
        if (!entry.is_regular_file() || !parsed || !input.eof() ||
            status > static_cast<unsigned>(snp::RepairResponseStatus::result_unknown) ||
            !snp::valid_operation_id(execution_id) ||
            !snp::valid_operation_id(record.plan_id) ||
            !storage::ObjectStore::valid_object_id(record.object_id) ||
            !storage::valid_generation_id(record.generation_id) ||
            record.chunk_index >= storage::kMaxChunkCount) {
            throw std::runtime_error("invalid durable repair publication record");
        }
        record.status = static_cast<snp::RepairResponseStatus>(status);
        if (!repair_records_.emplace(execution_id, std::move(record)).second)
            throw std::runtime_error("duplicate durable repair publication record");
    }
    if (error) throw std::system_error(error, "scan repair publication records");
}

bool StorageNodeServer::persist(std::string_view operation_id, const Record& record) {
    const auto temporary = operations_directory_ / (std::string(operation_id) + ".tmp");
    const auto final = operations_directory_ / std::string(operation_id);
    std::ofstream output(temporary, std::ios::trunc);
    output << static_cast<unsigned>(record.state) << ' ' << record.fingerprint << ' '
           << record.probe_chunk << ' '
           << record.object_id << ' ' << record.generation_id << '\n';
    output.flush();
    if (!output) return false;
    output.close();
    const int file = ::open(temporary.c_str(), O_RDWR | O_CLOEXEC);
    if (file < 0 || ::fdatasync(file) != 0) {
        if (file >= 0) ::close(file);
        return false;
    }
    if (::close(file) != 0) return false;
    std::error_code error; std::filesystem::rename(temporary, final, error);
    if (error) return false;
    const int directory = ::open(operations_directory_.c_str(),
                                 O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0 || ::fsync(directory) != 0) {
        if (directory >= 0) ::close(directory);
        return false;
    }
    return ::close(directory) == 0;
}

bool StorageNodeServer::persist_repair(
    std::string_view execution_id, const RepairRecord& record) {
    const auto temporary = repairs_directory_ / (std::string(execution_id) + ".tmp");
    const auto final = repairs_directory_ / std::string(execution_id);
    std::ofstream output(temporary, std::ios::trunc);
    output << static_cast<unsigned>(record.status) << ' ' << record.fingerprint << ' '
           << record.chunk_index << ' ' << record.plan_id << ' ' << record.object_id
           << ' ' << record.generation_id << '\n';
    output.flush();
    if (!output) return false;
    output.close();
    const int file = ::open(temporary.c_str(), O_RDWR | O_CLOEXEC);
    if (file < 0 || ::fdatasync(file) != 0) {
        if (file >= 0) ::close(file);
        return false;
    }
    if (::close(file) != 0) return false;
    std::error_code error;
    std::filesystem::rename(temporary, final, error);
    if (error) return false;
    const int directory = ::open(repairs_directory_.c_str(),
                                 O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0 || ::fsync(directory) != 0) {
        if (directory >= 0) ::close(directory);
        return false;
    }
    return ::close(directory) == 0;
}

protocol::Message StorageNodeServer::handle(const protocol::Message& request) {
    auto reply = [&](protocol::MessageType type, const snp::Response& response) {
        return protocol::Message{type, 0, request.request_id, snp::encode_response(response)};
    };
    auto repair_reply = [&](const snp::RepairResponse& response) {
        return protocol::Message{protocol::MessageType::storage_repair_chunk_response,
            0, request.request_id, snp::encode_repair_response(response)};
    };
    if (request.type == protocol::MessageType::storage_repair_chunk_request) {
        const auto fingerprint = integrity::crc32c(request.payload);
        const auto decoded = snp::decode_repair_chunk(request.payload);
        if (!decoded) {
            return repair_reply({snp::RepairResponseStatus::target_rejected,
                                 "-", "-", fingerprint});
        }
        if (decoded->target_node_id != node_->id()) {
            return repair_reply({snp::RepairResponseStatus::identity_mismatch,
                decoded->plan_id, decoded->execution_id, fingerprint});
        }
        if (const auto found = repair_records_.find(decoded->execution_id);
            found != repair_records_.end()) {
            const auto& record = found->second;
            const bool same = record.fingerprint == fingerprint &&
                record.plan_id == decoded->plan_id &&
                record.object_id == decoded->object_id &&
                record.generation_id == decoded->manifest.generation_id &&
                record.chunk_index == decoded->chunk.index;
            auto status = same ? record.status
                : snp::RepairResponseStatus::target_rejected;
            if (same && (status == snp::RepairResponseStatus::published ||
                         status == snp::RepairResponseStatus::already_present_valid)) {
                const auto visible = node_->get_chunk(decoded->object_id,
                    decoded->manifest.generation_id, decoded->chunk.index);
                if (visible.error != storage::StoreError::none ||
                    visible.manifest != decoded->manifest ||
                    visible.chunk != decoded->chunk || visible.data != decoded->data) {
                    status = snp::RepairResponseStatus::target_rejected;
                }
            }
            return repair_reply({status, decoded->plan_id,
                                 decoded->execution_id, fingerprint});
        }
        auto status = wire_status(node_->publish_repair(
            decoded->plan_id, decoded->execution_id, decoded->target_node_id,
            decoded->object_id, decoded->manifest, decoded->chunk, decoded->data).status);
        RepairRecord record{status, fingerprint, decoded->chunk.index,
            decoded->plan_id, decoded->object_id, decoded->manifest.generation_id};
        if (!persist_repair(decoded->execution_id, record)) {
            if (status == snp::RepairResponseStatus::published ||
                status == snp::RepairResponseStatus::already_present_valid) {
                status = snp::RepairResponseStatus::result_unknown;
            }
        } else {
            repair_records_[decoded->execution_id] = record;
        }
        if (drop_repair_response_once_ &&
            (status == snp::RepairResponseStatus::published ||
             status == snp::RepairResponseStatus::already_present_valid)) {
            drop_repair_response_once_ = false;
            throw std::runtime_error("injected lost repair response");
        }
        return repair_reply({status, decoded->plan_id,
                             decoded->execution_id, fingerprint});
    }
    if (request.type == protocol::MessageType::storage_prepare_request) {
        const auto decoded = snp::decode_prepare(request.payload);
        if (!decoded) return reply(protocol::MessageType::storage_prepare_response,
            {storage::StoreError::corrupt_object, "-", snp::OperationState::unknown, 0, {}});
        const auto fingerprint = integrity::crc32c(request.payload);
        auto found = records_.find(decoded->operation_id);
        if (found != records_.end()) {
            const bool same = found->second.fingerprint == fingerprint &&
                found->second.object_id == decoded->object_id &&
                found->second.generation_id == decoded->manifest.generation_id;
            const auto error = !same ? storage::StoreError::operation_conflict :
                found->second.state == snp::OperationState::prepared ? storage::StoreError::none :
                storage::StoreError::invalid_operation_state;
            return reply(protocol::MessageType::storage_prepare_response,
                {error, decoded->operation_id, found->second.state,
                 found->second.fingerprint, {}});
        }
        auto error = node_->prepare(decoded->operation_id, decoded->object_id, decoded->manifest,
                                    decoded->chunk_indices, decoded->object_data);
        if (error == storage::StoreError::none)
            error = node_->verify_prepared(decoded->operation_id, decoded->object_id,
                                           decoded->manifest.generation_id,
                                           decoded->chunk_indices);
        if (error == storage::StoreError::none) {
            Record record{snp::OperationState::prepared, fingerprint,
                          decoded->chunk_indices.empty() ? 0U : decoded->chunk_indices.front(),
                          decoded->object_id,
                          decoded->manifest.generation_id};
            if (!persist(decoded->operation_id, record)) error = storage::StoreError::filesystem_error;
            else records_[decoded->operation_id] = record;
        }
        return reply(protocol::MessageType::storage_prepare_response,
            {error, decoded->operation_id, error == storage::StoreError::none
                ? snp::OperationState::prepared : snp::OperationState::unknown,
             fingerprint, {}});
    }
    if (request.type == protocol::MessageType::storage_get_chunk_request) {
        const auto decoded = snp::decode_get_chunk(request.payload);
        if (!decoded) return reply(protocol::MessageType::storage_get_chunk_response,
            {storage::StoreError::corrupt_object, "-", snp::OperationState::unknown, 0, {}});
        auto chunk = node_->get_chunk(decoded->object_id, decoded->generation_id,
                                      decoded->chunk_index);
        return reply(protocol::MessageType::storage_get_chunk_response,
            {chunk.error, "-", snp::OperationState::unknown, 0, std::move(chunk)});
    }
    const auto decoded = snp::decode_identity(request.payload);
    if (!decoded) return reply(protocol::MessageType::error_response,
        {storage::StoreError::corrupt_object, "-", snp::OperationState::unknown, 0, {}});
    auto found = records_.find(decoded->operation_id);
    if (request.type == protocol::MessageType::storage_query_request) {
        return reply(protocol::MessageType::storage_query_response,
            {storage::StoreError::none, decoded->operation_id,
             found == records_.end() ? snp::OperationState::unknown : found->second.state,
             found == records_.end() ? 0U : found->second.fingerprint, {}});
    }
    if (request.type == protocol::MessageType::storage_delete_request) {
        return reply(protocol::MessageType::storage_delete_response,
            {node_->remove(decoded->object_id, decoded->generation_id), decoded->operation_id,
             snp::OperationState::unknown, 0, {}});
    }
    const auto response_type = request.type == protocol::MessageType::storage_commit_request
        ? protocol::MessageType::storage_commit_response
        : protocol::MessageType::storage_abort_response;
    if (found == records_.end() || found->second.object_id != decoded->object_id ||
        found->second.generation_id != decoded->generation_id) {
        return reply(response_type, {storage::StoreError::operation_conflict,
            decoded->operation_id, snp::OperationState::unknown, 0, {}});
    }
    auto& record = found->second;
    storage::StoreError error = storage::StoreError::none;
    if (request.type == protocol::MessageType::storage_commit_request) {
        if (record.state == snp::OperationState::committed) error = storage::StoreError::none;
        else if (record.state != snp::OperationState::prepared)
            error = storage::StoreError::invalid_operation_state;
        else {
            error = node_->commit(decoded->operation_id, record.object_id,
                                  record.generation_id);
            if (error == storage::StoreError::none) {
                record.state = snp::OperationState::committed;
                if (!persist(decoded->operation_id, record)) error = storage::StoreError::filesystem_error;
                else static_cast<void>(node_->finalize(decoded->operation_id, record.object_id,
                                                       record.generation_id));
            }
        }
    } else {
        if (record.state == snp::OperationState::aborted) error = storage::StoreError::none;
        else if (record.state != snp::OperationState::prepared)
            error = storage::StoreError::invalid_operation_state;
        else {
            error = node_->abort(decoded->operation_id, record.object_id,
                                 record.generation_id);
            if (error == storage::StoreError::none) {
                record.state = snp::OperationState::aborted;
                if (!persist(decoded->operation_id, record)) error = storage::StoreError::filesystem_error;
            }
        }
    }
    if (request.type == protocol::MessageType::storage_commit_request &&
        error == storage::StoreError::none && drop_commit_response_once_) {
        drop_commit_response_once_ = false;
        throw std::runtime_error("injected lost commit response");
    }
    return reply(response_type, {error, decoded->operation_id, record.state,
                                 record.fingerprint, {}});
}

}  // namespace atlasfs::replication
