#include "atlasfs/remote_storage_node.hpp"

#include "atlasfs/checksum.hpp"

#include <fstream>
#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>
#include <utility>

namespace atlasfs::replication {
namespace snp = storage_node_protocol;

RemoteStorageNodeClient::RemoteStorageNodeClient(std::string node_id, std::uint16_t port)
    : node_id_(std::move(node_id)), client_(port) {
    if (node_id_.empty()) throw std::invalid_argument("remote storage node requires id");
}

std::string_view RemoteStorageNodeClient::id() const noexcept { return node_id_; }

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

StorageNodeServer::StorageNodeServer(std::shared_ptr<StorageNode> node,
    std::filesystem::path journal_root, std::uint16_t port, bool drop_commit_response_once)
    : node_(std::move(node)), operations_directory_(std::move(journal_root) / "operations"),
      drop_commit_response_once_(drop_commit_response_once),
      server_(port, [this](const protocol::Message& request) { return handle(request); }) {
    if (!node_) throw std::invalid_argument("storage server requires node");
    std::filesystem::create_directories(operations_directory_);
    load_records();
}

std::uint16_t StorageNodeServer::port() const noexcept { return server_.port(); }
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
    const int file = ::open(temporary.c_str(), O_RDONLY | O_CLOEXEC);
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

protocol::Message StorageNodeServer::handle(const protocol::Message& request) {
    auto reply = [&](protocol::MessageType type, snp::Response response) {
        return protocol::Message{type, 0, request.request_id, snp::encode_response(response)};
    };
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
