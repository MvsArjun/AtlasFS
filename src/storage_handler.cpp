#include "atlasfs/storage_handler.hpp"

#include "atlasfs/object_protocol.hpp"

#include <stdexcept>
#include <utility>

namespace atlasfs::storage {
namespace {

object_protocol::Status to_status(StoreError error) {
    switch (error) {
        case StoreError::none:
            return object_protocol::Status::success;
        case StoreError::not_found:
            return object_protocol::Status::not_found;
        case StoreError::invalid_object_id:
            return object_protocol::Status::invalid_object_id;
        case StoreError::checksum_mismatch:
            return object_protocol::Status::checksum_mismatch;
        case StoreError::corrupt_object:
            return object_protocol::Status::corrupt_object;
        case StoreError::filesystem_error:
            return object_protocol::Status::filesystem_error;
        case StoreError::payload_too_large:
            return object_protocol::Status::payload_too_large;
        case StoreError::operation_conflict:
        case StoreError::invalid_operation_state:
        case StoreError::result_unknown:
        case StoreError::stale_epoch:
        case StoreError::epoch_unresolved:
            return object_protocol::Status::filesystem_error;
    }
    return object_protocol::Status::filesystem_error;
}

protocol::Message status_response(protocol::MessageType type, std::uint64_t request_id,
                                  object_protocol::Status status) {
    return {type, 0, request_id, object_protocol::encode_status_response(status)};
}

}  // namespace

StorageRequestHandler::StorageRequestHandler(std::shared_ptr<ObjectStore> store)
    : store_(std::move(store)) {
    if (!store_) {
        throw std::invalid_argument("storage request handler requires an object store");
    }
}

protocol::Message StorageRequestHandler::operator()(const protocol::Message& request) const {
    switch (request.type) {
        case protocol::MessageType::put_request: {
            const auto decoded = object_protocol::decode_put_request(request.payload);
            if (!decoded) {
                return status_response(protocol::MessageType::put_response, request.request_id,
                                       object_protocol::Status::malformed_request);
            }
            return status_response(
                protocol::MessageType::put_response, request.request_id,
                to_status(store_->put(decoded->object_id, decoded->data, decoded->checksum)));
        }
        case protocol::MessageType::get_request: {
            const auto decoded = object_protocol::decode_object_id_request(request.payload);
            if (!decoded) {
                return {protocol::MessageType::get_response, 0, request.request_id,
                        object_protocol::encode_get_response(
                            {object_protocol::Status::malformed_request, {}, 0})};
            }
            auto result = store_->get(decoded->object_id);
            return {protocol::MessageType::get_response, 0, request.request_id,
                    object_protocol::encode_get_response(
                        {to_status(result.error), std::move(result.data), result.checksum})};
        }
        case protocol::MessageType::delete_request: {
            const auto decoded = object_protocol::decode_object_id_request(request.payload);
            if (!decoded) {
                return status_response(protocol::MessageType::delete_response,
                                       request.request_id,
                                       object_protocol::Status::malformed_request);
            }
            return status_response(protocol::MessageType::delete_response, request.request_id,
                                   to_status(store_->remove(decoded->object_id)));
        }
        case protocol::MessageType::metadata_request:
        case protocol::MessageType::put_response:
        case protocol::MessageType::get_response:
        case protocol::MessageType::delete_response:
        case protocol::MessageType::response:
        case protocol::MessageType::error_response:
        case protocol::MessageType::storage_prepare_request:
        case protocol::MessageType::storage_prepare_response:
        case protocol::MessageType::storage_commit_request:
        case protocol::MessageType::storage_commit_response:
        case protocol::MessageType::storage_abort_request:
        case protocol::MessageType::storage_abort_response:
        case protocol::MessageType::storage_get_chunk_request:
        case protocol::MessageType::storage_get_chunk_response:
        case protocol::MessageType::storage_delete_request:
        case protocol::MessageType::storage_delete_response:
        case protocol::MessageType::storage_query_request:
        case protocol::MessageType::storage_query_response:
        case protocol::MessageType::storage_repair_chunk_request:
        case protocol::MessageType::storage_repair_chunk_response:
        case protocol::MessageType::epoch_activate_request:
        case protocol::MessageType::epoch_activate_response:
        case protocol::MessageType::epoch_query_request:
        case protocol::MessageType::epoch_query_response:
        case protocol::MessageType::epoch_error_response:
            return status_response(protocol::MessageType::error_response, request.request_id,
                                   object_protocol::Status::invalid_request);
    }
    return status_response(protocol::MessageType::error_response, request.request_id,
                           object_protocol::Status::invalid_request);
}

}  // namespace atlasfs::storage
