#pragma once

#include "atlasfs/object_store.hpp"

#include <cstdint>
#include <string>

namespace atlasfs::replication {

enum class OperationState : std::uint8_t { unknown, prepared, committed, aborted };

struct OperationQueryResult {
    storage::StoreError error{storage::StoreError::none};
    std::string operation_id;
    OperationState state{OperationState::unknown};
    std::uint32_t fingerprint{};
};

}  // namespace atlasfs::replication
