#pragma once

#include "atlasfs/object_store.hpp"
#include "atlasfs/protocol.hpp"

#include <memory>

namespace atlasfs::storage {

class StorageRequestHandler {
public:
    explicit StorageRequestHandler(std::shared_ptr<ObjectStore> store);

    [[nodiscard]] protocol::Message operator()(const protocol::Message& request) const;

private:
    std::shared_ptr<ObjectStore> store_;
};

}  // namespace atlasfs::storage
