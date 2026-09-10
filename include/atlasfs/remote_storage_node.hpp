#pragma once

#include "atlasfs/replication.hpp"
#include "atlasfs/storage_node_protocol.hpp"
#include "atlasfs/tcp_client.hpp"
#include "atlasfs/tcp_server.hpp"

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <string>

namespace atlasfs::replication {

class RemoteStorageNodeClient final : public StorageNode {
public:
    RemoteStorageNodeClient(std::string node_id, std::uint16_t port);
    [[nodiscard]] std::string_view id() const noexcept override;
    [[nodiscard]] storage::StoreError prepare(std::string_view, std::string_view,
        const storage::ObjectManifest&, std::span<const std::uint32_t>,
        std::span<const std::byte>) override;
    [[nodiscard]] storage::StoreError verify_prepared(std::string_view, std::string_view,
        std::string_view, std::span<const std::uint32_t>) const override;
    [[nodiscard]] storage::StoreError commit(std::string_view, std::string_view,
                                              std::string_view) override;
    [[nodiscard]] storage::StoreError abort(std::string_view, std::string_view,
                                             std::string_view) override;
    [[nodiscard]] storage::StoreError finalize(std::string_view, std::string_view,
                                                std::string_view) override;
    [[nodiscard]] storage::ChunkResult get_chunk(std::string_view, std::string_view,
        std::uint32_t) const override;
    [[nodiscard]] storage::StoreError remove(std::string_view,
                                             std::string_view) override;
    [[nodiscard]] storage_node_protocol::OperationState query(std::string_view operation_id);
private:
    [[nodiscard]] storage_node_protocol::Response exchange(
        protocol::MessageType, protocol::MessageType, std::vector<std::byte>,
        bool commit = false) const;
    std::string node_id_;
    mutable net::TcpClient client_;
    mutable std::uint64_t next_request_id_{1};
};

class StorageNodeServer {
public:
    StorageNodeServer(std::shared_ptr<StorageNode> node, std::filesystem::path journal_root,
                     std::uint16_t port = 0, bool drop_commit_response_once = false);
    [[nodiscard]] std::uint16_t port() const noexcept;
    void serve();
    void stop() noexcept;
private:
    struct Record {
        storage_node_protocol::OperationState state;
        std::uint32_t fingerprint{};
        std::uint32_t probe_chunk{};
        std::string object_id;
        std::string generation_id;
    };
    [[nodiscard]] protocol::Message handle(const protocol::Message& request);
    void load_records();
    [[nodiscard]] bool persist(std::string_view operation_id, const Record& record);
    std::shared_ptr<StorageNode> node_;
    std::filesystem::path operations_directory_;
    std::map<std::string, Record, std::less<>> records_;
    bool drop_commit_response_once_{};
    net::TcpServer server_;
};

}  // namespace atlasfs::replication
