#pragma once

#include "atlasfs/checksum.hpp"
#include "atlasfs/metrics.hpp"
#include "atlasfs/coordinator_operation.hpp"
#include "atlasfs/remote_storage_node.hpp"
#include "atlasfs/repair.hpp"
#include "../tests/tls_test_support.hpp"
#include <algorithm>
#include <exception>
#include <thread>

namespace atlasfs::bench {
inline void require(bool okay, const std::string& context) {
    if (!okay) throw std::runtime_error(context);
}
inline std::vector<std::byte> payload(std::size_t size, std::uint64_t seed) {
    std::vector<std::byte> result(size);
    // Specified xorshift64 sequence, independent of stdlib distribution details.
    auto state = seed ? seed : 1;
    for (auto& byte : result) {
        state ^= state << 13; state ^= state >> 7; state ^= state << 17;
        byte = static_cast<std::byte>(state & 255);
    }
    return result;
}
inline storage::ObjectManifest manifest(std::span<const std::byte> data, std::uint32_t chunk,
                                         std::string generation) {
    storage::ObjectManifest result{data.size(), chunk, integrity::crc32c(data), std::move(generation), {}};
    for (std::size_t at = 0; at < data.size(); at += chunk) {
        const auto part = data.subspan(at, std::min<std::size_t>(chunk, data.size() - at));
        result.chunks.push_back({static_cast<std::uint32_t>(result.chunks.size()),
            static_cast<std::uint32_t>(part.size()), integrity::crc32c(part)});
    }
    return result;
}
inline metadata::StorageNode member(std::string id, bool healthy = true) {
    return {std::move(id), {"127.0.0.1", 7000}, metadata::NodeState::active,
        healthy ? metadata::NodeHealthState::healthy : metadata::NodeHealthState::unavailable, 0, true};
}
inline metadata::PlacementPlan placement(std::string object, const storage::ObjectManifest& value,
                                         const std::vector<std::string>& replicas) {
    metadata::PlacementPlan result{std::move(object), value.generation_id, value.object_size,
        value.chunk_size, value.object_checksum, 10, static_cast<std::uint32_t>(replicas.size()), false, {}};
    for (const auto& chunk : value.chunks)
        result.chunks.push_back({{result.object_id, value.generation_id, chunk.index}, chunk, replicas});
    return result;
}
inline void seed_catalog(metadata::DurableReplicaCatalog& catalog, const metadata::PlacementPlan& plan) {
    for (const auto& chunk : plan.chunks) {
        std::vector<metadata::CatalogReplica> replicas;
        for (const auto& id : chunk.replica_node_ids)
            replicas.push_back({id, metadata::CatalogReplicaState::present, chunk.chunk.checksum, 10});
        require(catalog.upsert(chunk.identity, chunk.chunk, std::move(replicas)) == metadata::ReplicaCatalogError::none,
                "catalog seed object=" + plan.object_id);
    }
}
class Running {
public:
    Running(std::shared_ptr<replication::StorageNode> node, const std::filesystem::path& journal,
            net::TlsConfig tls, std::shared_ptr<epoch::Store> fence, bool lost = false)
        : server(std::move(node), journal, std::move(tls), 0, false, lost, std::move(fence)),
          thread([this, observer = metrics::current_owner()] {
              metrics::Scope scope(observer);
              try { server.serve(); } catch (...) { error = std::current_exception(); }
          }) {}
    ~Running() { stop(); }
    void stop() { server.stop(); if (thread.joinable()) thread.join(); }
    void check() { // Call only after stop/join. Lost-response injection intentionally stops serve().
        if (error) {
            try { std::rethrow_exception(error); }
            catch (const std::runtime_error& failure) {
                if (std::string_view(failure.what()) != "injected lost repair response") throw;
            }
        }
    }
    replication::StorageNodeServer server;
private:
    std::exception_ptr error;
    std::thread thread;
};
struct SecureNode {
    std::string id;
    std::filesystem::path data_root, journal;
    net::TlsConfig tls;
    std::shared_ptr<epoch::Store> fence;
    std::shared_ptr<replication::LocalStorageNode> local;
    std::unique_ptr<Running> running;
    std::shared_ptr<replication::RemoteStorageNodeClient> client;
};
class Cluster {
public:
    test::Certificates temporary;
    const epoch::Token token{"benchmark", 1, "coordinator", "activation-1"};
    net::TlsConfig coordinator{temporary.config({"coordinator", net::PeerRole::coordinator})};
    std::shared_ptr<epoch::Store> authority;
    std::vector<SecureNode> nodes;
    Cluster(std::uint32_t count, std::uint32_t chunk) {
        std::filesystem::create_directory(root() / "coordinator");
        epoch::Store::provision(root() / "coordinator" / "epoch", token);
        authority = std::make_shared<epoch::Store>(root() / "coordinator" / "epoch", token);
        for (std::uint32_t n = 0; n < count; ++n) {
            SecureNode node;
            node.id = "node-" + std::to_string(n);
            node.data_root = root() / (node.id + "-data"); node.journal = root() / (node.id + "-journal");
            std::filesystem::create_directory(node.journal);
            epoch::Store::provision(node.journal / "epoch", token);
            node.fence = std::make_shared<epoch::Store>(node.journal / "epoch", token);
            node.local = std::make_shared<replication::LocalStorageNode>(node.id, node.data_root,
                object_protocol::kMaxObjectDataSize, chunk);
            node.tls = temporary.config({node.id, net::PeerRole::storage});
            test::Certificates::allow(node.tls, coordinator.local_identity, {node.id});
            node.tls.peers.begin()->second.may_activate_epoch = true;
            test::Certificates::allow(coordinator, node.tls.local_identity, {node.id});
            nodes.push_back(std::move(node));
        }
        for (std::size_t n = 0; n < nodes.size(); ++n) start(n);
    }
    ~Cluster() { for (auto& node : nodes) { node.client.reset(); node.running.reset(); } }
    const std::filesystem::path& root() const { return temporary.root(); }
    void stop(std::size_t n) {
        auto& node = nodes.at(n); node.client.reset(); node.running->stop(); node.running->check(); node.running.reset();
    }
    void start(std::size_t n, bool lost = false) {
        auto& node = nodes.at(n);
        node.running = std::make_unique<Running>(node.local, node.journal, node.tls, node.fence, lost);
        node.client = std::make_shared<replication::RemoteStorageNodeClient>(node.id, node.running->server.port(), coordinator, token);
    }
    void restart(std::size_t n, std::uint32_t chunk, bool lost = false) {
        stop(n); auto& node = nodes.at(n);
        node.local = std::make_shared<replication::LocalStorageNode>(node.id, node.data_root,
            object_protocol::kMaxObjectDataSize, chunk);
        start(n, lost);
    }
    std::vector<std::shared_ptr<replication::StorageNode>> clients() const {
        std::vector<std::shared_ptr<replication::StorageNode>> result;
        for (const auto& node : nodes) result.push_back(node.client);
        return result;
    }
};
} // namespace atlasfs::bench
