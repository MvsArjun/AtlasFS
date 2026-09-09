#include "atlasfs/checksum.hpp"
#include "atlasfs/object_protocol.hpp"
#include "atlasfs/object_store.hpp"
#include "atlasfs/storage_handler.hpp"
#include "atlasfs/tcp_client.hpp"
#include "atlasfs/tcp_server.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace atlasfs::storage {
namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        static std::atomic_uint64_t sequence{0};
        path_ = std::filesystem::temp_directory_path() /
                ("atlasfs-integration-" +
                 std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
                 std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }
    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

class RunningStorageServer {
public:
    explicit RunningStorageServer(const std::filesystem::path& root)
        : store_(std::make_shared<ObjectStore>(root)),
          server_(0, StorageRequestHandler(store_)),
          thread_([this] {
              try {
                  server_.serve();
              } catch (...) {
                  error_ = std::current_exception();
              }
          }) {}

    ~RunningStorageServer() {
        server_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    [[nodiscard]] std::uint16_t port() const noexcept { return server_.port(); }

    void stop_and_rethrow() {
        server_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
        if (error_) {
            std::rethrow_exception(error_);
        }
    }

private:
    std::shared_ptr<ObjectStore> store_;
    net::TcpServer server_;
    std::exception_ptr error_;
    std::thread thread_;
};

std::vector<std::byte> bytes(std::initializer_list<unsigned char> values) {
    std::vector<std::byte> result;
    for (const auto value : values) {
        result.push_back(static_cast<std::byte>(value));
    }
    return result;
}

object_protocol::Status put(net::TcpClient& client, std::uint64_t request_id,
                            std::string object_id, const std::vector<std::byte>& data,
                            std::uint32_t checksum) {
    protocol::Message request{protocol::MessageType::put_request, 0, request_id,
                              object_protocol::encode_put_request(
                                  {std::move(object_id), data, checksum})};
    const auto response = client.request(request);
    EXPECT_EQ(response.type, protocol::MessageType::put_response);
    const auto status = object_protocol::decode_status_response(response.payload);
    EXPECT_TRUE(status);
    return status.value_or(object_protocol::Status::invalid_request);
}

object_protocol::Status put(net::TcpClient& client, std::uint64_t request_id,
                            std::string object_id, const std::vector<std::byte>& data) {
    return put(client, request_id, std::move(object_id), data, integrity::crc32c(data));
}

object_protocol::GetResponse get(net::TcpClient& client, std::uint64_t request_id,
                                 std::string object_id) {
    protocol::Message request{protocol::MessageType::get_request, 0, request_id,
                              object_protocol::encode_object_id_request(
                                  {std::move(object_id)})};
    const auto response = client.request(request);
    EXPECT_EQ(response.type, protocol::MessageType::get_response);
    const auto decoded = object_protocol::decode_get_response(response.payload);
    EXPECT_TRUE(decoded);
    return decoded.value_or(
        object_protocol::GetResponse{object_protocol::Status::invalid_request, {}, 0});
}

object_protocol::Status remove(net::TcpClient& client, std::uint64_t request_id,
                               std::string object_id) {
    protocol::Message request{protocol::MessageType::delete_request, 0, request_id,
                              object_protocol::encode_object_id_request(
                                  {std::move(object_id)})};
    const auto response = client.request(request);
    EXPECT_EQ(response.type, protocol::MessageType::delete_response);
    const auto status = object_protocol::decode_status_response(response.payload);
    EXPECT_TRUE(status);
    return status.value_or(object_protocol::Status::invalid_request);
}

TEST(StorageIntegration, PutGetDeleteOverPersistentConnection) {
    TemporaryDirectory directory;
    RunningStorageServer server(directory.path());
    net::TcpClient client(server.port());
    const auto first = bytes({0, 1, 2, 0xff});
    const auto second = bytes({9, 8, 0, 7});

    EXPECT_EQ(put(client, 1, "object", first), object_protocol::Status::success);
    EXPECT_EQ(get(client, 2, "object").data, first);
    EXPECT_EQ(put(client, 3, "object", second), object_protocol::Status::success);
    EXPECT_EQ(get(client, 4, "object").data, second);
    EXPECT_EQ(remove(client, 5, "object"), object_protocol::Status::success);
    EXPECT_EQ(get(client, 6, "object").status, object_protocol::Status::not_found);
    EXPECT_EQ(remove(client, 7, "object"), object_protocol::Status::not_found);

    client.close();
    server.stop_and_rethrow();
}

TEST(StorageIntegration, PersistsAcrossServerAndStoreRestart) {
    TemporaryDirectory directory;
    const auto input = bytes({4, 0, 5, 6});
    {
        RunningStorageServer server(directory.path());
        net::TcpClient client(server.port());
        ASSERT_EQ(put(client, 10, "persistent", input), object_protocol::Status::success);
        client.close();
        server.stop_and_rethrow();
    }
    {
        RunningStorageServer server(directory.path());
        net::TcpClient client(server.port());
        const auto result = get(client, 11, "persistent");
        EXPECT_EQ(result.status, object_protocol::Status::success);
        EXPECT_EQ(result.data, input);
        EXPECT_EQ(result.checksum, integrity::crc32c(input));
        client.close();
        server.stop_and_rethrow();
    }
}

TEST(StorageIntegration, ReportsChecksumMismatchAndInvalidIdentifier) {
    TemporaryDirectory directory;
    RunningStorageServer server(directory.path());
    net::TcpClient client(server.port());
    const auto input = bytes({1, 2, 3});
    EXPECT_EQ(put(client, 20, "object", input, 0),
              object_protocol::Status::checksum_mismatch);
    EXPECT_EQ(put(client, 21, "../escape", input),
              object_protocol::Status::invalid_object_id);
    client.close();
    server.stop_and_rethrow();
}

TEST(StorageIntegration, ReportsPersistedCorruptionWithoutReturningData) {
    TemporaryDirectory directory;
    const auto input = bytes({1, 2, 3});
    {
        RunningStorageServer server(directory.path());
        net::TcpClient client(server.port());
        ASSERT_EQ(put(client, 30, "object", input), object_protocol::Status::success);
        client.close();
        server.stop_and_rethrow();
    }
    const auto object_path = directory.path() / "objects" / "object";
    std::filesystem::resize_file(object_path, std::filesystem::file_size(object_path) - 1U);
    {
        RunningStorageServer server(directory.path());
        net::TcpClient client(server.port());
        const auto result = get(client, 31, "object");
        EXPECT_EQ(result.status, object_protocol::Status::corrupt_object);
        EXPECT_TRUE(result.data.empty());
        client.close();
        server.stop_and_rethrow();
    }
}

TEST(StorageIntegration, RejectsMalformedOperationPayload) {
    TemporaryDirectory directory;
    RunningStorageServer server(directory.path());
    net::TcpClient client(server.port());
    const protocol::Message malformed{protocol::MessageType::put_request, 0, 40,
                                      bytes({0, 1})};
    const auto response = client.request(malformed);
    EXPECT_EQ(response.type, protocol::MessageType::put_response);
    EXPECT_EQ(object_protocol::decode_status_response(response.payload),
              object_protocol::Status::malformed_request);
    client.close();
    server.stop_and_rethrow();
}

}  // namespace
}  // namespace atlasfs::storage
