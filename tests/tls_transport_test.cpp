#include "tls_test_support.hpp"
#include "atlasfs/tcp_client.hpp"
#include "atlasfs/tcp_server.hpp"
#include <openssl/ssl.h>
#include <gtest/gtest.h>
#include <array>
#include <atomic>
#include <future>
#include <thread>
#include <sys/socket.h>

namespace atlasfs::net {
namespace {
const PeerIdentity coordinator{"coordinator", PeerRole::coordinator};
const PeerIdentity storage{"d", PeerRole::storage};
const protocol::Message ping{protocol::MessageType::metadata_request, 0, 7, {std::byte{42}}};
class TlsTransport : public ::testing::Test {
protected:
    test::Certificates certificates;
    TlsConfig server_config{certificates.config(storage)};
    TlsConfig client_config{certificates.config(coordinator)};
    TlsTransport() {
        test::Certificates::allow(server_config, coordinator);
        test::Certificates::allow(client_config, storage);
    }
    class Running {
    public:
        explicit Running(TlsConfig config, std::uint32_t limit = protocol::kDefaultMaxPayloadSize)
            : server(std::move(config), 0, [this](const auto& message, const auto& peer) {
                ++dispatches;
                identity = peer.identity;
                return echo_response(message);
              }, limit), thread([this] { try { server.serve(); } catch (...) { error = std::current_exception(); } }) {}
        ~Running() { stop(); }
        void stop() { server.stop(); if (thread.joinable()) thread.join(); }
        TcpServer server;
        std::atomic_uint dispatches{0};
        PeerIdentity identity;
        std::exception_ptr error;
        std::thread thread;
    };
    void rejected(TlsConfig config) {
        Running running(server_config);
        EXPECT_ANY_THROW({ TcpClient client(running.server.port(), std::move(config), storage);
                           static_cast<void>(client.request(ping)); });
        running.stop();
        EXPECT_EQ(running.dispatches, 0U);
        EXPECT_FALSE(running.error);
    }
};
TEST_F(TlsTransport, AuthorizedMutualTlsPreservesFramingAndIdentity) {
    Running running(server_config);
    TcpClient client(running.server.port(), client_config, storage);
    const auto response = client.request(ping);
    EXPECT_EQ(response.request_id, ping.request_id);
    EXPECT_EQ(response.payload, ping.payload);
    client.close(); running.stop();
    EXPECT_EQ(running.identity, coordinator);
    EXPECT_EQ(running.dispatches, 1U);
    EXPECT_FALSE(running.error);
}
TEST_F(TlsTransport, ExpiredClientCertificateRejected) {
    auto config = certificates.config(coordinator, true);
    test::Certificates::allow(config, storage); rejected(std::move(config));
}
TEST_F(TlsTransport, InvalidCertificateSignatureRejected) {
    auto config = certificates.config(coordinator, false, nullptr, {}, true);
    test::Certificates::allow(config, storage); rejected(std::move(config));
}
TEST_F(TlsTransport, ExpiredServerCertificateRejected) {
    server_config = certificates.config(storage, true);
    test::Certificates::allow(server_config, coordinator); rejected(client_config);
}
TEST_F(TlsTransport, UntrustedClientAuthorityRejected) {
    test::Certificates other;
    auto config = certificates.config(coordinator, false, &other);
    test::Certificates::allow(config, storage); rejected(std::move(config));
}
TEST_F(TlsTransport, UntrustedServerAuthorityRejected) {
    test::Certificates other;
    client_config.trust_file = other.root() / "ca.pem"; rejected(client_config);
}
TEST_F(TlsTransport, ValidCertificateWithUnlistedIdentityRejected) {
    auto config = certificates.config({"intruder", PeerRole::coordinator});
    test::Certificates::allow(config, storage); rejected(std::move(config));
}
TEST_F(TlsTransport, ExpectedStorageIdentityMismatchRejected) {
    Running running(server_config);
    EXPECT_THROW(TcpClient(running.server.port(), client_config, {"other", PeerRole::storage}), TransportError);
    running.stop(); EXPECT_EQ(running.dispatches, 0U);
}
TEST_F(TlsTransport, LocalCertificateCannotClaimAnotherNode) {
    client_config.local_identity.id = "other";
    EXPECT_THROW(TlsContext context(client_config), TransportError);
}
TEST_F(TlsTransport, PolicyCannotElevateCertificateRole) {
    server_config.peers.begin()->second.identity.role = PeerRole::storage;
    EXPECT_THROW(TlsContext context(server_config), std::invalid_argument);
}
TEST_F(TlsTransport, Tls12CannotDowngradeSecureServer) {
    Running running(server_config);
    auto socket = connect_loopback(running.server.port());
    const timeval timeout{2, 0};
    ASSERT_EQ(::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> context(SSL_CTX_new(TLS_client_method()), SSL_CTX_free);
    ASSERT_TRUE(context);
    ASSERT_EQ(SSL_CTX_set_max_proto_version(context.get(), TLS1_2_VERSION), 1);
    std::unique_ptr<SSL, decltype(&SSL_free)> ssl(SSL_new(context.get()), SSL_free);
    ASSERT_TRUE(ssl); ASSERT_EQ(SSL_set_fd(ssl.get(), socket.get()), 1);
    EXPECT_LE(SSL_connect(ssl.get()), 0);
    running.stop(); EXPECT_EQ(running.dispatches, 0U);
}
TEST_F(TlsTransport, AmbiguousUriIdentitiesRejected) {
    auto config = certificates.config(coordinator, false, nullptr,
        "urn:atlasfs:coordinator:coordinator,URI:urn:atlasfs:storage:d");
    test::Certificates::allow(config, storage);
    EXPECT_THROW(TlsContext context(config), TransportError);
}
TEST_F(TlsTransport, MismatchedPrivateKeyRejected) {
    client_config.private_key_file = server_config.private_key_file;
    EXPECT_THROW(TlsContext context(client_config), TransportError);
}
TEST_F(TlsTransport, MissingCredentialsAndInvalidTimeoutRejected) {
    auto config = client_config; config.private_key_file.clear();
    EXPECT_THROW(TlsContext context(config), std::invalid_argument);
    config = client_config; config.timeout = std::chrono::milliseconds(0);
    EXPECT_THROW(TlsContext context(config), std::invalid_argument);
    config = client_config; config.trust_file = certificates.root() / "absent";
    EXPECT_THROW(TlsContext context(config), TransportError);
}
TEST_F(TlsTransport, RevokedClientRejectedWithConfiguredCrl) {
    server_config.crl_file = certificates.crl(true); rejected(client_config);
}
TEST_F(TlsTransport, ExpiredCrlFailsClosed) {
    server_config.crl_file = certificates.crl(false, true); rejected(client_config);
}
TEST_F(TlsTransport, CurrentCrlAllowsUnrevokedPeer) {
    server_config.crl_file = certificates.crl();
    Running running(server_config); TcpClient client(running.server.port(), client_config, storage);
    EXPECT_EQ(client.request(ping).payload, ping.payload);
}
TEST_F(TlsTransport, MissingClientCertificateRejected) {
    Running running(server_config);
    auto socket = connect_loopback(running.server.port());
    const timeval timeout{2, 0};
    ASSERT_EQ(::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> context(SSL_CTX_new(TLS_client_method()), SSL_CTX_free);
    ASSERT_TRUE(context);
    std::unique_ptr<SSL, decltype(&SSL_free)> ssl(SSL_new(context.get()), SSL_free);
    ASSERT_TRUE(ssl);
    ASSERT_EQ(SSL_set_fd(ssl.get(), socket.get()), 1);
    // TLS 1.3 may complete locally before receiving the server's rejection alert.
    const int handshake = SSL_connect(ssl.get());
    if (handshake == 1) { char byte{}; EXPECT_LE(SSL_read(ssl.get(), &byte, 1), 0); }
    else EXPECT_LE(handshake, 0);
    running.stop(); EXPECT_EQ(running.dispatches, 0U); EXPECT_FALSE(running.error);
}
TEST_F(TlsTransport, PlaintextCannotDowngradeSecureServer) {
    Running running(server_config);
    EXPECT_ANY_THROW({ TcpClient client(running.server.port()); static_cast<void>(client.request(ping)); });
    TcpClient authorized(running.server.port(), client_config, storage);
    EXPECT_EQ(authorized.request(ping).payload, ping.payload);
    authorized.close(); running.stop(); EXPECT_EQ(running.dispatches, 1U);
}
TEST_F(TlsTransport, TlsClientDoesNotFallbackToPlaintext) {
    TcpServer server;
    std::jthread thread([&] { server.serve(); });
    EXPECT_THROW(TcpClient(server.port(), client_config, storage), TransportError);
    server.stop();
}
TEST_F(TlsTransport, StalledHandshakeHasAbsoluteDeadlineAndAcceptsNextPeer) {
    server_config.timeout = std::chrono::milliseconds(100);
    Running running(server_config);
    auto socket = connect_loopback(running.server.port());
    std::array<std::byte, 1> byte{};
    const auto start = std::chrono::steady_clock::now();
    auto plain = plaintext_stream(socket.get());
    EXPECT_EQ(plain->read(byte, start + std::chrono::seconds(2)), 0U);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
    TcpClient authorized(running.server.port(), client_config, storage);
    EXPECT_EQ(authorized.request(ping).payload, ping.payload);
}
TEST_F(TlsTransport, ClientHandshakeDeadlineAgainstSilentPeer) {
    TcpServer unused;
    client_config.timeout = std::chrono::milliseconds(100);
    const auto start = std::chrono::steady_clock::now();
    EXPECT_THROW(TcpClient(unused.port(), client_config, storage), TransportError);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
}
TEST_F(TlsTransport, AuthenticatedMalformedAndOversizedFramesNeverDispatch) {
    Running running(server_config, 8);
    for (bool oversized : {false, true}) {
        TlsContext context(client_config);
        auto socket = connect_loopback(running.server.port());
        auto stream = context.connect(socket.get(), storage);
        auto frame = protocol::encode(ping);
        if (oversized) { frame = protocol::encode({ping.type, 0, 7, std::vector<std::byte>(9)}); }
        else frame[0] = std::byte{0};
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        stream->write_all(frame, deadline);
        std::array<std::byte, 1> byte{};
        EXPECT_ANY_THROW(static_cast<void>(stream->read(byte, deadline)));
    }
    running.stop(); EXPECT_EQ(running.dispatches, 0U);
}
TEST_F(TlsTransport, FragmentedFrameCannotExtendAbsoluteDeadline) {
    server_config.timeout = std::chrono::milliseconds(100);
    Running running(server_config);
    TlsContext context(client_config); auto socket = connect_loopback(running.server.port());
    auto stream = context.connect(socket.get(), storage);
    const auto frame = protocol::encode(ping);
    const auto start = std::chrono::steady_clock::now();
    stream->write_all(std::span(frame).first(1), start + std::chrono::seconds(2));
    std::array<std::byte, 1> byte{};
    EXPECT_ANY_THROW(static_cast<void>(stream->read(byte, start + std::chrono::seconds(2))));
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
    running.stop(); EXPECT_EQ(running.dispatches, 0U);
}
TEST_F(TlsTransport, ClientReadDeadlineAgainstAuthenticatedSilentHandler) {
    TcpServer server(server_config, 0, [](const auto& message, const auto&) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        return echo_response(message);
    });
    std::jthread thread([&] { server.serve(); });
    client_config.timeout = std::chrono::milliseconds(100);
    TcpClient client(server.port(), client_config, storage);
    const auto start = std::chrono::steady_clock::now();
    EXPECT_THROW(static_cast<void>(client.request(ping)), TransportError);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
    EXPECT_THROW(static_cast<void>(client.request(ping)), std::logic_error);
    server.stop();
}
TEST_F(TlsTransport, TlsWriteDeadlineAgainstPeerThatStopsReading) {
    int descriptors[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
    Socket client_socket(descriptors[0]), server_socket(descriptors[1]);
    const int small_buffer = 4096;
    ASSERT_EQ(::setsockopt(client_socket.get(), SOL_SOCKET, SO_SNDBUF, &small_buffer, sizeof(small_buffer)), 0);
    TlsContext client_context(client_config), server_context(server_config);
    std::promise<void> release;
    auto released = release.get_future();
    std::promise<void> ready;
    auto authenticated = ready.get_future();
    std::exception_ptr error;
    std::jthread thread([&] {
        try {
            auto stream = server_context.accept(server_socket.get());
            ready.set_value();
            released.wait();
        } catch (...) { error = std::current_exception(); ready.set_exception(error); }
    });
    EXPECT_NO_THROW({
        auto stream = client_context.connect(client_socket.get(), storage);
        authenticated.get();
        const std::vector<std::byte> payload(8 * 1024 * 1024);
        const auto start = std::chrono::steady_clock::now();
        EXPECT_THROW(stream->write_all(payload, start + std::chrono::milliseconds(100)), TransportError);
        EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
    });
    release.set_value();
    ::shutdown(client_socket.get(), SHUT_RDWR);
    thread.join();
    EXPECT_FALSE(error);
}
TEST_F(TlsTransport, ReplacingCredentialsAndTrustAuthenticatesNewConnections) {
    { Running original(server_config);
      TcpClient client(original.server.port(), client_config, storage);
      EXPECT_EQ(client.request(ping).payload, ping.payload); }
    test::Certificates replacement;
    auto replaced_server = replacement.config(storage);
    auto replaced_client = replacement.config(coordinator);
    test::Certificates::allow(replaced_server, coordinator);
    test::Certificates::allow(replaced_client, storage);
    Running rotated(replaced_server);
    EXPECT_ANY_THROW({ TcpClient old(rotated.server.port(), client_config, storage);
                      static_cast<void>(old.request(ping)); });
    TcpClient current(rotated.server.port(), replaced_client, storage);
    EXPECT_EQ(current.request(ping).payload, ping.payload);
}
TEST(TlsAuthorization, RoleAndTargetPermissionsAreSeparateFromAuthentication) {
    PeerPolicy peer{storage, {"d"}};
    EXPECT_TRUE(authorize_storage(peer, "d", protocol::MessageType::storage_get_chunk_request));
    for (auto type : {protocol::MessageType::storage_prepare_request, protocol::MessageType::storage_commit_request,
                     protocol::MessageType::storage_abort_request, protocol::MessageType::storage_delete_request,
                     protocol::MessageType::storage_query_request, protocol::MessageType::storage_repair_chunk_request}) {
        EXPECT_FALSE(authorize_storage(peer, "d", type));
        peer.identity = coordinator;
        EXPECT_TRUE(authorize_storage(peer, "d", type));
        EXPECT_FALSE(authorize_storage(peer, "other", type));
        peer.identity = storage;
    }
    peer.identity = coordinator;
    EXPECT_FALSE(authorize_storage(peer, "d", protocol::MessageType::put_request));
    EXPECT_FALSE(authorize_storage(peer, "d", protocol::MessageType::storage_commit_response));
}
} // namespace
} // namespace atlasfs::net
