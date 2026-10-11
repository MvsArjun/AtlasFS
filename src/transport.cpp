#include "atlasfs/transport.hpp"

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <utility>

namespace atlasfs::net {
namespace {
bool valid_id(std::string_view id) {
    return !id.empty() && id.size() <= 255 && id != "." && id != ".." &&
        std::all_of(id.begin(), id.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        });
}
void check_deadline(Deadline deadline) {
    if (std::chrono::steady_clock::now() >= deadline)
        throw TransportError("transport deadline exceeded");
}
std::string uri_identity(X509* certificate) {
    if (!certificate) throw TransportError("peer certificate required");
    std::unique_ptr<GENERAL_NAMES, decltype(&GENERAL_NAMES_free)> names(
        static_cast<GENERAL_NAMES*>(X509_get_ext_d2i(certificate,
            NID_subject_alt_name, nullptr, nullptr)), GENERAL_NAMES_free);
    std::string result;
    unsigned count = 0;
    if (names) for (int i = 0; i < sk_GENERAL_NAME_num(names.get()); ++i) {
        const auto* name = sk_GENERAL_NAME_value(names.get(), i);
        if (name->type != GEN_URI) continue;
        const auto* value = name->d.uniformResourceIdentifier;
        const auto length = ASN1_STRING_length(value);
        if (length <= 0 || length > 300) throw TransportError("invalid certificate identity");
        ++count;
        result.assign(reinterpret_cast<const char*>(ASN1_STRING_get0_data(value)),
                      static_cast<std::size_t>(length));
    }
    if (count != 1 || result.find('\0') != std::string::npos)
        throw TransportError("exactly one certificate URI identity required");
    return result;
}
class PlainStream final : public ByteStream {
public:
    explicit PlainStream(int descriptor) : descriptor_(descriptor) { make_nonblocking(descriptor); }
    std::size_t read(std::span<std::byte> buffer, Deadline deadline) override {
        for (;;) {
            check_deadline(deadline);
            const auto n = ::recv(descriptor_, buffer.data(), buffer.size(), 0);
            if (n >= 0) return static_cast<std::size_t>(n);
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) { wait_socket(descriptor_, false, deadline); continue; }
            throw TransportError("transport read failed");
        }
    }
    void write_all(std::span<const std::byte> bytes, Deadline deadline) override {
        while (!bytes.empty()) {
            check_deadline(deadline);
            const auto n = ::send(descriptor_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
            if (n > 0) { bytes = bytes.subspan(static_cast<std::size_t>(n)); continue; }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { wait_socket(descriptor_, true, deadline); continue; }
            throw TransportError("transport write failed");
        }
    }
private:
    int descriptor_;
};

// Socket BIO uses MSG_NOSIGNAL rather than changing the application's global
// signal disposition. Encryption and certificate validation remain in OpenSSL.
struct BioMethod {
    BIO_METHOD* value{BIO_meth_new(BIO_TYPE_SOURCE_SINK | BIO_get_new_index(), "AtlasFS socket")};
    BioMethod() {
        if (!value) throw TransportError("TLS allocation failed");
        BIO_meth_set_create(value, [](BIO* bio) { BIO_set_init(bio, 1); return 1; });
        BIO_meth_set_destroy(value, [](BIO*) { return 1; });
        BIO_meth_set_read(value, [](BIO* bio, char* bytes, int size) {
            BIO_clear_retry_flags(bio);
            const auto fd = static_cast<int>(reinterpret_cast<std::intptr_t>(BIO_get_data(bio)));
            const auto n = ::recv(fd, bytes, static_cast<std::size_t>(size), 0);
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) BIO_set_retry_read(bio);
            return static_cast<int>(n);
        });
        BIO_meth_set_write(value, [](BIO* bio, const char* bytes, int size) {
            BIO_clear_retry_flags(bio);
            const auto fd = static_cast<int>(reinterpret_cast<std::intptr_t>(BIO_get_data(bio)));
            const auto n = ::send(fd, bytes, static_cast<std::size_t>(size), MSG_NOSIGNAL);
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) BIO_set_retry_write(bio);
            return static_cast<int>(n);
        });
        BIO_meth_set_ctrl(value, [](BIO*, int command, long, void*) -> long {
            return command == BIO_CTRL_FLUSH ? 1L : 0L;
        });
    }
    ~BioMethod() { BIO_meth_free(value); }
};
class SecureStream final : public ByteStream {
public:
    SecureStream(SSL_CTX* context, int descriptor, const TlsConfig& config,
                 const PeerIdentity* expected)
        : descriptor_(descriptor), ssl_(SSL_new(context), SSL_free) {
        if (!ssl_) throw TransportError("TLS allocation failed");
        make_nonblocking(descriptor);
        static const BioMethod method;
        BIO* bio = BIO_new(method.value);
        if (!bio) throw TransportError("TLS allocation failed");
        BIO_set_data(bio, reinterpret_cast<void*>(static_cast<std::intptr_t>(descriptor)));
        SSL_set_bio(ssl_.get(), bio, bio);
        if (expected) SSL_set_connect_state(ssl_.get());
        else SSL_set_accept_state(ssl_.get());
        const auto deadline = std::chrono::steady_clock::now() + config.timeout;
        for (;;) {
            check_deadline(deadline);
            ERR_clear_error();
            const int result = SSL_do_handshake(ssl_.get());
            if (result == 1) break;
            retry(result, deadline);
        }
        if (SSL_get_verify_result(ssl_.get()) != X509_V_OK)
            throw TransportError("TLS certificate verification failed");
        std::unique_ptr<X509, decltype(&X509_free)> certificate(
            SSL_get1_peer_certificate(ssl_.get()), X509_free);
        const auto uri = uri_identity(certificate.get());
        const auto found = config.peers.find(uri);
        if (found == config.peers.end() || (expected && found->second.identity != *expected))
            throw TransportError("TLS peer identity rejected");
        peer_ = found->second;
    }
    const PeerPolicy* peer() const noexcept override { return &peer_; }
    std::size_t read(std::span<std::byte> buffer, Deadline deadline) override {
        for (;;) {
            check_deadline(deadline);
            ERR_clear_error();
            std::size_t n{};
            const int result = SSL_read_ex(ssl_.get(), buffer.data(), buffer.size(), &n);
            if (result == 1) return n;
            const int error = SSL_get_error(ssl_.get(), result);
            if (error == SSL_ERROR_ZERO_RETURN) return 0;
            retry_error(error, deadline);
        }
    }
    void write_all(std::span<const std::byte> bytes, Deadline deadline) override {
        while (!bytes.empty()) {
            check_deadline(deadline);
            ERR_clear_error();
            std::size_t n{};
            const int result = SSL_write_ex(ssl_.get(), bytes.data(), bytes.size(), &n);
            if (result == 1) { bytes = bytes.subspan(n); continue; }
            retry(result, deadline);
        }
    }
private:
    void retry(int result, Deadline deadline) { retry_error(SSL_get_error(ssl_.get(), result), deadline); }
    void retry_error(int error, Deadline deadline) {
        if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
            wait_socket(descriptor_, error == SSL_ERROR_WANT_WRITE, deadline); return;
        }
        throw TransportError("TLS handshake or I/O failed");
    }
    int descriptor_;
    std::unique_ptr<SSL, decltype(&SSL_free)> ssl_;
    PeerPolicy peer_;
};
} // namespace

std::string certificate_identity(const PeerIdentity& identity) {
    if (!valid_id(identity.id) ||
        (identity.role != PeerRole::coordinator && identity.role != PeerRole::storage))
        throw std::invalid_argument("invalid TLS identity");
    return std::string(identity.role == PeerRole::coordinator
        ? "urn:atlasfs:coordinator:" : "urn:atlasfs:storage:") + identity.id;
}
bool authorize_storage(const PeerPolicy& peer, std::string_view target,
                       protocol::MessageType operation) noexcept {
    if (!peer.targets.contains(target)) return false;
    if (operation == protocol::MessageType::storage_get_chunk_request)
        return peer.identity.role == PeerRole::coordinator || peer.identity.role == PeerRole::storage;
    if (peer.identity.role != PeerRole::coordinator) return false;
    switch (operation) {
        case protocol::MessageType::storage_prepare_request:
        case protocol::MessageType::storage_commit_request:
        case protocol::MessageType::storage_abort_request:
        case protocol::MessageType::storage_delete_request:
        case protocol::MessageType::storage_query_request:
        case protocol::MessageType::storage_repair_chunk_request: return true;
        case protocol::MessageType::epoch_query_request: return true;
        case protocol::MessageType::epoch_activate_request: return peer.may_activate_epoch;
        default: return false;
    }
}
void make_nonblocking(int descriptor) {
    const int flags = ::fcntl(descriptor, F_GETFL, 0);
    if (flags < 0 || ::fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) < 0)
        throw TransportError("nonblocking socket configuration failed");
}
void wait_socket(int descriptor, bool writing, Deadline deadline) {
    for (;;) {
        check_deadline(deadline);
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        pollfd value{descriptor, static_cast<short>(writing ? POLLOUT : POLLIN), 0};
        const int result = ::poll(&value, 1, static_cast<int>(std::clamp<std::int64_t>(remaining, 1, INT_MAX)));
        if (result > 0) return; // EOF/error readiness is handled by the next I/O call.
        if (result < 0 && errno == EINTR) continue;
        if (result == 0) throw TransportError("transport deadline exceeded");
        throw TransportError("socket wait failed");
    }
}
std::unique_ptr<ByteStream> plaintext_stream(int descriptor) { return std::make_unique<PlainStream>(descriptor); }
struct TlsContext::Impl {
    TlsConfig config;
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> context{nullptr, SSL_CTX_free};
};
TlsContext::TlsContext(TlsConfig config) : impl_(std::make_unique<Impl>()) {
    if (config.timeout.count() <= 0 || config.timeout > std::chrono::minutes(5) ||
        config.certificate_file.empty() || config.private_key_file.empty() ||
        config.trust_file.empty() || config.peers.empty() || config.peers.size() > 4096)
        throw std::invalid_argument("incomplete TLS configuration");
    const auto local_uri = certificate_identity(config.local_identity);
    for (const auto& [uri, policy] : config.peers) {
        if (uri != certificate_identity(policy.identity) || policy.targets.size() > 4096)
            throw std::invalid_argument("invalid TLS peer policy");
        for (const auto& target : policy.targets)
            if (!valid_id(target)) throw std::invalid_argument("invalid TLS target policy");
    }
    impl_->context.reset(SSL_CTX_new(TLS_method()));
    auto* context = impl_->context.get();
    // Reject encrypted keys without an interactive password prompt.
    if (context) SSL_CTX_set_default_passwd_cb(context,
        [](char*, int, int, void*) { return 0; });
    if (!context || SSL_CTX_set_min_proto_version(context, TLS1_3_VERSION) != 1 ||
        SSL_CTX_use_certificate_chain_file(context, config.certificate_file.string().c_str()) != 1 ||
        SSL_CTX_use_PrivateKey_file(context, config.private_key_file.string().c_str(), SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_check_private_key(context) != 1 ||
        SSL_CTX_load_verify_locations(context, config.trust_file.string().c_str(), nullptr) != 1)
        throw TransportError("TLS credential configuration failed");
    if (uri_identity(SSL_CTX_get0_certificate(context)) != local_uri)
        throw TransportError("TLS local certificate identity mismatch");
    SSL_CTX_set_verify(context, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    SSL_CTX_set_verify_depth(context, 8);
    SSL_CTX_set_max_cert_list(context, 64 * 1024);
    SSL_CTX_set_options(context, SSL_OP_NO_TICKET | SSL_OP_NO_RENEGOTIATION);
    SSL_CTX_set_session_cache_mode(context, SSL_SESS_CACHE_OFF);
    SSL_CTX_set_num_tickets(context, 0);
    SSL_CTX_set_max_early_data(context, 0);
    if (!config.crl_file.empty()) {
        auto* store = SSL_CTX_get_cert_store(context);
        if (X509_STORE_load_file(store, config.crl_file.string().c_str()) != 1 ||
            X509_STORE_set_flags(store, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL) != 1)
            throw TransportError("TLS revocation configuration failed");
    }
    impl_->config = std::move(config);
}
TlsContext::~TlsContext() = default;
std::unique_ptr<ByteStream> TlsContext::connect(int descriptor, const PeerIdentity& expected) const {
    return std::make_unique<SecureStream>(impl_->context.get(), descriptor, impl_->config, &expected);
}
std::unique_ptr<ByteStream> TlsContext::accept(int descriptor) const {
    return std::make_unique<SecureStream>(impl_->context.get(), descriptor, impl_->config, nullptr);
}
std::chrono::milliseconds TlsContext::timeout() const noexcept { return impl_->config.timeout; }
} // namespace atlasfs::net
