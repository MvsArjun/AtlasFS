#include "atlasfs/tcp_server.hpp"
#include "atlasfs/metrics.hpp"
#include "atlasfs/admission.hpp"

#include <arpa/inet.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <condition_variable>
#include <netinet/in.h>
#include <poll.h>
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>

namespace atlasfs::net {
namespace {

[[noreturn]] void throw_system_error(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

std::vector<std::byte> to_bytes(std::string_view text) {
    const auto* begin = reinterpret_cast<const std::byte*>(text.data());
    return {begin, begin + text.size()};
}

}  // namespace

struct TcpServer::DispatchState {
    explicit DispatchState(std::size_t workers) : descriptors(workers, -1) {}
    mutable std::mutex mutex;
    std::condition_variable ready;
    std::vector<int> descriptors;
    DispatchSnapshot counters;
    std::exception_ptr failure;
};

protocol::Message echo_response(const protocol::Message& request) {
    switch (request.type) {
        case protocol::MessageType::put_request:
        case protocol::MessageType::get_request:
        case protocol::MessageType::delete_request:
        case protocol::MessageType::metadata_request:
            return {protocol::MessageType::response, request.flags, request.request_id,
                    request.payload};
        case protocol::MessageType::response:
        case protocol::MessageType::error_response:
        case protocol::MessageType::put_response:
        case protocol::MessageType::get_response:
        case protocol::MessageType::delete_response:
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
            return {protocol::MessageType::error_response, 0, request.request_id,
                    to_bytes("unexpected response message")};
    }
    throw std::logic_error("unhandled AtlasFS message type");
}

TcpServer::TcpServer(std::uint16_t port, RequestHandler handler,
                     std::uint32_t max_payload_size, const DispatchConfig& dispatch)
    : handler_(std::move(handler)), max_payload_size_(max_payload_size), dispatch_(dispatch) {
    if (!handler_) {
        throw std::invalid_argument("TCP server requires a request handler");
    }
    if (dispatch_.workers < 1 || dispatch_.workers > 16 ||
        dispatch_.request_queue_capacity > 65536 ||
        dispatch_.max_requests_per_connection < 1 ||
        dispatch_.max_requests_per_connection > 1000000)
        throw std::invalid_argument("invalid TCP dispatch configuration");
    state_ = std::make_unique<DispatchState>(dispatch_.serial ? 1 : dispatch_.workers);
    if (!dispatch_.serial)
        admission_ = std::make_unique<scheduling::AdmissionQueue>(dispatch_.request_queue_capacity);

    listener_.reset(::socket(AF_INET, SOCK_STREAM, 0));
    if (!listener_.valid()) {
        throw_system_error("socket");
    }

    const int reuse_address = 1;
    if (::setsockopt(listener_.get(), SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                     sizeof(reuse_address)) < 0) {
        throw_system_error("setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener_.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) <
        0) {
        throw_system_error("bind");
    }
    if (::listen(listener_.get(), static_cast<int>(dispatch_.workers)) < 0) {
        throw_system_error("listen");
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener_.get(), reinterpret_cast<sockaddr*>(&address), &address_size) <
        0) {
        throw_system_error("getsockname");
    }
    port_ = ntohs(address.sin_port);
}

TcpServer::~TcpServer() {
    stop();
}

TcpServer::TcpServer(TlsConfig config, std::uint16_t port,
                     AuthenticatedRequestHandler handler, std::uint32_t max_payload_size,
                     const DispatchConfig& dispatch)
    : TcpServer(port, echo_response, max_payload_size, dispatch) {
    if (!handler) throw std::invalid_argument("TLS server requires an authenticated handler");
    tls_ = std::make_shared<TlsContext>(std::move(config));
    authenticated_handler_ = std::move(handler);
}

std::uint16_t TcpServer::port() const noexcept {
    return port_;
}

DispatchSnapshot TcpServer::snapshot() const noexcept {
    DispatchSnapshot result;
    {
        std::lock_guard lock(state_->mutex);
        result = state_->counters;
    }
    // Never nest lifecycle and admission locks, including in diagnostics.
    if (admission_) {
        const auto queue = admission_->snapshot();
        result.request_waiting = queue.waiting;
        result.peak_request_waiting = queue.peak_waiting;
    }
    return result;
}

void TcpServer::serve() {
    if (serving_.exchange(true)) throw std::logic_error("TCP server may only be served once");
    if (dispatch_.serial) {
        serve_serial();
    } else {
        std::vector<std::thread> workers;
        try {
            workers.reserve(dispatch_.workers);
            for (std::size_t index = 0; index < dispatch_.workers; ++index) {
                workers.emplace_back([this, index, observer = metrics::current_owner()] {
                    metrics::Scope scope(observer);
                    worker(index);
                });
            }
            while (!stopping_.load()) {
                Socket connection(::accept(listener_.get(), nullptr, nullptr));
                if (!connection.valid()) {
                    if (errno == EINTR) continue;
                    if (stopping_.load()) break;
                    throw_system_error("accept");
                }
                std::lock_guard lock(state_->mutex);
                ++state_->counters.accepted;
                auto available = std::find(state_->descriptors.begin(), state_->descriptors.end(), -1);
                if (stopping_.load() || available == state_->descriptors.end()) {
                    // Before TLS authentication/request identity, closing is the only
                    // safe rejection. No implicit retry or synthetic success reply.
                    ++state_->counters.rejected;
                    continue;
                }
                *available = connection.release();
                ++state_->counters.active;
                state_->counters.peak_active = std::max(state_->counters.peak_active,
                                                       state_->counters.active);
                state_->ready.notify_all();
            }
        } catch (...) {
            fail(std::current_exception());
        }
        stop();
        for (auto& thread : workers) if (thread.joinable()) thread.join();
    }
    std::exception_ptr failure;
    { std::lock_guard lock(state_->mutex); failure = state_->failure; }
    if (failure) std::rethrow_exception(failure);
}

void TcpServer::serve_serial() {
    while (!stopping_.load()) {
        const int descriptor = ::accept(listener_.get(), nullptr, nullptr);
        if (descriptor < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (stopping_.load()) {
                return;
            }
            fail(std::make_exception_ptr(std::system_error(errno, std::generic_category(), "accept")));
            return;
        }

        Socket connection(descriptor);
        {
            std::lock_guard lock(state_->mutex);
            if (stopping_.load()) return;
            state_->descriptors.front() = descriptor;
            ++state_->counters.accepted;
            ++state_->counters.active;
            state_->counters.peak_active = 1;
        }
        try {
            handle_connection(connection);
        } catch (const TransportError&) {
        } catch (const std::system_error&) {
        } catch (...) {
            fail(std::current_exception());
        }
        {
            std::lock_guard lock(state_->mutex);
            state_->descriptors.front() = -1;
            --state_->counters.active;
            connection.reset();
        }
    }
}

void TcpServer::worker(std::size_t index) noexcept {
    // Ownership survives exception unwinding until lifecycle retirement. A
    // worker-control error must not close an fd still visible to stop().
    Socket connection;
    try {
        for (;;) {
            {
                std::unique_lock lock(state_->mutex);
                state_->ready.wait(lock, [&] {
                    return stopping_.load() || state_->descriptors[index] >= 0;
                });
                const auto descriptor = state_->descriptors[index];
                if (descriptor < 0) return;
                connection.reset(descriptor);
            }
            if (!stopping_.load()) {
                try { handle_connection(connection); }
                catch (const TransportError&) {} // Connection-local authentication/I/O failure.
                catch (const std::system_error&) {} // TCP setup; admission errors fail before rethrow.
            }
            {
                std::lock_guard lock(state_->mutex);
                // Retire before close under the same lock used by stop().
                state_->descriptors[index] = -1;
                --state_->counters.active;
                connection.reset();
            }
            if (stopping_.load()) return;
        }
    } catch (...) {
        fail(std::current_exception());
        std::lock_guard lock(state_->mutex);
        const auto descriptor = state_->descriptors[index];
        if (descriptor >= 0) {
            // A wait error can occur before the assigned socket was claimed.
            if (!connection.valid()) connection.reset(descriptor);
            state_->descriptors[index] = -1;
            --state_->counters.active;
            connection.reset();
        }
    }
}

void TcpServer::fail(std::exception_ptr failure) noexcept {
    { std::lock_guard lock(state_->mutex); if (!state_->failure) state_->failure = std::move(failure); }
    stop();
}

void TcpServer::stop() noexcept {
    if (stopping_.exchange(true)) {
        return;
    }
    if (listener_.valid()) {
        ::shutdown(listener_.get(), SHUT_RDWR);
    }
    if (admission_) admission_->shutdown();
    std::lock_guard lock(state_->mutex);
    for (const auto descriptor : state_->descriptors)
        if (descriptor >= 0) ::shutdown(descriptor, SHUT_RDWR);
    state_->ready.notify_all();
}

bool TcpServer::disconnected(int descriptor) const noexcept {
    pollfd state{descriptor, POLLIN, 0};
    if (::poll(&state, 1, 0) <= 0) return false;
    if (state.revents & (POLLHUP | POLLERR | POLLNVAL)) return true;
    if (state.revents & POLLIN) {
        // Peek raw socket state only. Never consume encrypted bytes outside SSL.
        // Buffered TLS records/close_notify are not definitive disconnect evidence.
        std::byte byte;
        const auto result = ::recv(descriptor, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
        return result == 0 || (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
    }
    return false;
}

void TcpServer::handle_connection(Socket& connection) {
    protocol::StreamParser parser(max_payload_size_);
    std::array<std::byte, 8192> buffer{};
    if (tls_ && tls_->tcp_nodelay()) set_tcp_nodelay(connection.get(), true);
    auto stream = tls_ ? tls_->accept(connection.get()) : plaintext_stream(connection.get());
    const auto timeout = tls_ ? tls_->timeout() : std::chrono::seconds(5);
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t served = 0;

    while (!stopping_.load()) {
        const auto received = stream->read(buffer, deadline);
        if (received > 0) {
            auto result = parser.feed(
                std::span<const std::byte>(buffer).first(static_cast<std::size_t>(received)));
            for (const auto& request : result.messages) {
                metrics::add(metrics::Counter::frames_received);
                metrics::add(metrics::Counter::payload_received, request.payload.size());
                scheduling::AdmissionQueue::Result admitted;
                if (admission_) {
                    try { admitted = admission_->acquire(deadline); }
                    catch (...) {
                        // Synchronization failures are not expected socket I/O
                        // errors, even when they use std::system_error.
                        fail(std::current_exception());
                        throw;
                    }
                    if (admitted.status != scheduling::AdmissionQueue::Status::admitted) {
                        std::lock_guard lock(state_->mutex);
                        if (admitted.status == scheduling::AdmissionQueue::Status::deadline_expired)
                            ++state_->counters.admission_expired;
                        else ++state_->counters.admission_rejected;
                        return;
                    }
                    if (stopping_.load()) return;
                    if (disconnected(connection.get())) {
                        std::lock_guard lock(state_->mutex);
                        ++state_->counters.disconnect_cancellations;
                        return;
                    }
                }
                { std::lock_guard lock(state_->mutex); ++state_->counters.requests; }
                protocol::Message response;
                try {
                    // Authentication policy, epoch guard, physical publication and
                    // journals all remain in this one serialized handler boundary.
                    response = tls_ ? authenticated_handler_(request, *stream->peer())
                                    : handler_(request);
                    if (response.payload.size() > max_payload_size_)
                        throw std::length_error("TCP handler response exceeds payload limit");
                } catch (...) {
                    fail(std::current_exception());
                    throw;
                }
                admitted.lease.release(); // Slow response I/O does not own authority.
                stream->write_all(protocol::encode(response), deadline);
                metrics::add(metrics::Counter::frames_sent);
                metrics::add(metrics::Counter::payload_sent, response.payload.size());
                metrics::add(metrics::Counter::framed_sent, response.payload.size() + protocol::kHeaderSize);
                deadline = std::chrono::steady_clock::now() + timeout;
                if (++served == dispatch_.max_requests_per_connection) {
                    std::lock_guard lock(state_->mutex);
                    ++state_->counters.request_limit_closes;
                    return;
                }
            }
            if (result.error != protocol::ParseError::none) {
                return;
            }
            continue;
        }
        return;
    }
}

}  // namespace atlasfs::net
