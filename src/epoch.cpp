#include "atlasfs/epoch.hpp"
#include "atlasfs/checksum.hpp"
#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace atlasfs::epoch {
namespace {
constexpr std::size_t max_size = 1024 * 1024;
constexpr std::uint32_t magic = 0x41455043U; // AEPC
template<class T> void append(std::vector<std::byte>& out, T value) {
    for (std::size_t i = sizeof(T); i > 0; --i)
        out.push_back(static_cast<std::byte>((value >> ((i - 1) * 8U)) & 0xffU));
}
template<class T> bool take(std::span<const std::byte> bytes, std::size_t& at, T& value) {
    if (at > bytes.size() || bytes.size() - at < sizeof(T)) return false;
    value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i)
        value = static_cast<T>((value << 8U) | std::to_integer<unsigned>(bytes[at++]));
    return true;
}
bool valid_id(std::string_view id) noexcept {
    return !id.empty() && id.size() <= 96 && std::all_of(id.begin(), id.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_';
    });
}
void string(std::vector<std::byte>& out, std::string_view value) {
    append(out, static_cast<std::uint16_t>(value.size()));
    for (unsigned char c : value) out.push_back(static_cast<std::byte>(c));
}
bool string(std::span<const std::byte> bytes, std::size_t& at, std::string& value) {
    std::uint16_t length{};
    if (!take(bytes, at, length) || length > 255 || bytes.size() - at < length) return false;
    value.assign(reinterpret_cast<const char*>(bytes.data() + at), length); at += length;
    return true;
}
void token(std::vector<std::byte>& out, const Token& value) {
    string(out, value.authority); append(out, value.number);
    string(out, value.coordinator); string(out, value.activation);
}
bool token(std::span<const std::byte> bytes, std::size_t& at, Token& value) {
    return string(bytes, at, value.authority) && take(bytes, at, value.number) &&
        string(bytes, at, value.coordinator) && string(bytes, at, value.activation) && valid_token(value, true);
}
bool successor(const Token& old, const Token& next) {
    return valid_token(old, true) && valid_token(next) && old.authority == next.authority &&
        old.number != std::numeric_limits<std::uint64_t>::max() && next.number == old.number + 1;
}
bool valid_state(const State& state) {
    if (!valid_token(state.current, true) || state.participants.size() > 4096) return false;
    if (!state.proposed) return state.participants.empty();
    if (!successor(state.current, *state.proposed) || state.participants.empty()) return false;
    std::string previous;
    for (const auto& id : state.participants) {
        if (id.empty() || id.size() > 255 || (!previous.empty() && id <= previous) ||
            !std::all_of(id.begin(), id.end(), [](unsigned char c) {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
            }) || id == "." || id == "..") return false;
        previous = id;
    }
    return true;
}
bool sync_fd(int fd) { int result; do { result = ::fsync(fd); } while (result < 0 && errno == EINTR); return result == 0; }
bool sync_directory(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return false;
    const bool okay = sync_fd(fd); return ::close(fd) == 0 && okay;
}
struct Shared {
    pid_t process{::getpid()};
    std::recursive_mutex mutex;
    int descriptor{-1};
    std::size_t depth{};
    bool poisoned{};
    std::uint64_t observed{};
    std::optional<Token> observed_token;
    std::optional<Token> observed_pending;
    ~Shared() { if (descriptor >= 0) ::close(descriptor); }
};
std::shared_ptr<Shared> shared_lock(const std::filesystem::path& path) {
    static std::mutex registry_mutex;
    static std::map<std::pair<pid_t, std::filesystem::path>, std::weak_ptr<Shared>> registry;
    std::lock_guard lock(registry_mutex);
    const auto key = std::make_pair(::getpid(), path);
    if (auto result = registry[key].lock()) return result;
    auto result = std::make_shared<Shared>();
    result->descriptor = ::open(path.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (result->descriptor < 0) throw Error(Status::unresolved);
    registry[key] = result;
    return result;
}
} // namespace
bool valid_token(const Token& value, bool allow_zero) noexcept {
    if (!valid_id(value.authority)) return false;
    if (value.number == 0) return allow_zero && value.coordinator.empty() && value.activation.empty();
    return valid_id(value.coordinator) && valid_id(value.activation);
}
std::vector<std::byte> encode_token(const Token& value) {
    if (!valid_token(value, true)) throw std::invalid_argument("invalid epoch token");
    std::vector<std::byte> out; token(out, value); return out;
}
std::optional<Token> decode_token(std::span<const std::byte> bytes) {
    Token value; std::size_t at{};
    return token(bytes, at, value) && at == bytes.size() ? std::optional<Token>{value} : std::nullopt;
}
std::vector<std::byte> encode_state(const State& value) {
    if (!valid_state(value)) throw std::invalid_argument("invalid epoch state");
    std::vector<std::byte> out;
    append(out, magic); append(out, std::uint16_t{1}); append(out, std::uint16_t{0});
    token(out, value.current); append(out, static_cast<std::uint16_t>(value.proposed.has_value()));
    if (value.proposed) token(out, *value.proposed);
    append(out, static_cast<std::uint32_t>(value.participants.size()));
    for (const auto& id : value.participants) string(out, id);
    append(out, integrity::crc32c(out));
    if (out.size() > max_size) throw std::length_error("epoch state too large");
    return out;
}
std::optional<State> decode_state(std::span<const std::byte> bytes) {
    if (bytes.size() < 30 || bytes.size() > max_size) return std::nullopt;
    const auto body = bytes.first(bytes.size() - 4);
    std::size_t crc_at = body.size(); std::uint32_t crc{}, signature{}, count{};
    std::size_t at{}; std::uint16_t version{}, reserved{}, pending{}; State value;
    if (!take(bytes, crc_at, crc) || crc != integrity::crc32c(body) ||
        !take(body, at, signature) || signature != magic || !take(body, at, version) || version != 1 ||
        !take(body, at, reserved) || reserved != 0 || !token(body, at, value.current) ||
        !take(body, at, pending) || pending > 1) return std::nullopt;
    if (pending) { Token next; if (!token(body, at, next)) return std::nullopt; value.proposed = next; }
    if (!take(body, at, count) || count > 4096) return std::nullopt;
    for (std::uint32_t i = 0; i < count; ++i) {
        std::string id; if (!string(body, at, id)) return std::nullopt; value.participants.push_back(std::move(id));
    }
    return at == body.size() && valid_state(value) ? std::optional<State>{value} : std::nullopt;
}
struct Guard::Impl {
    std::shared_ptr<Shared> shared;
    std::unique_lock<std::recursive_mutex> lock;
    Impl(std::shared_ptr<Shared> value, const std::filesystem::path& path)
        : shared(std::move(value)), lock(shared->mutex) {
        struct stat held{}, current{};
        if (shared->process != ::getpid() || shared->poisoned || ::fstat(shared->descriptor, &held) != 0 ||
            ::lstat(path.c_str(), &current) != 0 || !S_ISREG(current.st_mode) ||
            current.st_dev != held.st_dev || current.st_ino != held.st_ino) throw Error(Status::unresolved);
        if (shared->depth == 0) {
            int result; do { result = ::flock(shared->descriptor, LOCK_EX | LOCK_NB); } while (result < 0 && errno == EINTR);
            if (result != 0) throw Error(Status::unresolved);
        }
        ++shared->depth;
    }
    ~Impl() { if (--shared->depth == 0 && ::flock(shared->descriptor, LOCK_UN) != 0) shared->poisoned = true; }
};
Guard::Guard(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Guard::~Guard() = default;
struct Store::Impl {
    std::filesystem::path directory;
    Token minimum;
    FaultInjector fault;
    std::shared_ptr<Shared> shared;
};
void Store::provision(const std::filesystem::path& directory, const Token& initial) {
    if (!valid_token(initial, true)) throw std::invalid_argument("invalid epoch enrollment");
    const auto marker = directory.parent_path() / (directory.filename().string() + ".enrolled");
    if (!std::filesystem::exists(directory.parent_path()) || std::filesystem::exists(directory) ||
        std::filesystem::exists(marker))
        throw Error(Status::conflict);
    const auto bytes = encode_state({initial, std::nullopt, {}});
    const int enrollment = ::open(marker.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (enrollment < 0) throw Error(Status::unresolved);
    std::size_t enrolled{}; bool enrolled_okay = true;
    while (enrolled < bytes.size()) {
        const auto n = ::write(enrollment, bytes.data() + enrolled, bytes.size() - enrolled);
        if (n > 0) enrolled += static_cast<std::size_t>(n);
        else if (n < 0 && errno == EINTR) continue;
        else { enrolled_okay = false; break; }
    }
    enrolled_okay = sync_fd(enrollment) && enrolled_okay;
    if (::close(enrollment) != 0 || !enrolled_okay || !sync_directory(directory.parent_path()) ||
        !std::filesystem::create_directory(directory)) throw Error(Status::unresolved);
    for (const auto& name : {"lock", "record"}) {
        const int fd = ::open((directory / name).c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (fd < 0) throw Error(Status::unresolved);
        bool okay = true; std::size_t at{};
        if (std::string_view(name) == "record") while (at < bytes.size()) {
            const auto n = ::write(fd, bytes.data() + at, bytes.size() - at);
            if (n > 0) at += static_cast<std::size_t>(n);
            else if (n < 0 && errno == EINTR) continue;
            else { okay = false; break; }
        }
        okay = sync_fd(fd) && okay;
        if (::close(fd) != 0 || !okay) throw Error(Status::unresolved);
    }
    if (!sync_directory(directory) || !sync_directory(directory.parent_path())) throw Error(Status::unresolved);
}
Store::Store(std::filesystem::path directory, Token minimum, FaultInjector fault)
    : impl_(std::make_unique<Impl>()) {
    if (!valid_token(minimum, true)) throw std::invalid_argument("invalid epoch minimum");
    impl_->directory = std::filesystem::canonical(directory);
    impl_->minimum = std::move(minimum); impl_->fault = std::move(fault);
    impl_->shared = shared_lock(impl_->directory / "lock");
    static_cast<void>(snapshot());
}
Store::~Store() = default;
std::unique_ptr<Guard> Store::lock() const {
    return std::unique_ptr<Guard>(new Guard(std::make_unique<Guard::Impl>(impl_->shared, impl_->directory / "lock")));
}
State Store::read_locked() const {
    const auto marker = impl_->directory.parent_path() / (impl_->directory.filename().string() + ".enrolled");
    std::ifstream enrollment(marker, std::ios::binary);
    std::vector<std::byte> enrolled;
    for (char c; enrollment.get(c);) {
        if (enrolled.size() >= 1024) throw Error(Status::unresolved);
        enrolled.push_back(static_cast<std::byte>(c));
    }
    const auto minimum = decode_state(enrolled);
    if (!enrollment.eof() || !minimum || minimum->proposed ||
        minimum->current.authority != impl_->minimum.authority) throw Error(Status::unresolved);
    const auto path = impl_->directory / "record";
    struct stat record_status{}, enrollment_status{};
    if (::lstat(path.c_str(), &record_status) != 0 || !S_ISREG(record_status.st_mode) ||
        ::lstat(marker.c_str(), &enrollment_status) != 0 || !S_ISREG(enrollment_status.st_mode))
        throw Error(Status::unresolved);
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error || size > max_size) throw Error(Status::unresolved);
    std::ifstream input(path, std::ios::binary);
    std::vector<std::byte> bytes(static_cast<std::size_t>(size));
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size));
    if (!input || input.peek() != std::char_traits<char>::eof()) throw Error(Status::unresolved);
    const auto value = decode_state(bytes);
    if (!value || value->current.authority != impl_->minimum.authority ||
        value->current.number < impl_->minimum.number ||
        (value->current.number == impl_->minimum.number && value->current != impl_->minimum) ||
        value->current.number < minimum->current.number ||
        (value->current.number == minimum->current.number && value->current != minimum->current) ||
        value->current.number < impl_->shared->observed ||
        (impl_->shared->observed_token && value->current.number == impl_->shared->observed &&
         value->current != *impl_->shared->observed_token) ||
        (impl_->shared->observed_pending && value->current.number < impl_->shared->observed_pending->number &&
         value->proposed != impl_->shared->observed_pending) ||
        (impl_->shared->observed_pending && value->current.number == impl_->shared->observed_pending->number &&
         value->current != *impl_->shared->observed_pending)) throw Error(Status::unresolved);
    impl_->shared->observed = value->current.number;
    impl_->shared->observed_token = value->current;
    impl_->shared->observed_pending = value->proposed;
    return *value;
}
State Store::snapshot() const { auto guard = lock(); return read_locked(); }
std::unique_ptr<Guard> Store::authorize(const Token& request) const {
    auto guard = lock(); const auto state = read_locked();
    if (state.proposed) throw Error(Status::unresolved);
    if (!valid_token(request) || request.authority != state.current.authority) throw Error(Status::unauthorized);
    if (request != state.current) throw Error(request.number < state.current.number ? Status::stale : Status::conflict);
    return guard;
}
Status Store::publish_locked(const State& state) {
    const auto bytes = encode_state(state);
    auto injected = [&](FaultPoint point) { return impl_->fault && impl_->fault(point); };
    if (injected(FaultPoint::creation)) return Status::unresolved;
    const auto temporary = impl_->directory / "record.tmp";
    const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) return Status::unresolved;
    bool okay = !injected(FaultPoint::write); std::size_t at{};
    while (okay && at < bytes.size()) {
        const auto n = ::write(fd, bytes.data() + at, bytes.size() - at);
        if (n > 0) at += static_cast<std::size_t>(n);
        else if (n < 0 && errno == EINTR) continue;
        else okay = false;
    }
    if (injected(FaultPoint::file_sync)) okay = false;
    if (okay) okay = sync_fd(fd);
    if (::close(fd) != 0) okay = false;
    if (!okay || injected(FaultPoint::rename)) return Status::unresolved;
    std::error_code error; std::filesystem::rename(temporary, impl_->directory / "record", error);
    if (error) return Status::unresolved;
    impl_->shared->observed = state.current.number;
    impl_->shared->observed_token = state.current;
    impl_->shared->observed_pending = state.proposed;
    if (injected(FaultPoint::directory_sync) || !sync_directory(impl_->directory)) {
        impl_->shared->poisoned = true; return Status::unresolved;
    }
    return Status::accepted;
}
Result Store::advance(const Token& expected, const Token& desired) {
    try {
        auto guard = lock(); const auto state = read_locked();
        if (!successor(expected, desired) || state.proposed) return {Status::conflict, state.current};
        if (state.current == desired) return {Status::accepted, desired};
        if (state.current.number > desired.number) return {Status::stale, state.current};
        if (state.current != expected) return {Status::conflict, state.current};
        const auto status = publish_locked({desired, std::nullopt, {}});
        return {status, status == Status::accepted ? std::optional<Token>{desired} : std::nullopt};
    } catch (const Error& error) { return {error.status(), std::nullopt}; }
}
Result Store::begin(const Token& desired, std::vector<std::string> participants) {
    try {
        auto guard = lock(); const auto state = read_locked();
        if (state.proposed) return {*state.proposed == desired && state.participants == participants ?
            Status::accepted : Status::conflict, state.proposed};
        State next{state.current, desired, std::move(participants)};
        if (!valid_state(next)) return {Status::invalid, std::nullopt};
        const auto status = publish_locked(next);
        if (status != Status::accepted) impl_->shared->poisoned = true;
        return {status, status == Status::accepted ? std::optional<Token>{desired} : std::nullopt};
    } catch (const Error& error) { return {error.status(), std::nullopt}; }
}
Result Store::finish(const Token& desired) {
    try {
        auto guard = lock(); const auto state = read_locked();
        if (!state.proposed) return {state.current == desired ? Status::accepted : Status::conflict, state.current};
        if (*state.proposed != desired) return {Status::conflict, state.current};
        const auto status = publish_locked({desired, std::nullopt, {}});
        return {status, status == Status::accepted ? std::optional<Token>{desired} : std::nullopt};
    } catch (const Error& error) { return {error.status(), std::nullopt}; }
}
const std::filesystem::path& Store::directory() const noexcept { return impl_->directory; }
Coordinator::Coordinator(Store& store, std::vector<std::shared_ptr<Participant>> participants)
    : store_(store), participants_(std::move(participants)) {
    if (participants_.empty() || participants_.size() > 4096 ||
        std::any_of(participants_.begin(), participants_.end(), [](const auto& p) { return !p; }))
        throw std::invalid_argument("epoch activation requires participants");
    std::sort(participants_.begin(), participants_.end(), [](const auto& a, const auto& b) { return a->id() < b->id(); });
    for (std::size_t i = 1; i < participants_.size(); ++i)
        if (participants_[i - 1]->id() == participants_[i]->id()) throw std::invalid_argument("duplicate epoch participant");
}
Result Coordinator::activate(std::string coordinator, std::string activation) {
    try {
        const auto state = store_.snapshot();
        if (!state.proposed && state.current.number != 0 && state.current.coordinator == coordinator &&
            state.current.activation == activation) return {Status::accepted, state.current};
        if (state.proposed || state.current.number == std::numeric_limits<std::uint64_t>::max())
            return {Status::conflict, std::nullopt};
        Token desired{state.current.authority, state.current.number + 1, std::move(coordinator), std::move(activation)};
        std::vector<std::string> ids; for (const auto& node : participants_) ids.emplace_back(node->id());
        const auto begun = store_.begin(desired, std::move(ids));
        if (begun.status != Status::accepted) return begun;
        return confirm(store_.snapshot(), true);
    } catch (const Error& error) { return {error.status(), std::nullopt}; }
}
Result Coordinator::reconcile() {
    try {
        const auto state = store_.snapshot();
        return state.proposed ? confirm(state, false) : Result{Status::conflict, state.current};
    } catch (const Error& error) { return {error.status(), std::nullopt}; }
}
Result Coordinator::resume_activation() {
    try { return confirm(store_.snapshot(), true); }
    catch (const Error& error) { return {error.status(), std::nullopt}; }
}
Result Coordinator::confirm(const State& state, bool activate_nodes) {
    if (!state.proposed) return {Status::unresolved, std::nullopt};
    std::vector<std::string> ids; for (const auto& node : participants_) ids.emplace_back(node->id());
    if (ids != state.participants) return {Status::conflict, std::nullopt};
    bool confirmed = true;
    for (const auto& node : participants_) {
        try {
            const auto result = activate_nodes ? node->activate_epoch(state.current, *state.proposed) : node->query_epoch();
            if (result.status != Status::accepted || result.token != state.proposed) confirmed = false;
        } catch (...) { confirmed = false; }
    }
    return confirmed ? store_.finish(*state.proposed) : Result{Status::unresolved, std::nullopt};
}
} // namespace atlasfs::epoch
