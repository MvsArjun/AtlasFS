#include "atlasfs/object_store.hpp"

#include "atlasfs/checksum.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace atlasfs::storage {
namespace {

constexpr std::array<std::byte, 4> kObjectMagic{
    static_cast<std::byte>('A'), static_cast<std::byte>('O'),
    static_cast<std::byte>('B'), static_cast<std::byte>('J')};
constexpr std::uint16_t kObjectVersion = 1;
constexpr std::size_t kObjectHeaderSize = 20;

class FileDescriptor {
public:
    explicit FileDescriptor(int descriptor = -1) noexcept : descriptor_(descriptor) {}
    ~FileDescriptor() { reset(); }
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    FileDescriptor(FileDescriptor&& other) noexcept : descriptor_(other.release()) {}
    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            reset();
            descriptor_ = other.release();
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept { return descriptor_; }
    [[nodiscard]] bool valid() const noexcept { return descriptor_ >= 0; }

    int release() noexcept {
        const int descriptor = descriptor_;
        descriptor_ = -1;
        return descriptor;
    }

    int close_now() noexcept {
        if (!valid()) {
            return 0;
        }
        const int descriptor = descriptor_;
        descriptor_ = -1;
        return ::close(descriptor);
    }

    void reset() noexcept {
        if (valid()) {
            const int descriptor = descriptor_;
            descriptor_ = -1;
            ::close(descriptor);
        }
    }

private:
    int descriptor_;
};

class TemporaryPath {
public:
    explicit TemporaryPath(std::filesystem::path path) : path_(std::move(path)) {}
    ~TemporaryPath() {
        if (active_) {
            std::error_code ignored;
            std::filesystem::remove(path_, ignored);
        }
    }
    TemporaryPath(const TemporaryPath&) = delete;
    TemporaryPath& operator=(const TemporaryPath&) = delete;

    [[nodiscard]] const std::filesystem::path& get() const noexcept { return path_; }
    void release() noexcept { active_ = false; }

private:
    std::filesystem::path path_;
    bool active_{true};
};

template <typename UInt>
void write_integer(std::span<std::byte> output, std::size_t offset, UInt value) {
    for (std::size_t index = 0; index < sizeof(UInt); ++index) {
        const auto shift = static_cast<unsigned>((sizeof(UInt) - index - 1U) * 8U);
        output[offset + index] = static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

template <typename UInt>
UInt read_integer(std::span<const std::byte> input, std::size_t offset) {
    UInt value = 0;
    for (std::size_t index = 0; index < sizeof(UInt); ++index) {
        value = static_cast<UInt>((value << 8U) |
                                  std::to_integer<unsigned>(input[offset + index]));
    }
    return value;
}

std::array<std::byte, kObjectHeaderSize> make_header(std::size_t data_size,
                                                     std::uint32_t checksum) {
    std::array<std::byte, kObjectHeaderSize> header{};
    std::copy(kObjectMagic.begin(), kObjectMagic.end(), header.begin());
    write_integer<std::uint16_t>(header, 4, kObjectVersion);
    write_integer<std::uint16_t>(header, 6, 0);
    write_integer<std::uint64_t>(header, 8, static_cast<std::uint64_t>(data_size));
    write_integer<std::uint32_t>(header, 16, checksum);
    return header;
}

bool write_all(int descriptor, std::span<const std::byte> data) noexcept {
    std::size_t written = 0;
    while (written < data.size()) {
        const auto result = ::write(descriptor, data.data() + written, data.size() - written);
        if (result > 0) {
            written += static_cast<std::size_t>(result);
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

bool read_all(int descriptor, std::span<std::byte> data) noexcept {
    std::size_t read_count = 0;
    while (read_count < data.size()) {
        const auto result = ::read(descriptor, data.data() + read_count,
                                   data.size() - read_count);
        if (result > 0) {
            read_count += static_cast<std::size_t>(result);
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

bool sync_file(int descriptor) noexcept {
    int result;
    do {
        result = ::fdatasync(descriptor);
    } while (result < 0 && errno == EINTR);
    return result == 0;
}

}  // namespace

ObjectStore::ObjectStore(std::filesystem::path root, std::size_t max_object_size)
    : max_object_size_(max_object_size) {
    if (max_object_size > object_protocol::kMaxObjectDataSize) {
        throw std::invalid_argument("object size limit exceeds the protocol-safe maximum");
    }
    std::error_code error;
    std::filesystem::create_directories(root / "objects", error);
    if (error) {
        throw std::system_error(error, "create AtlasFS object directory");
    }
    objects_directory_ = std::filesystem::canonical(root / "objects", error);
    if (error || !std::filesystem::is_directory(objects_directory_, error) || error) {
        throw std::runtime_error("AtlasFS object path is not a usable directory");
    }
}

StoreError ObjectStore::put(std::string_view object_id, std::span<const std::byte> data,
                            std::uint32_t supplied_checksum) {
    if (!valid_object_id(object_id)) {
        return StoreError::invalid_object_id;
    }
    if (data.size() > max_object_size_) {
        return StoreError::payload_too_large;
    }
    const auto actual_checksum = integrity::crc32c(data);
    if (actual_checksum != supplied_checksum) {
        return StoreError::checksum_mismatch;
    }

    FileDescriptor file;
    std::filesystem::path temporary_path;
    for (unsigned attempt = 0; attempt < 100; ++attempt) {
        temporary_path = next_temporary_path();
        file = FileDescriptor(::open(temporary_path.c_str(),
                                     O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
        if (file.valid() || errno != EEXIST) {
            break;
        }
    }
    if (!file.valid()) {
        return StoreError::filesystem_error;
    }
    TemporaryPath cleanup(temporary_path);

    const auto header = make_header(data.size(), actual_checksum);
    if (!write_all(file.get(), header) || !write_all(file.get(), data) ||
        !sync_file(file.get()) || file.close_now() != 0) {
        return StoreError::filesystem_error;
    }

    std::error_code error;
    std::filesystem::rename(temporary_path, object_path(object_id), error);
    if (error) {
        return StoreError::filesystem_error;
    }
    cleanup.release();
    return sync_directory();
}

GetResult ObjectStore::get(std::string_view object_id) const {
    if (!valid_object_id(object_id)) {
        return {StoreError::invalid_object_id, {}, 0};
    }
    FileDescriptor file(::open(object_path(object_id).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (!file.valid()) {
        return {errno == ENOENT ? StoreError::not_found : StoreError::filesystem_error, {}, 0};
    }

    struct stat metadata {};
    if (::fstat(file.get(), &metadata) != 0) {
        return {StoreError::filesystem_error, {}, 0};
    }
    if (!S_ISREG(metadata.st_mode) || metadata.st_size < 0 ||
        static_cast<std::uint64_t>(metadata.st_size) < kObjectHeaderSize ||
        static_cast<std::uint64_t>(metadata.st_size) >
            static_cast<std::uint64_t>(max_object_size_) + kObjectHeaderSize) {
        return {StoreError::corrupt_object, {}, 0};
    }

    std::array<std::byte, kObjectHeaderSize> header{};
    if (!read_all(file.get(), header) ||
        !std::equal(kObjectMagic.begin(), kObjectMagic.end(), header.begin()) ||
        read_integer<std::uint16_t>(header, 4) != kObjectVersion ||
        read_integer<std::uint16_t>(header, 6) != 0) {
        return {StoreError::corrupt_object, {}, 0};
    }
    const auto stored_size = read_integer<std::uint64_t>(header, 8);
    const auto stored_checksum = read_integer<std::uint32_t>(header, 16);
    const auto file_size = static_cast<std::uint64_t>(metadata.st_size);
    if (stored_size > max_object_size_ || stored_size != file_size - kObjectHeaderSize) {
        return {StoreError::corrupt_object, {}, 0};
    }

    std::vector<std::byte> data(static_cast<std::size_t>(stored_size));
    if (!read_all(file.get(), data)) {
        return {StoreError::corrupt_object, {}, 0};
    }
    std::byte trailing{};
    ssize_t trailing_result;
    do {
        trailing_result = ::read(file.get(), &trailing, 1);
    } while (trailing_result < 0 && errno == EINTR);
    if (trailing_result < 0) {
        return {StoreError::filesystem_error, {}, 0};
    }
    if (trailing_result > 0 || integrity::crc32c(data) != stored_checksum) {
        return {StoreError::corrupt_object, {}, 0};
    }
    return {StoreError::none, std::move(data), stored_checksum};
}

StoreError ObjectStore::remove(std::string_view object_id) {
    if (!valid_object_id(object_id)) {
        return StoreError::invalid_object_id;
    }
    if (::unlink(object_path(object_id).c_str()) != 0) {
        return errno == ENOENT ? StoreError::not_found : StoreError::filesystem_error;
    }
    return sync_directory();
}

const std::filesystem::path& ObjectStore::objects_directory() const noexcept {
    return objects_directory_;
}

bool ObjectStore::valid_object_id(std::string_view object_id) noexcept {
    if (object_id.empty() || object_id.size() > object_protocol::kMaxObjectIdSize ||
        object_id == "." || object_id == ".." ||
        object_id.starts_with(".atlasfs-tmp-")) {
        return false;
    }
    for (const unsigned char character : object_id) {
        const bool allowed = (character >= 'a' && character <= 'z') ||
                             (character >= 'A' && character <= 'Z') ||
                             (character >= '0' && character <= '9') || character == '-' ||
                             character == '_' || character == '.';
        if (!allowed) {
            return false;
        }
    }
    return true;
}

std::filesystem::path ObjectStore::object_path(std::string_view object_id) const {
    return objects_directory_ / std::string(object_id);
}

std::filesystem::path ObjectStore::next_temporary_path() {
    const auto sequence = temporary_counter_.fetch_add(1);
    return objects_directory_ /
           (".atlasfs-tmp-" + std::to_string(static_cast<unsigned long long>(::getpid())) +
            "-" + std::to_string(sequence));
}

StoreError ObjectStore::sync_directory() const noexcept {
    FileDescriptor directory(::open(objects_directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (!directory.valid()) {
        return StoreError::filesystem_error;
    }
    int result;
    do {
        result = ::fsync(directory.get());
    } while (result < 0 && errno == EINTR);
    return result == 0 ? StoreError::none : StoreError::filesystem_error;
}

}  // namespace atlasfs::storage
