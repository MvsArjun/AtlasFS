#include "atlasfs/checksum.hpp"
#include "atlasfs/object_store.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace atlasfs::storage {
namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        static std::atomic_uint64_t sequence{0};
        path_ = std::filesystem::temp_directory_path() /
                ("atlasfs-test-" + std::to_string(static_cast<unsigned long long>(::getpid())) +
                 "-" + std::to_string(sequence.fetch_add(1)));
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

std::vector<std::byte> bytes(std::initializer_list<unsigned char> values) {
    std::vector<std::byte> result;
    for (const auto value : values) {
        result.push_back(static_cast<std::byte>(value));
    }
    return result;
}

StoreError put(ObjectStore& store, std::string_view id, const std::vector<std::byte>& data) {
    return store.put(id, data, integrity::crc32c(data));
}

TEST(ObjectStore, PutThenGetPersistsBinaryObject) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    const auto input = bytes({0, 1, 0xff, 0, 2});
    ASSERT_EQ(put(store, "binary", input), StoreError::none);
    const auto result = store.get("binary");
    EXPECT_EQ(result.error, StoreError::none);
    EXPECT_EQ(result.data, input);
    EXPECT_EQ(result.checksum, integrity::crc32c(input));
}

TEST(ObjectStore, SupportsEmptyObjects) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    ASSERT_EQ(put(store, "empty", {}), StoreError::none);
    const auto result = store.get("empty");
    EXPECT_EQ(result.error, StoreError::none);
    EXPECT_TRUE(result.data.empty());
}

TEST(ObjectStore, OverwriteAtomicallyReplacesExistingObject) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    ASSERT_EQ(put(store, "x", bytes({1, 2})), StoreError::none);
    ASSERT_EQ(put(store, "x", bytes({3, 4, 5})), StoreError::none);
    EXPECT_EQ(store.get("x").data, bytes({3, 4, 5}));
}

TEST(ObjectStore, FailedOverwritePreservesExistingObject) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    ASSERT_EQ(put(store, "x", bytes({1, 2})), StoreError::none);
    EXPECT_EQ(store.put("x", bytes({9}), 0), StoreError::checksum_mismatch);
    EXPECT_EQ(store.get("x").data, bytes({1, 2}));
}

TEST(ObjectStore, DeleteIsStateIdempotentButReportsNotFound) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    ASSERT_EQ(put(store, "x", bytes({1})), StoreError::none);
    EXPECT_EQ(store.remove("x"), StoreError::none);
    EXPECT_EQ(store.get("x").error, StoreError::not_found);
    EXPECT_EQ(store.remove("x"), StoreError::not_found);
}

TEST(ObjectStore, RejectsUnsafeObjectIdentifiers) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    const std::vector<std::string> invalid{"", ".", "..", "../x", "a/b", "a\\b",
                                           "/absolute", "C:\\absolute", "white space",
                                           ".atlasfs-tmp-1-2"};
    for (const auto& id : invalid) {
        EXPECT_EQ(put(store, id, bytes({1})), StoreError::invalid_object_id) << id;
        EXPECT_EQ(store.get(id).error, StoreError::invalid_object_id) << id;
        EXPECT_EQ(store.remove(id), StoreError::invalid_object_id) << id;
    }
}

TEST(ObjectStore, ReportsMissingObject) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    EXPECT_EQ(store.get("missing").error, StoreError::not_found);
}

TEST(ObjectStore, RejectsSuppliedChecksumMismatchWithoutTemporaryFiles) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    EXPECT_EQ(store.put("x", bytes({1, 2}), 0), StoreError::checksum_mismatch);
    EXPECT_TRUE(std::filesystem::is_empty(store.objects_directory()));
}

TEST(ObjectStore, DetectsPersistedDataCorruptionAfterRestart) {
    TemporaryDirectory directory;
    {
        ObjectStore store(directory.path());
        ASSERT_EQ(put(store, "x", bytes({1, 2, 3})), StoreError::none);
    }
    const auto path = directory.path() / "objects" / "x";
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file);
    file.seekp(-1, std::ios::end);
    file.put(static_cast<char>(0xff));
    file.close();

    ObjectStore reopened(directory.path());
    EXPECT_EQ(reopened.get("x").error, StoreError::corrupt_object);
}

TEST(ObjectStore, RejectsUnexpectedDirectoryEntry) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    std::filesystem::create_directory(store.objects_directory() / "x");
    EXPECT_EQ(store.get("x").error, StoreError::corrupt_object);
    EXPECT_EQ(store.remove("x"), StoreError::filesystem_error);
}

TEST(ObjectStore, CleansTemporaryFileWhenAtomicRenameFails) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    std::filesystem::create_directory(store.objects_directory() / "x");
    EXPECT_EQ(put(store, "x", bytes({1, 2, 3})), StoreError::filesystem_error);
    for (const auto& entry : std::filesystem::directory_iterator(store.objects_directory())) {
        EXPECT_EQ(entry.path().filename(), "x");
    }
}

TEST(ObjectStore, ConstructorReportsFilesystemFailure) {
    TemporaryDirectory directory;
    const auto file_path = directory.path() / "regular-file";
    std::ofstream(file_path).put('x');
    EXPECT_THROW(ObjectStore(file_path), std::exception);
}

}  // namespace
}  // namespace atlasfs::storage
