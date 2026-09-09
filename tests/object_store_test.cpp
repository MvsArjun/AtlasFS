#include "atlasfs/checksum.hpp"
#include "atlasfs/manifest.hpp"
#include "atlasfs/object_store.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
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

std::vector<std::byte> read_file(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    std::vector<std::byte> result;
    for (char value; input.get(value);) {
        result.push_back(static_cast<std::byte>(static_cast<unsigned char>(value)));
    }
    return result;
}

ObjectManifest manifest_for(const ObjectStore& store, std::string_view id) {
    const auto encoded = read_file(store.objects_directory() / id);
    const auto manifest = decode_manifest(encoded);
    if (!manifest) {
        throw std::runtime_error("test could not decode object manifest");
    }
    return *manifest;
}

void write_file(const std::filesystem::path& path, const std::vector<std::byte>& data) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(data.data()),
                 static_cast<std::streamsize>(data.size()));
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
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator(store.chunks_directory()),
                            std::filesystem::directory_iterator()),
              1);
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
    const auto path = directory.path() / "objects" / "committed" / "x";
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
    EXPECT_EQ(store.remove("x"), StoreError::corrupt_object);
}

TEST(ObjectStore, CleansTemporaryFileWhenAtomicRenameFails) {
    TemporaryDirectory directory;
    ObjectStore store(directory.path());
    std::filesystem::create_directory(store.objects_directory() / "x");
    EXPECT_EQ(put(store, "x", bytes({1, 2, 3})), StoreError::filesystem_error);
    for (const auto& entry : std::filesystem::directory_iterator(store.objects_directory())) {
        EXPECT_EQ(entry.path().filename(), "x");
    }
    EXPECT_TRUE(std::filesystem::is_empty(store.chunks_directory()));
}

TEST(ObjectStore, ConstructorReportsFilesystemFailure) {
    TemporaryDirectory directory;
    const auto file_path = directory.path() / "regular-file";
    std::ofstream(file_path).put('x');
    EXPECT_THROW(ObjectStore(file_path), std::exception);
}

TEST(ObjectStore, ChunksAllBoundarySizesAndReassemblesExactly) {
    const std::vector<std::size_t> sizes{0, 1, 3, 4, 5, 8, 9, 17};
    for (const auto size : sizes) {
        TemporaryDirectory directory;
        ObjectStore store(directory.path(), 1024, 4);
        std::vector<std::byte> input(size);
        for (std::size_t index = 0; index < size; ++index) {
            input[index] = static_cast<std::byte>(index & 0xffU);
        }
        ASSERT_EQ(put(store, "object", input), StoreError::none) << size;
        EXPECT_TRUE(std::filesystem::is_empty(store.transactions_directory())) << size;
        EXPECT_EQ(store.get("object").data, input) << size;
        const auto manifest = manifest_for(store, "object");
        const auto expected = size == 0 ? 0U : 1U + (size - 1U) / 4U;
        EXPECT_EQ(manifest.chunks.size(), expected) << size;
    }
}

TEST(ObjectStore, PersistsLargeChunkedObjectAcrossStoreRestart) {
    TemporaryDirectory directory;
    std::vector<std::byte> input(1024U * 1024U);
    for (std::size_t index = 0; index < input.size(); ++index) {
        input[index] = static_cast<std::byte>(index & 0xffU);
    }
    {
        ObjectStore store(directory.path(), object_protocol::kMaxObjectDataSize, 64U * 1024U);
        ASSERT_EQ(put(store, "large", input), StoreError::none);
    }
    ObjectStore reopened(directory.path(), object_protocol::kMaxObjectDataSize, 64U * 1024U);
    EXPECT_EQ(reopened.get("large").data, input);
}

TEST(ObjectStore, DetectsMissingTruncatedExtendedAndModifiedChunks) {
    for (unsigned corruption = 0; corruption < 5; ++corruption) {
        TemporaryDirectory directory;
        ObjectStore store(directory.path(), 1024, 4);
        ASSERT_EQ(put(store, "object", bytes({1, 2, 3, 4, 5, 6})), StoreError::none);
        const auto manifest = manifest_for(store, "object");
        const auto chunk = store.chunks_directory() / manifest.generation_id / "chunk_000000";
        if (corruption == 0) {
            std::filesystem::remove(chunk);
        } else if (corruption == 1) {
            std::filesystem::resize_file(chunk, 3);
        } else if (corruption == 2) {
            std::ofstream output(chunk, std::ios::binary | std::ios::app);
            output.put('x');
        } else if (corruption == 3) {
            std::fstream output(chunk, std::ios::binary | std::ios::in | std::ios::out);
            output.put('x');
        } else {
            std::ofstream output(store.chunks_directory() / manifest.generation_id / "unexpected");
            output.put('x');
        }
        EXPECT_EQ(store.get("object").error, StoreError::corrupt_object) << corruption;
    }
}

TEST(ObjectStore, DetectsProtectedMetadataAndWholeObjectIntegrityFailures) {
    for (unsigned corruption = 0; corruption < 3; ++corruption) {
        TemporaryDirectory directory;
        ObjectStore store(directory.path(), 1024, 4);
        ASSERT_EQ(put(store, "object", bytes({1, 2, 3, 4, 5, 6, 7, 8})), StoreError::none);
        auto manifest = manifest_for(store, "object");
        const auto generation = store.chunks_directory() / manifest.generation_id;
        if (corruption == 0) {
            manifest.object_checksum ^= 1U;
            const auto encoded = encode_manifest(manifest);
            write_file(store.objects_directory() / "object", encoded);
            write_file(generation / "manifest", encoded);
        } else if (corruption == 1) {
            manifest.chunks[0].checksum ^= 1U;
            const auto encoded = encode_manifest(manifest);
            write_file(store.objects_directory() / "object", encoded);
            write_file(generation / "manifest", encoded);
        } else {
            const auto first = generation / "chunk_000000";
            const auto second = generation / "chunk_000001";
            const auto temporary = generation / "swap";
            std::filesystem::rename(first, temporary);
            std::filesystem::rename(second, first);
            std::filesystem::rename(temporary, second);
        }
        EXPECT_EQ(store.get("object").error, StoreError::corrupt_object) << corruption;
    }
}

TEST(ObjectStore, FailedPutAtEveryInjectedStagePreservesOldObjectAndCleansState) {
    const std::vector<PutFaultPoint> points{
        PutFaultPoint::checksum_calculation, PutFaultPoint::chunk_creation,
        PutFaultPoint::chunk_write, PutFaultPoint::chunk_sync,
        PutFaultPoint::manifest_creation, PutFaultPoint::manifest_write,
        PutFaultPoint::manifest_sync, PutFaultPoint::generation_rename,
        PutFaultPoint::commit_rename};
    for (const auto point : points) {
        TemporaryDirectory directory;
        const auto old_data = bytes({1, 2, 3, 4, 5});
        {
            ObjectStore initial(directory.path(), 1024, 4);
            ASSERT_EQ(put(initial, "object", old_data), StoreError::none);
        }
        ObjectStore failing(directory.path(), 1024, 4,
                            [point](PutFaultPoint current) { return current == point; });
        EXPECT_EQ(put(failing, "object", bytes({9, 8, 7, 6, 5, 4})),
                  StoreError::filesystem_error);
        EXPECT_EQ(failing.get("object").data, old_data);
        EXPECT_TRUE(std::filesystem::is_empty(failing.transactions_directory()));
        std::size_t generations = 0;
        for ([[maybe_unused]] const auto& entry :
             std::filesystem::directory_iterator(failing.chunks_directory())) {
            ++generations;
        }
        EXPECT_EQ(generations, 1U);
    }
}

TEST(ObjectStore, StartupRemovesTransactionsTemporaryManifestsAndOrphanGenerations) {
    TemporaryDirectory directory;
    {
        ObjectStore store(directory.path(), 1024, 4);
        ASSERT_EQ(put(store, "kept", bytes({1, 2, 3, 4, 5})), StoreError::none);
        std::filesystem::create_directories(store.transactions_directory() / "leftover");
        std::filesystem::create_directories(store.chunks_directory() / "generation-orphan");
        std::ofstream(store.objects_directory() / ".atlasfs-tmp-leftover").put('x');
    }
    ObjectStore recovered(directory.path(), 1024, 4);
    EXPECT_TRUE(std::filesystem::is_empty(recovered.transactions_directory()));
    EXPECT_FALSE(std::filesystem::exists(recovered.chunks_directory() / "generation-orphan"));
    EXPECT_FALSE(std::filesystem::exists(
        recovered.objects_directory() / ".atlasfs-tmp-leftover"));
    EXPECT_EQ(recovered.get("kept").data, bytes({1, 2, 3, 4, 5}));
}

}  // namespace
}  // namespace atlasfs::storage
