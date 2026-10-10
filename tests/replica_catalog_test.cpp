#include "atlasfs/checksum.hpp"
#include "atlasfs/replica_catalog.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace atlasfs::metadata {
namespace {

class TempRoot {
public:
    TempRoot() {
        static std::atomic_uint64_t sequence{0};
        path_ = std::filesystem::temp_directory_path() /
                ("atlasfs-replica-catalog-test-" +
                 std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
                 std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }

    ~TempRoot() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

constexpr std::uint32_t kChunkChecksum = 0x11223344U;

CatalogReplica replica(std::string node_id,
                       CatalogReplicaState state = CatalogReplicaState::present,
                       std::uint64_t evidence_version = 7,
                       std::uint32_t checksum = kChunkChecksum) {
    if (state == CatalogReplicaState::missing ||
        state == CatalogReplicaState::unavailable) {
        checksum = 0;
    }
    return {std::move(node_id), state, checksum, evidence_version};
}

ChunkIdentity identity(std::string object_id = "object",
                       std::string generation_id = "generation-1",
                       std::uint32_t chunk_index = 0) {
    return {std::move(object_id), std::move(generation_id), chunk_index};
}

storage::ChunkMetadata chunk(std::uint32_t index = 0,
                             std::uint32_t length = 4,
                             std::uint32_t checksum = kChunkChecksum) {
    return {index, length, checksum};
}

ReplicaCatalogSnapshot snapshot() {
    return {0x0102030405060708ULL,
            {{identity(), chunk(), 0x0102030405060708ULL,
              {replica("node-a"),
               replica("node-b", CatalogReplicaState::missing, 8),
               replica("node-c", CatalogReplicaState::corrupt, 9, 0x55667788U),
               replica("node-d", CatalogReplicaState::unavailable, 10)}}}};
}

void rewrite_crc(std::vector<std::byte>& bytes) {
    const auto checksum =
        integrity::crc32c(std::span(bytes).first(bytes.size() - 4U));
    for (unsigned index = 0; index < 4; ++index) {
        bytes[bytes.size() - 4U + index] = static_cast<std::byte>(
            (checksum >> ((3U - index) * 8U)) & 0xffU);
    }
}

void write_u32(std::vector<std::byte>& bytes, std::size_t offset,
               std::uint32_t value) {
    for (unsigned index = 0; index < 4; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> ((3U - index) * 8U)) & 0xffU);
    }
}

std::size_t first_replica_count_offset(const ReplicaCatalogSnapshot& value) {
    const auto& record = value.records.front();
    return 24U + 2U + record.identity.object_id.size() + 2U +
           record.identity.generation_id.size() + 4U + 4U + 4U + 8U;
}

std::size_t first_replica_state_offset(const ReplicaCatalogSnapshot& value) {
    return first_replica_count_offset(value) + 4U + 2U +
           value.records.front().replicas.front().node_id.size();
}

std::size_t first_generation_id_offset(const ReplicaCatalogSnapshot& value) {
    return 24U + 2U + value.records.front().identity.object_id.size() + 2U;
}

std::vector<std::byte> read_bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    const auto size = std::filesystem::file_size(path);
    std::vector<std::byte> bytes(static_cast<std::size_t>(size));
    if (!bytes.empty()) {
        input.read(reinterpret_cast<char*>(bytes.data()),
                   static_cast<std::streamsize>(bytes.size()));
    }
    return bytes;
}

void write_bytes(const std::filesystem::path& path,
                 std::span<const std::byte> bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!bytes.empty()) {
        output.write(reinterpret_cast<const char*>(bytes.data()),
                     static_cast<std::streamsize>(bytes.size()));
    }
}

TEST(ReplicaCatalogSerialization, RoundTripsDeterministicArpcRepresentation) {
    const auto value = snapshot();
    const auto first = encode_replica_catalog(value);
    const auto second = encode_replica_catalog(value);

    EXPECT_EQ(first, second);
    ASSERT_GE(first.size(), 24U + 4U);
    EXPECT_EQ(std::to_integer<unsigned>(first[0]), 'A');
    EXPECT_EQ(std::to_integer<unsigned>(first[1]), 'R');
    EXPECT_EQ(std::to_integer<unsigned>(first[2]), 'P');
    EXPECT_EQ(std::to_integer<unsigned>(first[3]), 'C');
    EXPECT_EQ(std::to_integer<unsigned>(first[4]), 0U);
    EXPECT_EQ(std::to_integer<unsigned>(first[5]), 1U);
    for (unsigned index = 0; index < 8; ++index) {
        EXPECT_EQ(std::to_integer<unsigned>(first[8U + index]), index + 1U);
    }
    EXPECT_EQ(std::to_integer<unsigned>(first[19]), 1U);
    EXPECT_EQ(decode_replica_catalog(first), value);
    EXPECT_EQ(decode_replica_catalog(encode_replica_catalog({0, {}})),
              (ReplicaCatalogSnapshot{0, {}}));
}

TEST(ReplicaCatalogSerialization, RejectsTruncationTrailingBytesAndBadCrc) {
    const auto encoded = encode_replica_catalog(snapshot());

    auto truncated = encoded;
    truncated.pop_back();
    EXPECT_FALSE(decode_replica_catalog(truncated));

    auto trailing = encoded;
    trailing.push_back(std::byte{0});
    EXPECT_FALSE(decode_replica_catalog(trailing));

    auto valid_crc_trailing = encoded;
    valid_crc_trailing.insert(valid_crc_trailing.end() - 4, std::byte{0});
    rewrite_crc(valid_crc_trailing);
    EXPECT_FALSE(decode_replica_catalog(valid_crc_trailing));

    auto corrupt = encoded;
    corrupt[24] ^= std::byte{1};
    EXPECT_FALSE(decode_replica_catalog(corrupt));
}

TEST(ReplicaCatalogSerialization, RejectsReservedStateAndCountViolations) {
    const auto value = snapshot();
    const auto encoded = encode_replica_catalog(value);

    for (const auto [offset, replacement] : {
             std::pair<std::size_t, std::byte>{0U, std::byte{0}},
             std::pair<std::size_t, std::byte>{5U, std::byte{2}}}) {
        auto changed = encoded;
        changed[offset] = replacement;
        rewrite_crc(changed);
        EXPECT_FALSE(decode_replica_catalog(changed));
    }

    for (const auto offset : {6U, 20U,
                              static_cast<unsigned>(
                                  first_replica_state_offset(value) + 1U),
                              static_cast<unsigned>(
                                  first_replica_state_offset(value) + 2U)}) {
        auto changed = encoded;
        changed[offset] = std::byte{1};
        rewrite_crc(changed);
        EXPECT_FALSE(decode_replica_catalog(changed));
    }

    auto invalid_state = encoded;
    invalid_state[first_replica_state_offset(value)] = std::byte{99};
    rewrite_crc(invalid_state);
    EXPECT_FALSE(decode_replica_catalog(invalid_state));

    auto excessive_records = encoded;
    write_u32(excessive_records, 16U, kMaxReplicaCatalogRecords + 1U);
    rewrite_crc(excessive_records);
    EXPECT_FALSE(decode_replica_catalog(excessive_records));

    auto excessive_replicas = encoded;
    write_u32(excessive_replicas, first_replica_count_offset(value),
              kMaxCatalogReplicasPerChunk + 1U);
    rewrite_crc(excessive_replicas);
    EXPECT_FALSE(decode_replica_catalog(excessive_replicas));
}

TEST(ReplicaCatalogSerialization, RejectsInvalidIdentityStateAndEvidence) {
    auto value = snapshot();
    value.records[0].identity.object_id = "../object";
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    value = snapshot();
    value.records[0].identity.generation_id = "bad/generation";
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    value = snapshot();
    value.records[0].chunk.index = 1;
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    value = snapshot();
    value.records[0].replicas[0].node_id = "bad/node";
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    value = snapshot();
    value.records[0].replicas[0].state =
        static_cast<CatalogReplicaState>(99);
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    value = snapshot();
    value.records[0].replicas[0].integrity_checksum++;
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    value = snapshot();
    value.records[0].replicas[0].evidence_version = 0;
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    const auto canonical = snapshot();
    const auto encoded = encode_replica_catalog(canonical);

    auto invalid_object = encoded;
    invalid_object[26] = std::byte{'/'};
    rewrite_crc(invalid_object);
    EXPECT_FALSE(decode_replica_catalog(invalid_object));

    auto invalid_generation = encoded;
    invalid_generation[first_generation_id_offset(canonical)] = std::byte{'/'};
    rewrite_crc(invalid_generation);
    EXPECT_FALSE(decode_replica_catalog(invalid_generation));

    auto invalid_node = encoded;
    const auto node_offset = first_replica_state_offset(canonical) -
                             canonical.records[0].replicas[0].node_id.size();
    invalid_node[node_offset] = std::byte{'/'};
    rewrite_crc(invalid_node);
    EXPECT_FALSE(decode_replica_catalog(invalid_node));

    auto zero_evidence = encoded;
    const auto evidence_offset = first_replica_state_offset(canonical) + 8U;
    std::fill_n(zero_evidence.begin() + evidence_offset, 8U, std::byte{0});
    rewrite_crc(zero_evidence);
    EXPECT_FALSE(decode_replica_catalog(zero_evidence));

    auto future_record = encoded;
    const auto record_version_offset = first_replica_count_offset(canonical) - 8U;
    std::fill_n(future_record.begin() + record_version_offset, 8U,
                std::byte{0xff});
    rewrite_crc(future_record);
    EXPECT_FALSE(decode_replica_catalog(future_record));
}

TEST(ReplicaCatalogSerialization, RejectsNonCanonicalReplicaAndRecordOrdering) {
    auto value = snapshot();
    std::swap(value.records[0].replicas[0], value.records[0].replicas[1]);
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    value = snapshot();
    value.records[0].replicas.push_back(value.records[0].replicas.back());
    EXPECT_THROW(static_cast<void>(encode_replica_catalog(value)), std::invalid_argument);

    auto first = snapshot().records.front();
    first.identity.object_id = "object-b";
    first.catalog_version = 1;
    auto second = snapshot().records.front();
    second.identity.object_id = "object-a";
    second.catalog_version = 2;
    EXPECT_THROW(static_cast<void>(encode_replica_catalog({2, {first, second}})),
                 std::invalid_argument);

    std::swap(first, second);
    second.catalog_version = first.catalog_version;
    EXPECT_THROW(static_cast<void>(encode_replica_catalog({2, {first, second}})),
                 std::invalid_argument);
}

TEST(DurableReplicaCatalog, VersionsChangesAndLeavesNoOpsAndConflictsStable) {
    TempRoot root;
    DurableReplicaCatalog catalog(root.path());
    const auto key = identity();
    const auto metadata = chunk();
    const std::vector<CatalogReplica> three{
        replica("node-a"), replica("node-b"), replica("node-c")};
    const std::vector<CatalogReplica> four{
        replica("node-a"), replica("node-b"), replica("node-c"),
        replica("node-d")};

    ASSERT_EQ(catalog.upsert(key, metadata, three), ReplicaCatalogError::none);
    EXPECT_EQ(catalog.version(), 1U);
    ASSERT_TRUE(catalog.get(key));
    EXPECT_EQ(catalog.get(key)->catalog_version, 1U);

    EXPECT_EQ(catalog.upsert(key, metadata, three), ReplicaCatalogError::none);
    EXPECT_EQ(catalog.version(), 1U);

    auto conflicting = metadata;
    conflicting.length++;
    EXPECT_EQ(catalog.upsert(key, conflicting, three),
              ReplicaCatalogError::conflict);
    EXPECT_EQ(catalog.version(), 1U);

    ASSERT_EQ(catalog.upsert(key, metadata, four), ReplicaCatalogError::none);
    EXPECT_EQ(catalog.version(), 2U);
    ASSERT_TRUE(catalog.get(key));
    EXPECT_EQ(catalog.get(key)->catalog_version, 2U);
    EXPECT_EQ(catalog.get(key)->replicas, four);
}

TEST(DurableReplicaCatalog, EraseRecreateNeverReusesVersionsAndSurvivesRestart) {
    TempRoot root;
    const auto key = identity();
    {
        DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(catalog.upsert(key, chunk(), {replica("node-a")}),
                  ReplicaCatalogError::none);
        ASSERT_EQ(catalog.erase(key), ReplicaCatalogError::none);
        EXPECT_EQ(catalog.version(), 2U);
        EXPECT_FALSE(catalog.get(key));
        EXPECT_EQ(catalog.erase(key), ReplicaCatalogError::not_found);
        EXPECT_EQ(catalog.version(), 2U);
        ASSERT_EQ(catalog.upsert(key, chunk(), {replica("node-b")}),
                  ReplicaCatalogError::none);
        EXPECT_EQ(catalog.version(), 3U);
        ASSERT_TRUE(catalog.get(key));
        EXPECT_EQ(catalog.get(key)->catalog_version, 3U);
    }

    DurableReplicaCatalog restarted(root.path());
    EXPECT_EQ(restarted.version(), 3U);
    ASSERT_TRUE(restarted.get(key));
    EXPECT_EQ(restarted.get(key)->catalog_version, 3U);
    EXPECT_EQ(restarted.get(key)->replicas,
              (std::vector<CatalogReplica>{replica("node-b")}));
}

TEST(DurableReplicaCatalog, ListsCanonicalIdentitiesAndPreservesLastChangedVersion) {
    TempRoot root;
    DurableReplicaCatalog catalog(root.path());
    const auto key_b = identity("object-b", "generation-2", 1);
    const auto key_a1 = identity("object-a", "generation-2", 1);
    const auto key_a0 = identity("object-a", "generation-1", 0);

    ASSERT_EQ(catalog.upsert(key_b, chunk(1), {replica("node-b")}),
              ReplicaCatalogError::none);
    ASSERT_EQ(catalog.upsert(key_a1, chunk(1), {replica("node-a")}),
              ReplicaCatalogError::none);
    ASSERT_EQ(catalog.upsert(key_a0, chunk(), {replica("node-c")}),
              ReplicaCatalogError::none);

    const auto records = catalog.list();
    ASSERT_EQ(records.size(), 3U);
    EXPECT_EQ(records[0].identity, key_a0);
    EXPECT_EQ(records[1].identity, key_a1);
    EXPECT_EQ(records[2].identity, key_b);
    EXPECT_EQ(records[0].catalog_version, 3U);
    EXPECT_EQ(records[1].catalog_version, 2U);
    EXPECT_EQ(records[2].catalog_version, 1U);

    ASSERT_EQ(catalog.upsert(key_a0, chunk(),
                             {replica("node-c"), replica("node-d")}),
              ReplicaCatalogError::none);
    EXPECT_EQ(catalog.version(), 4U);
    EXPECT_EQ(catalog.get(key_a0)->catalog_version, 4U);
    EXPECT_EQ(catalog.get(key_a1)->catalog_version, 2U);
    EXPECT_EQ(catalog.get(key_b)->catalog_version, 1U);
}

TEST(DurableReplicaCatalog, RejectsInvalidAndNonCanonicalMutations) {
    TempRoot root;
    DurableReplicaCatalog catalog(root.path());

    EXPECT_EQ(catalog.upsert(identity("bad/object"), chunk(),
                             {replica("node-a")}),
              ReplicaCatalogError::invalid_record);
    EXPECT_EQ(catalog.upsert(identity(), chunk(1), {replica("node-a")}),
              ReplicaCatalogError::invalid_record);
    EXPECT_EQ(catalog.upsert(identity(), chunk(),
                             {replica("node-b"), replica("node-a")}),
              ReplicaCatalogError::invalid_record);
    EXPECT_EQ(catalog.upsert(identity(), chunk(),
                             {replica("node-a"), replica("node-a")}),
              ReplicaCatalogError::invalid_record);
    EXPECT_EQ(catalog.erase(identity("bad/object")),
              ReplicaCatalogError::invalid_record);
    EXPECT_EQ(catalog.version(), 0U);
    EXPECT_TRUE(catalog.list().empty());
}

TEST(DurableReplicaCatalog, PreRenameFailuresKeepPublishedSnapshotAtomic) {
    for (const auto point : {ReplicaCatalogFaultPoint::record_creation,
                             ReplicaCatalogFaultPoint::record_write,
                             ReplicaCatalogFaultPoint::record_sync,
                             ReplicaCatalogFaultPoint::record_rename}) {
        TempRoot root;
        const auto key = identity();
        std::filesystem::path record_path;
        std::vector<std::byte> before;
        {
            DurableReplicaCatalog baseline(root.path());
            ASSERT_EQ(baseline.upsert(key, chunk(), {replica("node-a")}),
                      ReplicaCatalogError::none);
            record_path = baseline.record_path();
            before = read_bytes(record_path);
        }

        DurableReplicaCatalog failing(
            root.path(),
            [point](ReplicaCatalogFaultPoint current) { return current == point; });
        EXPECT_EQ(failing.upsert(
                      key, chunk(), {replica("node-a"), replica("node-b")}),
                  ReplicaCatalogError::persistence_error);
        EXPECT_EQ(failing.version(), 1U);
        ASSERT_TRUE(failing.get(key));
        EXPECT_EQ(failing.get(key)->replicas,
                  (std::vector<CatalogReplica>{replica("node-a")}));
        EXPECT_EQ(read_bytes(record_path), before);

        DurableReplicaCatalog restarted(root.path());
        EXPECT_EQ(restarted.version(), 1U);
        ASSERT_TRUE(restarted.get(key));
        EXPECT_EQ(restarted.get(key)->replicas,
                  (std::vector<CatalogReplica>{replica("node-a")}));
    }
}

TEST(DurableReplicaCatalog, DirectorySyncFailurePoisonsWriterWithoutVersionReuse) {
    TempRoot root;
    const auto key = identity();
    DurableReplicaCatalog uncertain(
        root.path(), [](ReplicaCatalogFaultPoint point) {
            return point == ReplicaCatalogFaultPoint::directory_sync;
        });

    EXPECT_EQ(uncertain.upsert(key, chunk(), {replica("node-a")}),
              ReplicaCatalogError::durability_unknown);
    EXPECT_TRUE(uncertain.durability_unknown());
    EXPECT_EQ(uncertain.version(), 1U);
    ASSERT_TRUE(uncertain.get(key));
    EXPECT_EQ(uncertain.get(key)->catalog_version, 1U);

    EXPECT_EQ(uncertain.upsert(
                  key, chunk(), {replica("node-a"), replica("node-b")}),
              ReplicaCatalogError::durability_unknown);
    EXPECT_EQ(uncertain.erase(key), ReplicaCatalogError::durability_unknown);
    EXPECT_EQ(uncertain.version(), 1U);

    DurableReplicaCatalog restarted(root.path());
    EXPECT_FALSE(restarted.durability_unknown());
    EXPECT_EQ(restarted.version(), 1U);
    ASSERT_EQ(restarted.upsert(
                  key, chunk(), {replica("node-a"), replica("node-b")}),
              ReplicaCatalogError::none);
    EXPECT_EQ(restarted.version(), 2U);
    EXPECT_EQ(restarted.get(key)->catalog_version, 2U);
}

TEST(DurableReplicaCatalog, ExhaustedVersionCannotBeReused) {
    TempRoot root;
    const auto directory = root.path() / "coordinator" / "replicas";
    const auto path = directory / "catalog";
    std::filesystem::create_directories(directory);
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    write_bytes(path, encode_replica_catalog(
                          {maximum,
                           {{identity(), chunk(), maximum,
                             {replica("node-a")}}}}));

    DurableReplicaCatalog catalog(root.path());
    EXPECT_EQ(catalog.version(), maximum);
    EXPECT_EQ(catalog.upsert(identity(), chunk(), {replica("node-a")}),
              ReplicaCatalogError::none);
    EXPECT_EQ(catalog.upsert(
                  identity(), chunk(),
                  {replica("node-a"), replica("node-b")}),
              ReplicaCatalogError::version_exhausted);
    EXPECT_EQ(catalog.compare_exchange(
                  {identity(), maximum, {replica("node-a")},
                   {replica("node-a"), replica("node-b")}}).status,
              ReplicaCatalogCasStatus::version_exhausted);
    EXPECT_EQ(catalog.erase(identity()), ReplicaCatalogError::version_exhausted);
    EXPECT_EQ(catalog.version(), maximum);
}

TEST(ReplicaCatalogCas, CommitsAtomicallyAndPreservesUnchangedRecordVersion) {
    TempRoot root;
    const auto first_key = identity("object-a");
    const auto second_key = identity("object-b");
    const std::vector<CatalogReplica> expected{
        replica("node-a"), replica("node-b"), replica("node-c")};
    const std::vector<CatalogReplica> desired{
        replica("node-a"), replica("node-b"), replica("node-c"),
        replica("node-d", CatalogReplicaState::present, 8)};
    {
        DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(catalog.upsert(first_key, chunk(), expected),
                  ReplicaCatalogError::none);
        ASSERT_EQ(catalog.upsert(second_key, chunk(), {replica("node-z")}),
                  ReplicaCatalogError::none);
        const auto stale_global = catalog.compare_exchange(
            {first_key, 1, expected, desired});
        EXPECT_EQ(stale_global.status, ReplicaCatalogCasStatus::conflict);
        EXPECT_EQ(catalog.get(first_key)->catalog_version, 1U);
        const auto result = catalog.compare_exchange(
            {first_key, 2, expected, desired});
        EXPECT_EQ(result.status, ReplicaCatalogCasStatus::committed);
        EXPECT_EQ(result.catalog_version, 3U);
        ASSERT_TRUE(result.current_record);
        EXPECT_EQ(result.current_record->catalog_version, 3U);
        EXPECT_EQ(result.current_record->replicas, desired);
        EXPECT_EQ(catalog.get(second_key)->catalog_version, 2U);
    }

    DurableReplicaCatalog restarted(root.path());
    EXPECT_EQ(restarted.version(), 3U);
    EXPECT_EQ(restarted.get(first_key)->replicas, desired);
    EXPECT_EQ(restarted.get(first_key)->catalog_version, 3U);
    EXPECT_EQ(restarted.get(second_key)->catalog_version, 2U);
}

TEST(ReplicaCatalogCas, RejectsStaleVersionAndExpectedStateWithoutMutation) {
    TempRoot root;
    DurableReplicaCatalog catalog(root.path());
    const auto key = identity();
    const std::vector<CatalogReplica> original{
        replica("node-a"), replica("node-b"), replica("node-c")};
    const std::vector<CatalogReplica> desired{
        replica("node-a"), replica("node-b"), replica("node-c"),
        replica("node-d", CatalogReplicaState::present, 8)};
    ASSERT_EQ(catalog.upsert(key, chunk(), original), ReplicaCatalogError::none);
    ASSERT_EQ(catalog.compare_exchange({key, 1, original, desired}).status,
              ReplicaCatalogCasStatus::committed);

    const auto retried = catalog.compare_exchange({key, 1, original, desired});
    EXPECT_EQ(retried.status, ReplicaCatalogCasStatus::conflict);
    ASSERT_TRUE(retried.current_record);
    EXPECT_EQ(retried.current_record->catalog_version, 2U);
    EXPECT_EQ(retried.current_record->replicas, desired);

    auto wrong_expected = desired;
    wrong_expected.back().node_id = "node-e";
    const auto mismatched = catalog.compare_exchange(
        {key, 2, wrong_expected, original});
    EXPECT_EQ(mismatched.status, ReplicaCatalogCasStatus::conflict);
    EXPECT_EQ(catalog.version(), 2U);
    EXPECT_EQ(catalog.get(key)->replicas, desired);

    const auto unchanged = catalog.compare_exchange({key, 2, desired, desired});
    EXPECT_EQ(unchanged.status, ReplicaCatalogCasStatus::no_change);
    EXPECT_EQ(unchanged.catalog_version, 2U);
}

TEST(ReplicaCatalogCas, SerializesConcurrentLogicalWriters) {
    TempRoot root;
    DurableReplicaCatalog catalog(root.path());
    const auto key = identity();
    const std::vector<CatalogReplica> original{
        replica("node-a"), replica("node-b"), replica("node-c")};
    const std::vector<CatalogReplica> with_d{
        replica("node-a"), replica("node-b"), replica("node-c"),
        replica("node-d", CatalogReplicaState::present, 8)};
    const std::vector<CatalogReplica> with_e{
        replica("node-a"), replica("node-b"), replica("node-c"),
        replica("node-e", CatalogReplicaState::present, 8)};
    ASSERT_EQ(catalog.upsert(key, chunk(), original), ReplicaCatalogError::none);

    std::atomic_uint ready{0};
    std::atomic_bool start{false};
    ReplicaCatalogCasResult first;
    ReplicaCatalogCasResult second;
    const auto run = [&](const auto& desired, ReplicaCatalogCasResult& result) {
        ready.fetch_add(1);
        while (!start.load()) std::this_thread::yield();
        result = catalog.compare_exchange({key, 1, original, desired});
    };
    std::thread one(run, std::cref(with_d), std::ref(first));
    std::thread two(run, std::cref(with_e), std::ref(second));
    while (ready.load() != 2U) std::this_thread::yield();
    start.store(true);
    one.join();
    two.join();

    const auto committed = static_cast<unsigned>(
        first.status == ReplicaCatalogCasStatus::committed) +
        static_cast<unsigned>(second.status == ReplicaCatalogCasStatus::committed);
    const auto conflicts = static_cast<unsigned>(
        first.status == ReplicaCatalogCasStatus::conflict) +
        static_cast<unsigned>(second.status == ReplicaCatalogCasStatus::conflict);
    EXPECT_EQ(committed, 1U);
    EXPECT_EQ(conflicts, 1U);
    EXPECT_EQ(catalog.version(), 2U);
    const auto current = catalog.get(key); ASSERT_TRUE(current);
    EXPECT_TRUE(current->replicas == with_d || current->replicas == with_e);
}

TEST(ReplicaCatalogCas, RejectsInvalidRequestsAndDuplicateReplicas) {
    TempRoot root;
    DurableReplicaCatalog catalog(root.path());
    const auto key = identity();
    const std::vector<CatalogReplica> original{replica("node-a")};
    ASSERT_EQ(catalog.upsert(key, chunk(), original), ReplicaCatalogError::none);

    EXPECT_EQ(catalog.compare_exchange({key, 0, original, original}).status,
              ReplicaCatalogCasStatus::invalid_request);
    EXPECT_EQ(catalog.compare_exchange(
                  {identity("missing"), 1, original, original}).status,
              ReplicaCatalogCasStatus::not_found);
    EXPECT_EQ(catalog.compare_exchange(
                  {key, 1, original, {replica("node-a"), replica("node-a")}}).status,
              ReplicaCatalogCasStatus::invalid_request);
    EXPECT_EQ(catalog.version(), 1U);
    EXPECT_EQ(catalog.get(key)->replicas, original);
}

TEST(ReplicaCatalogCas, PreRenameFailuresRetainOldDurableState) {
    for (const auto point : {ReplicaCatalogFaultPoint::record_creation,
                             ReplicaCatalogFaultPoint::record_write,
                             ReplicaCatalogFaultPoint::record_sync,
                             ReplicaCatalogFaultPoint::record_rename}) {
        TempRoot root;
        const auto key = identity();
        const std::vector<CatalogReplica> original{replica("node-a")};
        const std::vector<CatalogReplica> desired{
            replica("node-a"),
            replica("node-b", CatalogReplicaState::present, 8)};
        {
            DurableReplicaCatalog baseline(root.path());
            ASSERT_EQ(baseline.upsert(key, chunk(), original),
                      ReplicaCatalogError::none);
        }
        DurableReplicaCatalog failing(root.path(), [point](auto current) {
            return current == point;
        });
        const auto result = failing.compare_exchange({key, 1, original, desired});
        EXPECT_EQ(result.status, ReplicaCatalogCasStatus::persistence_error);
        EXPECT_EQ(result.catalog_version, 1U);
        EXPECT_EQ(failing.get(key)->replicas, original);
        DurableReplicaCatalog restarted(root.path());
        EXPECT_EQ(restarted.version(), 1U);
        EXPECT_EQ(restarted.get(key)->replicas, original);
    }
}

TEST(ReplicaCatalogCas, DirectorySyncFailureReportsUnknownAndPoisonsWriter) {
    TempRoot root;
    bool fail = false;
    DurableReplicaCatalog catalog(root.path(), [&](auto point) {
        return fail && point == ReplicaCatalogFaultPoint::directory_sync;
    });
    const auto key = identity();
    const std::vector<CatalogReplica> original{replica("node-a")};
    const std::vector<CatalogReplica> desired{
        replica("node-a"), replica("node-b", CatalogReplicaState::present, 8)};
    ASSERT_EQ(catalog.upsert(key, chunk(), original), ReplicaCatalogError::none);
    fail = true;

    const auto result = catalog.compare_exchange({key, 1, original, desired});
    EXPECT_EQ(result.status, ReplicaCatalogCasStatus::durability_unknown);
    EXPECT_EQ(result.catalog_version, 2U);
    ASSERT_TRUE(result.current_record);
    EXPECT_EQ(result.current_record->replicas, desired);
    EXPECT_TRUE(catalog.durability_unknown());
    EXPECT_EQ(catalog.compare_exchange({key, 2, desired, original}).status,
              ReplicaCatalogCasStatus::durability_unknown);

    DurableReplicaCatalog restarted(root.path());
    EXPECT_FALSE(restarted.durability_unknown());
    EXPECT_EQ(restarted.version(), 2U);
    EXPECT_EQ(restarted.get(key)->replicas, desired);
}

TEST(DurableReplicaCatalog, CorruptPublishedCatalogStopsStartupWithoutDeletion) {
    TempRoot root;
    std::filesystem::path path;
    {
        DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(catalog.upsert(identity(), chunk(), {replica("node-a")}),
                  ReplicaCatalogError::none);
        path = catalog.record_path();
    }
    auto corrupt = read_bytes(path);
    corrupt[26] = std::byte{'/'};
    rewrite_crc(corrupt);
    write_bytes(path, corrupt);

    EXPECT_THROW(static_cast<void>(DurableReplicaCatalog(root.path())),
                 std::runtime_error);
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(read_bytes(path), corrupt);
}

TEST(DurableReplicaCatalog, StartupIgnoresStaleTemporaryCatalog) {
    TempRoot root;
    std::filesystem::path path;
    {
        DurableReplicaCatalog catalog(root.path());
        ASSERT_EQ(catalog.upsert(identity(), chunk(), {replica("node-a")}),
                  ReplicaCatalogError::none);
        path = catalog.record_path();
    }

    const ReplicaCatalogSnapshot uncommitted{
        2, {{identity(), chunk(), 2, {replica("node-b")}}}};
    const auto temporary = path.parent_path() / "catalog.tmp";
    write_bytes(temporary, encode_replica_catalog(uncommitted));

    DurableReplicaCatalog restarted(root.path());
    EXPECT_EQ(restarted.version(), 1U);
    ASSERT_TRUE(restarted.get(identity()));
    EXPECT_EQ(restarted.get(identity())->replicas,
              (std::vector<CatalogReplica>{replica("node-a")}));
    EXPECT_TRUE(std::filesystem::exists(temporary));
}

TEST(DurableReplicaCatalog, RejectsOversizedPublishedCatalogBeforeAllocation) {
    TempRoot root;
    const auto directory = root.path() / "coordinator" / "replicas";
    const auto path = directory / "catalog";
    std::filesystem::create_directories(directory);
    write_bytes(path, std::vector<std::byte>{std::byte{'x'}});
    std::filesystem::resize_file(path, kMaxReplicaCatalogSize + 1U);

    EXPECT_THROW(static_cast<void>(DurableReplicaCatalog(root.path())),
                 std::runtime_error);
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(std::filesystem::file_size(path),
              static_cast<std::uintmax_t>(kMaxReplicaCatalogSize + 1U));
}

TEST(DurableReplicaCatalog, NeverRewritesHistoricalCoordinatorOperationBytes) {
    TempRoot root;
    const auto operations = root.path() / "coordinator" / "operations";
    const auto historical = operations / "historical-op";
    std::filesystem::create_directories(operations);
    const std::vector<std::byte> sentinel{
        std::byte{'A'}, std::byte{'C'}, std::byte{'O'}, std::byte{'P'},
        std::byte{0}, std::byte{1}, std::byte{0x5a}, std::byte{0xa5}};
    write_bytes(historical, sentinel);

    {
        DurableReplicaCatalog catalog(root.path());
        const std::vector<CatalogReplica> original{replica("node-a")};
        const std::vector<CatalogReplica> desired{
            replica("node-a"),
            replica("node-b", CatalogReplicaState::present, 8)};
        ASSERT_EQ(catalog.upsert(identity(), chunk(), original),
                  ReplicaCatalogError::none);
        ASSERT_EQ(catalog.compare_exchange(
                      {identity(), 1, original, desired}).status,
                  ReplicaCatalogCasStatus::committed);
        ASSERT_EQ(catalog.erase(identity()), ReplicaCatalogError::none);
    }
    DurableReplicaCatalog restarted(root.path());

    EXPECT_TRUE(restarted.list().empty());
    EXPECT_EQ(restarted.version(), 3U);
    EXPECT_TRUE(std::filesystem::exists(historical));
    EXPECT_EQ(read_bytes(historical), sentinel);
}

}  // namespace
}  // namespace atlasfs::metadata
