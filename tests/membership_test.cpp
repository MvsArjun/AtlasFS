#include "atlasfs/checksum.hpp"
#include "atlasfs/membership.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <fstream>
#include <iterator>
#include <limits>
#include <unistd.h>

namespace atlasfs::metadata {
namespace {
class TempRoot {
public:
    TempRoot() {
        static std::atomic_uint64_t sequence{0};
        path_ = std::filesystem::temp_directory_path() / ("atlasfs-membership-test-" +
            std::to_string(static_cast<unsigned long long>(::getpid())) + "-" +
            std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }
    ~TempRoot() { std::error_code error; std::filesystem::remove_all(path_, error); }
    const auto& path() const { return path_; }
private: std::filesystem::path path_;
};
StorageNode node(std::string id, NodeState state = NodeState::active) {
    return {std::move(id), {"127.0.0.1", 7000}, state};
}
void rewrite_crc(std::vector<std::byte>& bytes) {
    const auto crc = integrity::crc32c(std::span(bytes).first(bytes.size() - 4));
    for (unsigned i = 0; i < 4; ++i)
        bytes[bytes.size() - 4 + i] = static_cast<std::byte>((crc >> ((3U - i) * 8U)) & 0xffU);
}
class FakeHeartbeat final : public HeartbeatClient {
public:
    explicit FakeHeartbeat(HeartbeatResult initial) : result(std::move(initial)) {}
    HeartbeatResult result;
    HeartbeatResult check(const NodeEndpoint&) override { ++calls; return result; }
    unsigned calls{};
};
void write_snapshot(const std::filesystem::path& path, const MembershipSnapshot& snapshot) {
    const auto bytes = encode_membership(snapshot);
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();
    ASSERT_TRUE(output);
}
std::vector<char> read_bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

TEST(MembershipSerialization, EmptyAndMultipleNodesRoundTrip) {
    EXPECT_EQ(decode_membership(encode_membership({7, {}})), (MembershipSnapshot{7, {}}));
    MembershipSnapshot value{9, {node("a"), node("b", NodeState::draining)}};
    value.nodes[1].health = NodeHealthState::suspect; value.nodes[1].consecutive_failures = 1;
    auto decoded = decode_membership(encode_membership(value)); ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->version, 9U); EXPECT_EQ(decoded->nodes.size(), 2U);
    EXPECT_FALSE(decoded->nodes[0].health_current);
}
TEST(MembershipSerialization, RejectsCorruptionAndInvalidFields) {
    auto bytes = encode_membership({1, {node("a")}});
    auto truncated = bytes; truncated.pop_back(); EXPECT_FALSE(decode_membership(truncated));
    auto trailing = bytes; trailing.push_back(std::byte{0}); EXPECT_FALSE(decode_membership(trailing));
    for (const auto mutation : {std::pair{0U, std::byte{0}}, std::pair{5U, std::byte{2}},
                                std::pair{6U, std::byte{1}}}) {
        auto changed = bytes; changed[mutation.first] = mutation.second; rewrite_crc(changed);
        EXPECT_FALSE(decode_membership(changed));
    }
    auto invalid = node("a"); invalid.endpoint.port = 0;
    EXPECT_THROW(static_cast<void>(encode_membership({1, {invalid}})),
                 std::invalid_argument);
}
TEST(DurableMembership, MutationsAreVersionedPersistedAndRestartSafe) {
    TempRoot root;
    { DurableMembership membership(root.path());
      ASSERT_EQ(membership.add(node("b")), MembershipError::none);
      ASSERT_EQ(membership.add(node("a")), MembershipError::none);
      EXPECT_EQ(membership.version(), 2U);
      ASSERT_EQ(membership.set_state("b", NodeState::draining), MembershipError::none);
      const auto version = membership.version();
      EXPECT_EQ(membership.set_state("b", NodeState::draining), MembershipError::none);
      EXPECT_EQ(membership.version(), version); }
    DurableMembership restarted(root.path());
    EXPECT_EQ(restarted.version(), 3U); ASSERT_EQ(restarted.list().size(), 2U);
    EXPECT_EQ(restarted.list()[0].node_id, "a"); EXPECT_FALSE(restarted.list()[0].health_current);
    EXPECT_EQ(restarted.list()[1].state, NodeState::draining);
    ASSERT_EQ(restarted.remove("a"), MembershipError::none); EXPECT_EQ(restarted.version(), 4U);
}
TEST(DurableMembership, RejectedMutationDoesNotAdvanceVersion) {
    TempRoot root; DurableMembership membership(root.path());
    EXPECT_EQ(membership.add(node("bad/id")), MembershipError::invalid_node);
    EXPECT_EQ(membership.remove("missing"), MembershipError::unknown_node);
    EXPECT_EQ(membership.version(), 0U);
}
TEST(DurableMembership, ExhaustedVersionRejectsMutationAndPreservesDurableStateAcrossRestart) {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    TempRoot root;
    std::filesystem::path path;
    { DurableMembership initial(root.path());
      ASSERT_EQ(initial.add(node("a")), MembershipError::none); path = initial.record_path(); }
    write_snapshot(path, {maximum, {node("a")}});
    const auto durable_before = read_bytes(path);
    {
        DurableMembership membership(root.path());
        const auto before = membership.list();
        EXPECT_EQ(membership.add(node("b")), MembershipError::version_exhausted);
        EXPECT_EQ(membership.remove("a"), MembershipError::version_exhausted);
        EXPECT_EQ(membership.set_state("a", NodeState::draining), MembershipError::version_exhausted);
        EXPECT_EQ(membership.set_state("a", NodeState::active), MembershipError::none);
        EXPECT_EQ(membership.version(), maximum);
        EXPECT_EQ(membership.list(), before);
        EXPECT_FALSE(membership.durability_unknown());
        EXPECT_EQ(read_bytes(path), durable_before);
    }
    DurableMembership restarted(root.path());
    EXPECT_EQ(restarted.version(), maximum);
    ASSERT_EQ(restarted.list().size(), 1U);
    EXPECT_EQ(restarted.list()[0].node_id, "a");
    EXPECT_EQ(restarted.list()[0].state, NodeState::active);
    EXPECT_EQ(restarted.add(node("b")), MembershipError::version_exhausted);
    EXPECT_EQ(read_bytes(path), durable_before);
}
TEST(FailureDetector, ExhaustedVersionCannotMakeRestartHealthFreshOrPersistWrappedEvidence) {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    TempRoot root;
    std::filesystem::path path;
    { DurableMembership initial(root.path());
      ASSERT_EQ(initial.add(node("a")), MembershipError::none); path = initial.record_path(); }
    write_snapshot(path, {maximum, {node("a")}});
    const auto durable_before = read_bytes(path);
    for (const auto status : {HeartbeatStatus::success, HeartbeatStatus::timeout}) {
        DurableMembership membership(root.path());
        const auto before = membership.list();
        FakeHeartbeat heartbeat({status, "a"});
        EXPECT_EQ(membership.check_node("a", heartbeat), HeartbeatStatus::persistence_error);
        EXPECT_EQ(heartbeat.calls, 1U);
        EXPECT_EQ(membership.version(), maximum);
        EXPECT_EQ(membership.list(), before);
        EXPECT_TRUE(membership.eligible_nodes().empty());
        EXPECT_EQ(read_bytes(path), durable_before);
    }
}
TEST(DurableMembership, PublicationFailuresDoNotExposeCandidate) {
    for (const auto point : {MembershipFaultPoint::record_creation,
         MembershipFaultPoint::record_write, MembershipFaultPoint::record_sync,
         MembershipFaultPoint::record_rename, MembershipFaultPoint::directory_sync}) {
        TempRoot root; DurableMembership membership(root.path(), {},
            [point](auto current) { return point == current; });
        EXPECT_EQ(membership.add(node("a")), MembershipError::persistence_error);
        EXPECT_FALSE(membership.get("a")); EXPECT_EQ(membership.version(), 0U);
        EXPECT_EQ(membership.durability_unknown(), point == MembershipFaultPoint::directory_sync);
    }
}
TEST(DurableMembership, AmbiguousPublicationCannotReuseVersionOrRunHeartbeat) {
    TempRoot root; bool fail_sync = false;
    DurableMembership membership(root.path(), {}, [&](auto point) {
        return fail_sync && point == MembershipFaultPoint::directory_sync;
    });
    ASSERT_EQ(membership.add(node("a")), MembershipError::none);
    fail_sync = true;
    ASSERT_EQ(membership.add(node("b")), MembershipError::persistence_error);
    EXPECT_TRUE(membership.durability_unknown());
    EXPECT_TRUE(membership.eligible_nodes().empty());
    EXPECT_EQ(membership.version(), 1U);
    fail_sync = false;
    EXPECT_EQ(membership.add(node("c")), MembershipError::persistence_error);
    EXPECT_EQ(membership.set_state("a", NodeState::active), MembershipError::persistence_error);
    EXPECT_EQ(membership.remove("a"), MembershipError::persistence_error);
    FakeHeartbeat heartbeat({HeartbeatStatus::success, "a"});
    EXPECT_EQ(membership.check_node("a", heartbeat), HeartbeatStatus::persistence_error);
    EXPECT_EQ(heartbeat.calls, 0U);
    DurableMembership restarted(root.path());
    EXPECT_FALSE(restarted.durability_unknown());
    EXPECT_EQ(restarted.version(), 2U);
    EXPECT_TRUE(restarted.get("a")); EXPECT_TRUE(restarted.get("b"));
    EXPECT_FALSE(restarted.get("c"));
    EXPECT_EQ(restarted.add(node("c")), MembershipError::none);
    EXPECT_EQ(restarted.version(), 3U);
}
TEST(DurableMembership, RejectsOversizedTruncatedTrailingAndNonRegularStartupRecords) {
    for (unsigned corruption = 0; corruption < 4; ++corruption) {
        TempRoot root; std::filesystem::path path;
        { DurableMembership store(root.path());
          ASSERT_EQ(store.add(node("a")), MembershipError::none); path = store.record_path(); }
        if (corruption == 0) std::filesystem::resize_file(path, 2U * 1024U * 1024U + 1U);
        if (corruption == 1) std::filesystem::resize_file(path, std::filesystem::file_size(path) - 1U);
        if (corruption == 2) std::ofstream(path, std::ios::binary | std::ios::app).put('x');
        if (corruption == 3) { std::filesystem::remove(path); std::filesystem::create_directory(path); }
        EXPECT_THROW(DurableMembership(root.path()), std::runtime_error) << corruption;
        EXPECT_TRUE(std::filesystem::exists(path));
    }
}
TEST(DurableMembership, CorruptRecordStopsStartupWithoutDeletion) {
    TempRoot root; std::filesystem::path record;
    { DurableMembership membership(root.path()); ASSERT_EQ(membership.add(node("a")), MembershipError::none);
      record = membership.record_path(); }
    std::ofstream(record, std::ios::binary | std::ios::trunc).put('x');
    EXPECT_THROW(DurableMembership(root.path()), std::runtime_error);
    EXPECT_TRUE(std::filesystem::exists(record));
}
TEST(FailureDetector, RequiresThresholdAndPreservesDrainingIntent) {
    TempRoot root; DurableMembership membership(root.path(), {2});
    ASSERT_EQ(membership.add(node("a", NodeState::draining)), MembershipError::none);
    FakeHeartbeat heartbeat{{HeartbeatStatus::timeout, {}}};
    EXPECT_EQ(membership.check_node("a", heartbeat), HeartbeatStatus::timeout);
    EXPECT_EQ(membership.get("a")->health, NodeHealthState::suspect);
    EXPECT_EQ(membership.check_node("a", heartbeat), HeartbeatStatus::timeout);
    EXPECT_EQ(membership.get("a")->health, NodeHealthState::unavailable);
    EXPECT_EQ(membership.get("a")->state, NodeState::draining);
    EXPECT_TRUE(membership.eligible_nodes().empty());
}
TEST(FailureDetector, VerifiesIdentityAndRecoversOnlyHealth) {
    TempRoot root; DurableMembership membership(root.path(), {1});
    ASSERT_EQ(membership.add(node("a")), MembershipError::none);
    FakeHeartbeat wrong{{HeartbeatStatus::success, "b"}};
    EXPECT_EQ(membership.check_node("a", wrong), HeartbeatStatus::identity_mismatch);
    EXPECT_EQ(membership.get("a")->health, NodeHealthState::unavailable);
    FakeHeartbeat good{{HeartbeatStatus::success, "a"}};
    EXPECT_EQ(membership.check_node("a", good), HeartbeatStatus::success);
    EXPECT_EQ(membership.get("a")->health, NodeHealthState::healthy);
    EXPECT_EQ(membership.get("a")->state, NodeState::active);
    EXPECT_EQ(good.calls, 1U);
}
TEST(FailureDetector, FreshHeartbeatSupersedesStaleRestartObservation) {
    TempRoot root;
    { DurableMembership membership(root.path()); ASSERT_EQ(membership.add(node("a")), MembershipError::none); }
    DurableMembership restarted(root.path()); ASSERT_FALSE(restarted.get("a")->health_current);
    EXPECT_TRUE(restarted.eligible_nodes().empty());
    FakeHeartbeat heartbeat{{HeartbeatStatus::success, "a"}};
    EXPECT_EQ(restarted.check_node("a", heartbeat), HeartbeatStatus::success);
    EXPECT_TRUE(restarted.get("a")->health_current); EXPECT_EQ(restarted.eligible_nodes().size(), 1U);
}
TEST(FailureDetector, ConfigurationIsBounded) {
    TempRoot root;
    EXPECT_THROW(DurableMembership(root.path(), {0}), std::invalid_argument);
    EXPECT_THROW(DurableMembership(root.path(), {kMaxFailureThreshold + 1}), std::invalid_argument);
}
}  // namespace
}  // namespace atlasfs::metadata
