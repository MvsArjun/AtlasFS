#include "atlasfs/protocol.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <span>
#include <vector>

namespace atlasfs::protocol {
namespace {

std::vector<std::byte> bytes(std::initializer_list<unsigned char> values) {
    std::vector<std::byte> result;
    result.reserve(values.size());
    for (const auto value : values) {
        result.push_back(static_cast<std::byte>(value));
    }
    return result;
}

Message sample_message() {
    return {MessageType::put_request, 0x10203040U, 0x0102030405060708ULL,
            bytes({0x00, 0x41, 0xff, 0x7e})};
}

TEST(ProtocolEncoder, UsesStableBigEndianWireLayout) {
    const auto encoded = encode(sample_message());
    const auto expected = bytes({
        0x41, 0x54, 0x46, 0x53, 0x00, 0x01, 0x00, 0x01,
        0x10, 0x20, 0x30, 0x40, 0x01, 0x02, 0x03, 0x04,
        0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x41, 0xff, 0x7e,
    });
    EXPECT_EQ(encoded, expected);
}

TEST(StreamParser, ParsesCompleteMessage) {
    StreamParser parser;
    const auto message = sample_message();
    const auto result = parser.feed(encode(message));
    EXPECT_EQ(result.error, ParseError::none);
    ASSERT_EQ(result.messages.size(), 1U);
    EXPECT_EQ(result.messages.front(), message);
    EXPECT_FALSE(parser.has_partial_message());
}

TEST(StreamParser, AcceptsEveryPossibleTwoPartFragmentation) {
    const auto message = sample_message();
    const auto encoded = encode(message);
    for (std::size_t split = 0; split < encoded.size(); ++split) {
        StreamParser parser;
        const auto first = parser.feed(std::span(encoded).first(split));
        EXPECT_EQ(first.error, ParseError::none) << "split=" << split;
        EXPECT_TRUE(first.messages.empty()) << "split=" << split;

        const auto second = parser.feed(std::span(encoded).subspan(split));
        EXPECT_EQ(second.error, ParseError::none) << "split=" << split;
        ASSERT_EQ(second.messages.size(), 1U) << "split=" << split;
        EXPECT_EQ(second.messages.front(), message) << "split=" << split;
    }
}

TEST(StreamParser, AcceptsOneByteAtATime) {
    const auto message = sample_message();
    const auto encoded = encode(message);
    StreamParser parser;
    std::vector<Message> messages;
    for (const auto byte : encoded) {
        const std::array fragment{byte};
        auto result = parser.feed(fragment);
        ASSERT_EQ(result.error, ParseError::none);
        messages.insert(messages.end(), result.messages.begin(), result.messages.end());
    }
    ASSERT_EQ(messages.size(), 1U);
    EXPECT_EQ(messages.front(), message);
}

TEST(StreamParser, ParsesMultipleCoalescedMessages) {
    const Message first{MessageType::get_request, 0, 7, bytes({0x61})};
    const Message second{MessageType::delete_request, 1, 8, {}};
    auto wire = encode(first);
    const auto second_wire = encode(second);
    wire.insert(wire.end(), second_wire.begin(), second_wire.end());

    StreamParser parser;
    const auto result = parser.feed(wire);
    EXPECT_EQ(result.error, ParseError::none);
    EXPECT_EQ(result.messages, (std::vector<Message>{first, second}));
}

TEST(StreamParser, RetainsTrailingPartialMessage) {
    const Message first{MessageType::response, 0, 10, bytes({1, 2})};
    const Message second{MessageType::error_response, 0, 11, bytes({3, 4, 5})};
    auto wire = encode(first);
    const auto second_wire = encode(second);
    wire.insert(wire.end(), second_wire.begin(), second_wire.begin() + 10);

    StreamParser parser;
    const auto initial = parser.feed(wire);
    ASSERT_EQ(initial.messages.size(), 1U);
    EXPECT_EQ(initial.messages.front(), first);
    EXPECT_TRUE(parser.has_partial_message());

    const auto remainder = std::span(second_wire).subspan(10);
    const auto final = parser.feed(remainder);
    ASSERT_EQ(final.messages.size(), 1U);
    EXPECT_EQ(final.messages.front(), second);
    EXPECT_FALSE(parser.has_partial_message());
}

TEST(StreamParser, ReportsTruncatedFrameAtEndOfStream) {
    StreamParser parser;
    auto wire = encode(sample_message());
    wire.pop_back();
    const auto result = parser.feed(wire);
    EXPECT_EQ(result.error, ParseError::none);
    EXPECT_TRUE(result.messages.empty());
    EXPECT_TRUE(parser.has_partial_message());
}

TEST(StreamParser, RejectsInvalidMagicAndEntersFailedState) {
    auto wire = encode(sample_message());
    wire[0] = std::byte{0};
    StreamParser parser;
    EXPECT_EQ(parser.feed(wire).error, ParseError::invalid_magic);
    EXPECT_TRUE(parser.failed());
    EXPECT_EQ(parser.feed({}).error, ParseError::parser_failed);
}

TEST(StreamParser, RejectsUnsupportedVersion) {
    auto wire = encode(sample_message());
    wire[5] = std::byte{2};
    StreamParser parser;
    EXPECT_EQ(parser.feed(wire).error, ParseError::unsupported_version);
}

TEST(StreamParser, RejectsUnknownMessageType) {
    auto wire = encode(sample_message());
    wire[6] = std::byte{0x7f};
    wire[7] = std::byte{0xff};
    StreamParser parser;
    EXPECT_EQ(parser.feed(wire).error, ParseError::unknown_message_type);
}

TEST(StreamParser, RejectsOversizedPayloadBeforeBufferingIt) {
    auto wire = encode(sample_message());
    wire.resize(kHeaderSize);
    wire[20] = std::byte{0};
    wire[21] = std::byte{0};
    wire[22] = std::byte{0};
    wire[23] = std::byte{5};
    StreamParser parser(4);
    EXPECT_EQ(parser.feed(wire).error, ParseError::payload_too_large);
}

TEST(StreamParser, ResetRecoversFromProtocolError) {
    auto invalid = encode(sample_message());
    invalid[0] = std::byte{0};
    StreamParser parser;
    ASSERT_EQ(parser.feed(invalid).error, ParseError::invalid_magic);
    parser.reset();
    const auto result = parser.feed(encode(sample_message()));
    EXPECT_EQ(result.error, ParseError::none);
    EXPECT_EQ(result.messages.size(), 1U);
}

}  // namespace
}  // namespace atlasfs::protocol
