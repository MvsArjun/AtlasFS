#include "atlasfs/checksum.hpp"
#include "atlasfs/object_protocol.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <vector>

namespace atlasfs::object_protocol {
namespace {

std::vector<std::byte> data() {
    return {std::byte{0}, std::byte{1}, std::byte{0xff}, std::byte{0}};
}

TEST(ObjectProtocol, PutRequestRoundTripsBinaryData) {
    const auto input_data = data();
    const PutRequest input{"object-1", input_data, integrity::crc32c(input_data)};
    EXPECT_EQ(decode_put_request(encode_put_request(input)), input);
}

TEST(ObjectProtocol, PutRequestRoundTripsEmptyObject) {
    const PutRequest input{"empty", {}, integrity::crc32c({})};
    EXPECT_EQ(decode_put_request(encode_put_request(input)), input);
}

TEST(ObjectProtocol, ObjectIdRequestsRoundTrip) {
    const ObjectIdRequest input{"object.name"};
    EXPECT_EQ(decode_object_id_request(encode_object_id_request(input)), input);
}

TEST(ObjectProtocol, StatusResponsesRoundTrip) {
    for (std::uint16_t raw = 0;
         raw <= static_cast<std::uint16_t>(Status::malformed_request); ++raw) {
        const auto status = static_cast<Status>(raw);
        EXPECT_EQ(decode_status_response(encode_status_response(status)), status);
    }
}

TEST(ObjectProtocol, GetResponseRoundTripsBinaryDataAndChecksum) {
    const auto input_data = data();
    const GetResponse input{Status::success, input_data, integrity::crc32c(input_data)};
    EXPECT_EQ(decode_get_response(encode_get_response(input)), input);
}

TEST(ObjectProtocol, FailedGetResponseContainsOnlyStatus) {
    const GetResponse input{Status::not_found, {}, 0};
    EXPECT_EQ(decode_get_response(encode_get_response(input)), input);
}

TEST(ObjectProtocol, RejectsTruncatedPutPayloadAtEveryBoundary) {
    const auto input_data = data();
    const PutRequest input{"object", input_data, integrity::crc32c(input_data)};
    const auto encoded = encode_put_request(input);
    for (std::size_t size = 0; size < encoded.size(); ++size) {
        EXPECT_FALSE(decode_put_request(std::span(encoded).first(size))) << size;
    }
}

TEST(ObjectProtocol, RejectsMalformedAndOversizedLengths) {
    const auto input_data = data();
    auto encoded = encode_put_request({"x", input_data, integrity::crc32c(input_data)});
    encoded[0] = std::byte{1};
    encoded[1] = std::byte{0};
    EXPECT_FALSE(decode_put_request(encoded));

    encoded = encode_put_request({"x", input_data, integrity::crc32c(input_data)});
    encoded[2] = std::byte{0x7f};
    encoded[3] = std::byte{0xff};
    encoded[4] = std::byte{0xff};
    encoded[5] = std::byte{0xff};
    EXPECT_FALSE(decode_put_request(encoded));
}

TEST(ObjectProtocol, RejectsTrailingBytes) {
    auto encoded = encode_object_id_request({"x"});
    encoded.push_back(std::byte{0});
    EXPECT_FALSE(decode_object_id_request(encoded));
}

TEST(ObjectProtocol, PreservesSuppliedChecksumEvenWhenItIsWrong) {
    const PutRequest input{"object", data(), 123U};
    const auto decoded = decode_put_request(encode_put_request(input));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->checksum, 123U);
    EXPECT_NE(decoded->checksum, integrity::crc32c(decoded->data));
}

TEST(ObjectProtocol, RejectsGetResponseWithWrongChecksum) {
    const auto input_data = data();
    auto encoded = encode_get_response(
        {Status::success, input_data, integrity::crc32c(input_data)});
    encoded.back() ^= std::byte{1};
    EXPECT_FALSE(decode_get_response(encoded));
}

}  // namespace
}  // namespace atlasfs::object_protocol
