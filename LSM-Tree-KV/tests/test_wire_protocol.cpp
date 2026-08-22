#include "cluster/wire_protocol.h"
#include "tests.h"
#include "test_framework.h"
#include <string>

namespace {

// Every encode_* function returns a full frame: [4B len][1B type][payload].
// decode_payload() only takes the payload, so tests strip the 5-byte
// (length + type... wait, type is part of payload) header consistently:
// strip the 4-byte length prefix, then decode_payload gets [type][fields].
std::vector<uint8_t> strip_length_prefix(const std::vector<uint8_t>& frame) {
    return std::vector<uint8_t>(frame.begin() + 4, frame.end());
}

uint32_t read_length_prefix(const std::vector<uint8_t>& frame) {
    return static_cast<uint32_t>(frame[0]) | (static_cast<uint32_t>(frame[1]) << 8) |
           (static_cast<uint32_t>(frame[2]) << 16) | (static_cast<uint32_t>(frame[3]) << 24);
}

} // namespace

void kv_tests::run_test_wire_protocol() {
    std::cout << "--- [Test] Wire Protocol ---" << std::endl;

    // PUT_REQUEST round trip
    {
        auto frame = kv_cluster::encode_put_request("hello", "world");
        KV_CHECK_EQ(uint32_t(frame.size() - 4), read_length_prefix(frame),
                    "PUT_REQUEST frame's length prefix matches payload size (type + fields)");
        auto decoded = kv_cluster::decode_payload(strip_length_prefix(frame));
        KV_CHECK(decoded.has_value(), "PUT_REQUEST decodes successfully");
        if (decoded.has_value()) {
            auto* r = std::get_if<kv_cluster::PutRequest>(&*decoded);
            KV_CHECK(r != nullptr, "decoded PUT_REQUEST holds a PutRequest alternative");
            if (r) {
                KV_CHECK_EQ(std::string("hello"), r->key, "PUT_REQUEST key round-trips");
                KV_CHECK_EQ(std::string("world"), r->value, "PUT_REQUEST value round-trips");
            }
        }
    }

    // GET_REQUEST round trip
    {
        auto frame = kv_cluster::encode_get_request("some_key");
        auto decoded = kv_cluster::decode_payload(strip_length_prefix(frame));
        auto* r = decoded ? std::get_if<kv_cluster::GetRequest>(&*decoded) : nullptr;
        KV_CHECK(r != nullptr && r->key == "some_key", "GET_REQUEST round-trips its key");
    }

    // DELETE_REQUEST round trip
    {
        auto frame = kv_cluster::encode_delete_request("doomed_key");
        auto decoded = kv_cluster::decode_payload(strip_length_prefix(frame));
        auto* r = decoded ? std::get_if<kv_cluster::DeleteRequest>(&*decoded) : nullptr;
        KV_CHECK(r != nullptr && r->key == "doomed_key", "DELETE_REQUEST round-trips its key");
    }

    // PUT_RESPONSE round trip (both ok values)
    {
        auto frame_ok = kv_cluster::encode_put_response(true);
        auto decoded_ok = kv_cluster::decode_payload(strip_length_prefix(frame_ok));
        auto* r_ok = decoded_ok ? std::get_if<kv_cluster::PutResponse>(&*decoded_ok) : nullptr;
        KV_CHECK(r_ok != nullptr && r_ok->ok, "PUT_RESPONSE(true) round-trips as ok=true");

        auto frame_bad = kv_cluster::encode_put_response(false);
        auto decoded_bad = kv_cluster::decode_payload(strip_length_prefix(frame_bad));
        auto* r_bad = decoded_bad ? std::get_if<kv_cluster::PutResponse>(&*decoded_bad) : nullptr;
        KV_CHECK(r_bad != nullptr && !r_bad->ok, "PUT_RESPONSE(false) round-trips as ok=false");
    }

    // GET_RESPONSE round trip: found with a value, and not-found
    {
        auto frame_found = kv_cluster::encode_get_response(true, "the_value");
        auto decoded_found = kv_cluster::decode_payload(strip_length_prefix(frame_found));
        auto* r_found = decoded_found ? std::get_if<kv_cluster::GetResponse>(&*decoded_found) : nullptr;
        KV_CHECK(r_found != nullptr && r_found->found && r_found->value == "the_value",
                 "GET_RESPONSE(found=true) round-trips found flag and value");

        auto frame_missing = kv_cluster::encode_get_response(false, "");
        auto decoded_missing = kv_cluster::decode_payload(strip_length_prefix(frame_missing));
        auto* r_missing = decoded_missing ? std::get_if<kv_cluster::GetResponse>(&*decoded_missing) : nullptr;
        KV_CHECK(r_missing != nullptr && !r_missing->found,
                 "GET_RESPONSE(found=false) round-trips found=false");
    }

    // DELETE_RESPONSE round trip
    {
        auto frame = kv_cluster::encode_delete_response(true);
        auto decoded = kv_cluster::decode_payload(strip_length_prefix(frame));
        auto* r = decoded ? std::get_if<kv_cluster::DeleteResponse>(&*decoded) : nullptr;
        KV_CHECK(r != nullptr && r->ok, "DELETE_RESPONSE round-trips");
    }

    // ERROR_RESPONSE round trip
    {
        auto frame = kv_cluster::encode_error_response("compute node unreachable");
        auto decoded = kv_cluster::decode_payload(strip_length_prefix(frame));
        auto* r = decoded ? std::get_if<kv_cluster::ErrorResponse>(&*decoded) : nullptr;
        KV_CHECK(r != nullptr && r->message == "compute node unreachable",
                 "ERROR_RESPONSE round-trips its message");
    }

    // Malformed input handling: decode_payload must not throw or crash, just
    // report failure via nullopt.
    {
        KV_CHECK_FALSE(kv_cluster::decode_payload({}).has_value(),
                       "decode_payload on an empty buffer returns nullopt, not a crash");

        std::vector<uint8_t> truncated{static_cast<uint8_t>(kv_cluster::MsgType::PutRequest),
                                        0x05, 0x00, 0x00}; // claims a 5-byte key, provides 0
        KV_CHECK_FALSE(kv_cluster::decode_payload(truncated).has_value(),
                       "decode_payload on a truncated length-prefixed field returns nullopt");

        std::vector<uint8_t> unknown_type{0x77}; // not any defined MsgType
        KV_CHECK_FALSE(kv_cluster::decode_payload(unknown_type).has_value(),
                       "decode_payload on an unrecognized msg_type returns nullopt");
    }

    // Values/keys containing embedded content that could confuse a naive
    // parser (empty string, binary-looking bytes) still round-trip since
    // framing is length-prefixed, not delimiter-based.
    {
        auto frame = kv_cluster::encode_put_request("", std::string("a\0b", 3));
        auto decoded = kv_cluster::decode_payload(strip_length_prefix(frame));
        auto* r = decoded ? std::get_if<kv_cluster::PutRequest>(&*decoded) : nullptr;
        KV_CHECK(r != nullptr && r->key.empty() && r->value.size() == 3,
                 "empty key and an embedded-NUL value both round-trip correctly (length-prefixed, not delimiter-based)");
    }
}
