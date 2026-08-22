#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "cluster/tcp_socket.h"

namespace kv_cluster {

// Wire format, used identically for client<->leader and leader<->compute-
// node (the leader is a thin proxy, no protocol translation):
//
//   [4B little-endian uint32 payload_len][1B msg_type][payload...]
//
// payload_len counts msg_type + payload, NOT the 4-byte length prefix
// itself -- a reader does recv_exact(4) to learn payload_len, then
// recv_exact(payload_len) to get everything decode_payload() needs.
//
// Field framing inside each payload (uint32 lengths, raw bytes after)
// mirrors the on-disk record framing already used in wal.cpp/sstable.cpp.
enum class MsgType : uint8_t {
    PutRequest = 0x01,
    GetRequest = 0x02,
    DeleteRequest = 0x03,
    PutResponse = 0x81,
    GetResponse = 0x82,
    DeleteResponse = 0x83,
    ErrorResponse = 0xFF,
};

struct PutRequest    { std::string key; std::string value; };
struct GetRequest    { std::string key; };
struct DeleteRequest { std::string key; };
struct PutResponse    { bool ok; };
struct GetResponse    { bool found; std::string value; };
struct DeleteResponse { bool ok; };
struct ErrorResponse  { std::string message; };

using Message = std::variant<PutRequest, GetRequest, DeleteRequest,
                              PutResponse, GetResponse, DeleteResponse,
                              ErrorResponse>;

// Each encode_* function returns a COMPLETE frame (length prefix + type +
// payload), ready to hand straight to TcpSocket::send_all -- no separate
// length-prefixing step needed by the caller.
std::vector<uint8_t> encode_put_request(const std::string& key, const std::string& value);
std::vector<uint8_t> encode_get_request(const std::string& key);
std::vector<uint8_t> encode_delete_request(const std::string& key);
std::vector<uint8_t> encode_put_response(bool ok);
std::vector<uint8_t> encode_get_response(bool found, const std::string& value);
std::vector<uint8_t> encode_delete_response(bool ok);
std::vector<uint8_t> encode_error_response(const std::string& message);

// Decodes ONE payload (everything AFTER the 4-byte length prefix -- the
// socket layer strips that separately since it needs the length before it
// even has a complete payload to hand here). Returns nullopt on anything
// malformed (truncated field, unknown msg_type, trailing garbage).
std::optional<Message> decode_payload(const std::vector<uint8_t>& payload);

// Reads one complete frame off `sock` (length prefix, then that many
// payload bytes, both via recv_exact so short reads are handled) and
// decodes it. Returns nullopt if the connection closes/errors before a
// full frame arrives, or if the payload doesn't decode. Shared by
// ComputeNodeServer and LeaderServer so this logic exists exactly once.
std::optional<Message> receive_message(TcpSocket& sock);

} // namespace kv_cluster
