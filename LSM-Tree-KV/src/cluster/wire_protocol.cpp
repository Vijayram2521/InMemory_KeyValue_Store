#include "cluster/wire_protocol.h"

#include <cstring>

namespace kv_cluster {

namespace {

void append_u32_le(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void append_string(std::vector<uint8_t>& buf, const std::string& s) {
    append_u32_le(buf, static_cast<uint32_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

// Wraps [msg_type][fields...] with the 4-byte length prefix the socket
// layer needs. `body` is everything after msg_type is already appended.
std::vector<uint8_t> finish_frame(MsgType type, std::vector<uint8_t> body) {
    std::vector<uint8_t> frame;
    append_u32_le(frame, static_cast<uint32_t>(1 + body.size())); // msg_type + body
    frame.push_back(static_cast<uint8_t>(type));
    frame.insert(frame.end(), body.begin(), body.end());
    return frame;
}

// Reads a uint32 LE at `pos`, advances `pos` past it. Returns false (leaves
// `pos`/`out` untouched otherwise) if fewer than 4 bytes remain.
bool read_u32_le(const std::vector<uint8_t>& buf, size_t& pos, uint32_t& out) {
    if (pos + 4 > buf.size()) return false;
    out = static_cast<uint32_t>(buf[pos]) | (static_cast<uint32_t>(buf[pos + 1]) << 8) |
          (static_cast<uint32_t>(buf[pos + 2]) << 16) | (static_cast<uint32_t>(buf[pos + 3]) << 24);
    pos += 4;
    return true;
}

// Reads a length-prefixed string at `pos`, advances `pos` past it. Returns
// false if the buffer doesn't actually hold `len` more bytes after the
// length field -- guards against a truncated/malformed frame.
bool read_string(const std::vector<uint8_t>& buf, size_t& pos, std::string& out) {
    uint32_t len;
    if (!read_u32_le(buf, pos, len)) return false;
    if (pos + len > buf.size()) return false;
    out.assign(reinterpret_cast<const char*>(buf.data() + pos), len);
    pos += len;
    return true;
}

} // namespace

std::vector<uint8_t> encode_put_request(const std::string& key, const std::string& value) {
    std::vector<uint8_t> body;
    append_string(body, key);
    append_string(body, value);
    return finish_frame(MsgType::PutRequest, std::move(body));
}

std::vector<uint8_t> encode_get_request(const std::string& key) {
    std::vector<uint8_t> body;
    append_string(body, key);
    return finish_frame(MsgType::GetRequest, std::move(body));
}

std::vector<uint8_t> encode_delete_request(const std::string& key) {
    std::vector<uint8_t> body;
    append_string(body, key);
    return finish_frame(MsgType::DeleteRequest, std::move(body));
}

std::vector<uint8_t> encode_put_response(bool ok) {
    std::vector<uint8_t> body{static_cast<uint8_t>(ok ? 1 : 0)};
    return finish_frame(MsgType::PutResponse, std::move(body));
}

std::vector<uint8_t> encode_get_response(bool found, const std::string& value) {
    std::vector<uint8_t> body{static_cast<uint8_t>(found ? 1 : 0)};
    append_string(body, found ? value : std::string());
    return finish_frame(MsgType::GetResponse, std::move(body));
}

std::vector<uint8_t> encode_delete_response(bool ok) {
    std::vector<uint8_t> body{static_cast<uint8_t>(ok ? 1 : 0)};
    return finish_frame(MsgType::DeleteResponse, std::move(body));
}

std::vector<uint8_t> encode_error_response(const std::string& message) {
    std::vector<uint8_t> body;
    append_string(body, message);
    return finish_frame(MsgType::ErrorResponse, std::move(body));
}

std::optional<Message> decode_payload(const std::vector<uint8_t>& payload) {
    if (payload.empty()) return std::nullopt;
    MsgType type = static_cast<MsgType>(payload[0]);
    size_t pos = 1;

    switch (type) {
        case MsgType::PutRequest: {
            PutRequest r;
            if (!read_string(payload, pos, r.key)) return std::nullopt;
            if (!read_string(payload, pos, r.value)) return std::nullopt;
            return Message{std::move(r)};
        }
        case MsgType::GetRequest: {
            GetRequest r;
            if (!read_string(payload, pos, r.key)) return std::nullopt;
            return Message{std::move(r)};
        }
        case MsgType::DeleteRequest: {
            DeleteRequest r;
            if (!read_string(payload, pos, r.key)) return std::nullopt;
            return Message{std::move(r)};
        }
        case MsgType::PutResponse: {
            if (pos >= payload.size()) return std::nullopt;
            PutResponse r{payload[pos] != 0};
            return Message{r};
        }
        case MsgType::GetResponse: {
            if (pos >= payload.size()) return std::nullopt;
            bool found = payload[pos] != 0;
            pos += 1;
            GetResponse r;
            r.found = found;
            if (!read_string(payload, pos, r.value)) return std::nullopt;
            return Message{std::move(r)};
        }
        case MsgType::DeleteResponse: {
            if (pos >= payload.size()) return std::nullopt;
            DeleteResponse r{payload[pos] != 0};
            return Message{r};
        }
        case MsgType::ErrorResponse: {
            ErrorResponse r;
            if (!read_string(payload, pos, r.message)) return std::nullopt;
            return Message{std::move(r)};
        }
        default:
            return std::nullopt;
    }
}

std::optional<Message> receive_message(TcpSocket& sock) {
    std::vector<uint8_t> len_bytes;
    if (!sock.recv_exact(4, len_bytes)) return std::nullopt;
    uint32_t payload_len = static_cast<uint32_t>(len_bytes[0]) |
                            (static_cast<uint32_t>(len_bytes[1]) << 8) |
                            (static_cast<uint32_t>(len_bytes[2]) << 16) |
                            (static_cast<uint32_t>(len_bytes[3]) << 24);

    std::vector<uint8_t> payload;
    if (!sock.recv_exact(payload_len, payload)) return std::nullopt;

    return decode_payload(payload);
}

} // namespace kv_cluster
