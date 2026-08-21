#include "../include/engine/sstable.h"
#include <algorithm>
#include <fstream>
#include <cstdint>

namespace kv_engine {

namespace {
    // Trailer written at the very end of every SSTable file, at a fixed
    // offset from EOF, so a reader can seek(file_size - kFooterSize) without
    // needing to know anything else about the file up front.
    //   [0..8)   index block offset (from start of file)
    //   [8..16)  number of index entries
    //   [16..20) magic number, so a lookup fails fast on a non-SSTable file
    //            instead of interpreting garbage as an offset/count.
    constexpr uint64_t kSSTableMagic = 0x53535442; // "SSTB" as a little-endian u32
    constexpr size_t kFooterSize = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

    void write_put_record(std::ofstream& ofs, const std::string& key, const std::string& value) {
        char type = 1;
        uint32_t kLen = static_cast<uint32_t>(key.size());
        uint32_t vLen = static_cast<uint32_t>(value.size());
        ofs.write(&type, 1);
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(key.data(), kLen);
        ofs.write(reinterpret_cast<const char*>(&vLen), sizeof(vLen));
        ofs.write(value.data(), vLen);
    }

    uint64_t put_record_size(const std::string& key, const std::string& value) {
        return 1 + sizeof(uint32_t) + key.size() + sizeof(uint32_t) + value.size();
    }

    void write_delete_record(std::ofstream& ofs, const std::string& key) {
        char type = 2;
        uint32_t kLen = static_cast<uint32_t>(key.size());
        ofs.write(&type, 1);
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(key.data(), kLen);
    }

    uint64_t delete_record_size(const std::string& key) {
        return 1 + sizeof(uint32_t) + key.size();
    }
} // namespace

bool SSTable::write_file(const std::string& filename,
                          const std::map<std::string, std::string>& data,
                          const std::set<std::string>& tombstones,
                          std::vector<IndexEntry>& out_index) {
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs.is_open()) return false;

    out_index.clear();
    out_index.reserve(data.size() + tombstones.size());

    // Merge the two already-sorted containers into one ascending-key
    // sequence. A key can never appear in both: Put() clears any tombstone
    // for the key it just wrote, and Delete() clears any memtable entry for
    // the key it just marked deleted, so the two sets are always disjoint
    // for a single flush generation.
    auto it_data = data.begin();
    auto it_tomb = tombstones.begin();
    uint64_t offset = 0;

    while (it_data != data.end() || it_tomb != tombstones.end()) {
        bool take_data;
        if (it_tomb == tombstones.end()) take_data = true;
        else if (it_data == data.end()) take_data = false;
        else take_data = it_data->first < *it_tomb;

        if (take_data) {
            out_index.push_back({it_data->first, offset});
            write_put_record(ofs, it_data->first, it_data->second);
            offset += put_record_size(it_data->first, it_data->second);
            ++it_data;
        } else {
            out_index.push_back({*it_tomb, offset});
            write_delete_record(ofs, *it_tomb);
            offset += delete_record_size(*it_tomb);
            ++it_tomb;
        }
    }

    // Index block: one (keyLen, key, offset) triple per data record, same
    // ascending-key order as the data block above.
    uint64_t index_offset = offset;
    for (const auto& entry : out_index) {
        uint32_t kLen = static_cast<uint32_t>(entry.key.size());
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(entry.key.data(), kLen);
        ofs.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
    }

    // Footer.
    uint64_t index_count = out_index.size();
    uint32_t magic = static_cast<uint32_t>(kSSTableMagic);
    ofs.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    ofs.write(reinterpret_cast<const char*>(&index_count), sizeof(index_count));
    ofs.write(reinterpret_cast<const char*>(&magic), sizeof(magic));

    ofs.close();
    return true;
}

std::vector<IndexEntry> SSTable::load_index(const std::string& filename) {
    std::vector<IndexEntry> index;
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs.is_open()) return index;

    ifs.seekg(0, std::ios::end);
    std::streamoff file_size = ifs.tellg();
    if (file_size < static_cast<std::streamoff>(kFooterSize)) return index;

    ifs.seekg(file_size - static_cast<std::streamoff>(kFooterSize));
    uint64_t index_offset = 0, index_count = 0;
    uint32_t magic = 0;
    if (!ifs.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset))) return index;
    if (!ifs.read(reinterpret_cast<char*>(&index_count), sizeof(index_count))) return index;
    if (!ifs.read(reinterpret_cast<char*>(&magic), sizeof(magic))) return index;
    if (magic != static_cast<uint32_t>(kSSTableMagic)) return index;

    ifs.seekg(static_cast<std::streamoff>(index_offset));
    index.reserve(index_count);
    for (uint64_t i = 0; i < index_count; ++i) {
        uint32_t kLen = 0;
        if (!ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) break;
        std::string key(kLen, '\0');
        if (kLen > 0 && !ifs.read(&key[0], kLen)) break;
        uint64_t rec_offset = 0;
        if (!ifs.read(reinterpret_cast<char*>(&rec_offset), sizeof(rec_offset))) break;
        index.push_back({std::move(key), rec_offset});
    }
    return index;
}

SearchResult SSTable::search_with_index(const std::string& filename,
                                         const std::vector<IndexEntry>& index,
                                         const std::string& key) {
    auto it = std::lower_bound(index.begin(), index.end(), key,
        [](const IndexEntry& entry, const std::string& k) { return entry.key < k; });
    if (it == index.end() || it->key != key) {
        return {false, "", false};
    }

    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs.is_open()) return {false, "", false};
    ifs.seekg(static_cast<std::streamoff>(it->offset));

    char type_raw;
    if (!ifs.read(&type_raw, 1)) return {false, "", false};
    uint8_t type = static_cast<uint8_t>(type_raw);

    uint32_t kLen = 0;
    if (!ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) return {false, "", false};
    std::string found_key(kLen, '\0');
    if (kLen > 0 && !ifs.read(&found_key[0], kLen)) return {false, "", false};
    if (found_key != key) {
        // The index pointed somewhere that doesn't actually hold `key` --
        // treat as not-found rather than risk returning the wrong value.
        return {false, "", false};
    }

    if (type == 2) { // DELETE / tombstone
        return {true, "", true};
    }

    uint32_t vLen = 0;
    if (!ifs.read(reinterpret_cast<char*>(&vLen), sizeof(vLen))) return {false, "", false};
    std::string value(vLen, '\0');
    if (vLen > 0 && !ifs.read(&value[0], vLen)) return {false, "", false};
    return {true, value, false};
}

} // namespace kv_engine
