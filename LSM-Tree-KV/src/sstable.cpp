#include "../include/engine/sstable.h"
#include <fstream>
#include <cstdint>

namespace kv_engine {

bool SSTable::write_file(const std::string& filename, const std::map<std::string, std::string>& data,
                       const std::set<std::string>& tombstones) {
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs.is_open()) return false;

    for (const auto& [key, value] : data) {
        char type = 1;
        uint32_t kLen = static_cast<uint32_t>(key.size());
        uint32_t vLen = static_cast<uint32_t>(value.size());

        ofs.write(&type, 1);
        // Write Key
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(key.data(), kLen);

        // Write Value
        ofs.write(reinterpret_cast<const char*>(&vLen), sizeof(vLen));
        ofs.write(value.data(), vLen);
    }

    for (const auto& key : tombstones) {
        char type = 2; // DELETE
        uint32_t k_len = key.size();
        uint32_t v_len = 0; // No value for tombstones

        ofs.write(&type, 1);
        ofs.write(reinterpret_cast<const char*>(&k_len), 4);
        ofs.write(key.data(), k_len);
        ofs.write(reinterpret_cast<const char*>(&v_len), 4);
    }

    ofs.close();
    return true;
}

SearchResult SSTable::search_file(const std::string& filename, const std::string& key) {
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs.is_open()) {
            return {false, "", false}; 
        }

        while (ifs.peek() != EOF) {
            char type_raw;
            uint32_t kLen, vLen;

            // 1. Read the Type Marker (1 byte)
            if (!ifs.read(&type_raw, sizeof(type_raw))) break;
            uint8_t type = static_cast<uint8_t>(type_raw);

            // 2. Read Key Length and the Key itself
            if (!ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) break;
            std::string current_key(kLen, '\0');
            ifs.read(&current_key[0], kLen);

            // 3. Logic: Did we find the key?
            if (current_key == key) {
                if (type == 2) { // DELETE / Tombstone
                    return {true, "", true}; 
                } else { // PUT / Value
                    if (!ifs.read(reinterpret_cast<char*>(&vLen), sizeof(vLen))) break;
                    std::string value(vLen, '\0');
                    ifs.read(&value[0], vLen);
                    return {true, value, false};
                }
            } else {
                // 4. Key mismatch: Skip the rest of this record to move to the next
                if (type == 1) { // Only PUTs have a value length and body to skip
                    if (!ifs.read(reinterpret_cast<char*>(&vLen), sizeof(vLen))) break;
                    ifs.seekg(vLen, std::ios::cur);
                }
                // Note: If type is 2 (DELETE), there is no value field, so we are already 
                // at the start of the next record.
            }
        }

        return {false, "", false}; 
    }
}