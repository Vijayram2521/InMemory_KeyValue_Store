#include "../include/engine/sstable.h"
#include <fstream>
#include <cstdint>

namespace kv_engine {

bool SSTable::write_file(const std::string& filename, const std::map<std::string, std::string>& data) {
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs.is_open()) return false;

    for (const auto& [key, value] : data) {
        uint32_t kLen = static_cast<uint32_t>(key.size());
        uint32_t vLen = static_cast<uint32_t>(value.size());

        // Write Key
        ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        ofs.write(key.data(), kLen);

        // Write Value
        ofs.write(reinterpret_cast<const char*>(&vLen), sizeof(vLen));
        ofs.write(value.data(), vLen);
    }

    ofs.close();
    return true;
}

std::optional<std::string> SSTable::search_file(const std::string& filename, const std::string& key) {
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs.is_open()) return std::nullopt;

    while (ifs.peek() != EOF) {
        uint32_t kLen, vLen;
        
        ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen));
        std::string current_key(kLen, ' ');
        ifs.read(&current_key[0], kLen);

        ifs.read(reinterpret_cast<char*>(&vLen), sizeof(vLen));
        if (current_key == key) {
            std::string value(vLen, ' ');
            ifs.read(&value[0], vLen);
            return value;
        } else {
            // Skip the value if the key doesn't match
            ifs.seekg(vLen, std::ios::cur);
        }
    }
    return std::nullopt;
}

}