#pragma once
#include <map>
#include <string>
#include <optional>

namespace kv_engine {
    class SSTable {
    public:
        // Writes a map to a file named "00000X.sst"
        static bool write_file(const std::string& filename, const std::map<std::string, std::string>& data);
        
        // Simple search: Since it's sorted, we could use binary search, 
        // but for now, we'll do a linear scan.
        static std::optional<std::string> search_file(const std::string& filename, const std::string& key);
    };
}