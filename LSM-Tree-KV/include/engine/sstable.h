#pragma once
#include <map>
#include <string>
#include <optional>
#include <set>

namespace kv_engine {
    struct SearchResult {
        bool found = false;
        std::string value;
        bool is_tombstone = false;
    };
    class SSTable {
    public:
        // Writes a map to a file named "00000X.sst"
        static bool write_file(const std::string& filename, const std::map<std::string, std::string>& data,
                               const std::set<std::string>& tombstones);
        
        // Simple search: Since it's sorted, we could use binary search, 
        // but for now, we'll do a linear scan.
        static SearchResult search_file(const std::string& filename, const std::string& key);
    };
}