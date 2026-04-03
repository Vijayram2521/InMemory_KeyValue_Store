#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <cstdint>

namespace kv_engine {
    struct ManifestEntry {
        std::string filename;
        uint64_t sequence;
    };
    class Manifest {
    public:
        static void add_entry(const std::string& dir, const std::string& filename, uint64_t seq);
        static std::vector<ManifestEntry> load_history(const std::string& dir);
        // Reads the last sequence number from "MANIFEST" file. Returns 0 if not found.
        static uint64_t get_last_seq(const std::string& dir);
    };
}