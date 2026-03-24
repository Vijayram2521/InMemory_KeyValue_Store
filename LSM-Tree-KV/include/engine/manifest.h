#pragma once
#include <string>
#include <fstream>
#include <cstdint>

namespace kv_engine {
    class Manifest {
    public:
        // Reads the last sequence number from "MANIFEST" file. Returns 0 if not found.
        static uint64_t get_last_seq(const std::string& dir);
        
        // Atomically updates the "MANIFEST" file with a new sequence number.
        static void update_seq(const std::string& dir, uint64_t seq);
    };
}