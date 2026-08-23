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
        // Returns the highest sequence number across all entries. Returns 0
        // if not found. Deliberately the max, not the last entry's -- once
        // compaction can rewrite the file with an entry that has a high
        // sequence number but sits earlier in the file (it's logically the
        // oldest surviving generation), "last line" and "highest sequence"
        // are no longer the same thing.
        static uint64_t get_last_seq(const std::string& dir);
        // Atomically replaces the entire MANIFEST file's contents with
        // `entries`, in the given order (temp file + rename, so a crash
        // never leaves a partially-written MANIFEST). Used by compaction to
        // retire two old generations and register one merged one in a
        // single durable step -- add_entry's simple append can't remove
        // entries, only add them.
        static void rewrite(const std::string& dir, const std::vector<ManifestEntry>& entries);
    };
}