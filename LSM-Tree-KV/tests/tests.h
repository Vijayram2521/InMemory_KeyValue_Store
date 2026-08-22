#pragma once

#include <filesystem>
#include <fstream>
#include <string>

namespace kv_tests {
    inline void cleanup_test_dir(const std::string& dir) {
        namespace fs = std::filesystem;
        if (fs::exists(dir)) {
            fs::remove_all(dir);
        }
    }
    void run_test_wal();
    void run_test_sstable();
    void run_test_sstable_index();
    void run_test_bloom_filter();
    void run_test_tombstone();
    void run_test_manifest();
    void run_test_storage_engine_edge_cases();
    void run_test_storage_engine_generations();

    // Cluster-layer tests (kv_cluster_tests binary, Linux-only -- see
    // CMakeLists.txt's if(UNIX) guard).
    void run_test_hash_ring();
    void run_test_wire_protocol();
    void run_test_cluster_integration();
}