#include "engine/storage_engine.h"
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include "tests.h"
#include "test_framework.h"

using namespace kv_engine;
namespace fs = std::filesystem;

int main() {
    try {
        kv_tests::run_test_wal();
        std::cout << std::endl;

        kv_tests::run_test_sstable();
        std::cout << std::endl;

        kv_tests::run_test_sstable_index();
        std::cout << std::endl;

        kv_tests::run_test_del_bitmap();
        std::cout << std::endl;

        kv_tests::run_test_bloom_filter();
        std::cout << std::endl;

        kv_tests::run_test_tombstone();
        std::cout << std::endl;

        kv_tests::run_test_manifest();
        std::cout << std::endl;

        kv_tests::run_test_index_checkpoint();
        std::cout << std::endl;

        kv_tests::run_test_storage_engine_edge_cases();
        std::cout << std::endl;

        kv_tests::run_test_storage_engine_generations();
        std::cout << std::endl;

        kv_tests::run_test_global_index_restart();
        std::cout << std::endl;

        kv_tests::run_test_compaction();
        std::cout << std::endl;

        const auto& s = kv_tests::stats();
        std::cout << "===================================" << std::endl;
        std::cout << "Results: " << s.passed << " passed, " << s.failed << " failed" << std::endl;
        std::cout << "===================================" << std::endl;

        return s.failed == 0 ? 0 : 1;
    } catch (const std::exception& e) {
        std::cerr << "Test Suite crashed with error: " << e.what() << std::endl;
        return 1;
    }
}
