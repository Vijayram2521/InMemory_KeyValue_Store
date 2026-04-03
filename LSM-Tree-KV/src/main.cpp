#include "engine/storage_engine.h"
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include "tests.h"

using namespace kv_engine;
namespace fs = std::filesystem;

int main() {
    try {
        kv_tests::run_test_wal();
        std::cout << std::endl;
        
        kv_tests::run_test_sstable();
        std::cout << std::endl;
        
        kv_tests::run_test_tombstone();
        std::cout << std::endl;

        std::cout << "All tests completed." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Test Suite crashed with error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}