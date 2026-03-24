#include "engine/storage_engine.h"
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>

using namespace kv_engine;
namespace fs = std::filesystem;

void cleanup_test_dir(const std::string& dir) {
    if (fs::exists(dir)) {
        fs::remove_all(dir);
    }
}

// TEST 1: Crash and Recover (WAL Test)
void test_crash_and_recover() {
    std::cout << "\n--- [Test 1] Crash and Recover (WAL) ---" << std::endl;
    std::string test_dir = "./test_crash";
    cleanup_test_dir(test_dir);

    {
        StorageEngine engine(test_dir);
        engine.Put("session_1", "active");
        engine.Put("user_id", "101");
        std::cout << "Data written to WAL. Crashing engine..." << std::endl;
    }

    // Recreate engine and check if WAL replayed into MemTable
    StorageEngine engine(test_dir);
    auto val = engine.Get("user_id");
    if (val && *val == "101") {
        std::cout << "✅ SUCCESS: WAL replayed successfully." << std::endl;
    } else {
        std::cout << "❌ FAILURE: WAL data lost." << std::endl;
    }
}

// TEST 2: Persist to Disk (SSTable Test)
void test_persist_to_disk() {
    std::cout << "\n--- [Test 2] Persist to Disk (SSTable) ---" << std::endl;
    std::string test_dir = "./test_disk";
    cleanup_test_dir(test_dir);

    {
        StorageEngine engine(test_dir);
        // We put 8 keys. Since THRESHOLD = 5, the first 5 will flush to 000001.sst
        // The remaining 3 will stay in the new 000002.log
        std::cout << "Writing 8 keys to trigger a flush..." << std::endl;
        engine.Put("key_1", "Value 1"); // SSTable 1
        engine.Put("key_2", "Value 2"); // SSTable 1
        engine.Put("key_3", "Value 3"); // SSTable 1
        engine.Put("key_4", "Value 4"); // SSTable 1
        engine.Put("key_5", "Value 5"); // SSTable 1 (FLUSH TRIGGERED HERE)
        
        engine.Put("key_6", "Value 6"); // WAL 2
        engine.Put("key_7", "Value 7"); // WAL 2
        engine.Put("key_8", "Value 8"); // WAL 2
        
        std::cout << "Closing engine. MemTable for keys 1-5 is now cleared from RAM." << std::endl;
    }

    std::cout << "Restarting engine. Loading Manifest and scanning SSTables..." << std::endl;
    StorageEngine engine(test_dir);

    // Verify key from SSTable (Disk)
    auto v1 = engine.Get("key_1");
    // Verify key from WAL (New Active Log)
    auto v8 = engine.Get("key_8");

    bool success = true;
    if (!v1 || *v1 != "Value 1") {
        std::cout << "❌ FAILURE: Could not retrieve key_1 from SSTable!" << std::endl;
        success = false;
    }
    if (!v8 || *v8 != "Value 8") {
        std::cout << "❌ FAILURE: Could not retrieve key_8 from WAL!" << std::endl;
        success = false;
    }

    if (success) {
        std::cout << "✅ SUCCESS: Data retrieved from both SSTable and WAL!" << std::endl;
    }
}

int main() {
    try {
        test_crash_and_recover();
        test_persist_to_disk();
    } catch (const std::exception& e) {
        std::cerr << "Global Error: " << e.what() << std::endl;
    }
    return 0;
}