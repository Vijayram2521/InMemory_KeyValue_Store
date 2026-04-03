#include "engine/storage_engine.h"
#include "tests.h"
#include <iostream>
#include <cassert>
#include <filesystem>

void kv_tests::run_test_sstable() {
    std::cout << "\n--- [Test 2] Persist to Disk (SSTable) ---" << std::endl;
    std::string test_dir = "./TestStorage/test_disk";
    cleanup_test_dir(test_dir);

    {
        kv_engine::StorageEngine engine(test_dir);
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
    kv_engine::StorageEngine engine(test_dir);

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
