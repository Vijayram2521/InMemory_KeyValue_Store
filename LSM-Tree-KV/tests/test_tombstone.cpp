#include "engine/storage_engine.h"
#include "tests.h"
#include <iostream>
#include <cassert>
#include <filesystem>

void kv_tests::run_test_tombstone() {
    std::string path = "./TestStorage/test_db";
    cleanup_test_dir(path);
    kv_engine::StorageEngine engine(path);
    std::cout << "\n--- [Test 3] Resurrection Bug (Tombstone Handling) ---" << std::endl;
    
    // 1. Initial State
    engine.Put("user_1", "active");
    engine.ForceFlush(); // Ensure it hits the disk as an SSTable
    
    // 2. The Delete
    engine.Delete("user_1"); 
    engine.Put("user_2", "active"); 
    engine.ForceFlush(); 
    
    // 3. The "Crash" (Simulate by closing and reopening)
    if(engine.Get("user_1").has_value()) {
        std::cout << "user_1 : " << *engine.Get("user_1") << std::endl;
        std::cout << "❌ FAILURE: user_1 should be deleted but is still retrievable!" << std::endl;
    } else {
        std::cout << "✅ SUCCESS: user_1 correctly marked as deleted." << std::endl;
    }

}