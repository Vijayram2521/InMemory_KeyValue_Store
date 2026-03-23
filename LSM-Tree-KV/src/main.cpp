#include <iostream>
#include <string>
#include "../include/engine/storage_engine.h"

/**
 * Basic CLI / Driver for the LSM-Tree Key-Value Store.
 * This file verifies our In-Memory implementation before we 
 * move on to the Write-Ahead Log (WAL).
 */
int main() {
    // 1. Initialize the Engine
    kv_engine::StorageEngine db;

    std::cout << "--- Starting LSM-KV Store Test ---" << std::endl;

    // 2. Test the "Put" (Write Path)
    std::cout << "Inserting keys..." << std::endl;
    db.Put("driver_id:101", "John_Doe");
    db.Put("driver_id:102", "Jane_Smith");
    db.Put("system_status", "ACTIVE");

    // 3. Test the "Get" (Read Path)
    std::cout << "Retrieving 'driver_id:101'..." << std::endl;
    auto result = db.Get("driver_id:101");

    if (result.has_value()) {
        std::cout << "SUCCESS: Found Value -> " << result.value() << std::endl;
    } else {
        std::cout << "FAILURE: Key not found." << std::endl;
    }

    // 4. Test "Delete" Logic
    std::cout << "Deleting 'system_status'..." << std::endl;
    if (db.Delete("system_status")) {
        std::cout << "Key deleted successfully." << std::endl;
    }

    // Verify deletion
    auto deleted_check = db.Get("system_status");
    if (!deleted_check.has_value()) {
        std::cout << "Verification: 'system_status' is no longer in memory." << std::endl;
    }

    // 5. Test Non-existent Key
    auto missing = db.Get("unknown_key");
    if (!missing.has_value()) {
        std::cout << "Correctly handled missing key 'unknown_key'." << std::endl;
    }

    std::cout << "--- All basic tests passed! ---" << std::endl;

    return 0;
}