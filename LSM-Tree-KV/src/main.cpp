#include "engine/storage_engine.h"
#include <iostream>
#include <cassert>

using namespace kv_engine;

void run_persistence_test() {
    const std::string wal_path = "wal.log";

    // --- SESSION 1: Create and Write ---
    std::cout << "\n--- Session 1: Writing Data ---" << std::endl;
    {
        StorageEngine engine(wal_path);
        engine.Put("user_1", "Alice");
        engine.Put("user_2", "Bob");
        engine.Put("status", "Active");
        
        std::cout << "Data committed to WAL. Destroying engine now..." << std::endl;
        // Engine goes out of scope here and is DESTROYED
    }

    std::cout << "Engine is now DEAD. Memory is cleared." << std::endl;

    // --- SESSION 2: Recreate and Verify ---
    std::cout << "\n--- Session 2: Recovering Data ---" << std::endl;
    {
        StorageEngine engine(wal_path);
        
        // Check if the data "survived" the destruction of the first engine
        auto val1 = engine.Get("user_1");
        auto val2 = engine.Get("user_2");
        auto val3 = engine.Get("status");

        if (val1 && *val1 == "Alice" && val2 && *val2 == "Bob") {
            std::cout << "✅ SUCCESS: Data recovered from WAL!" << std::endl;
            std::cout << "user_1: " << *val1 << std::endl;
            std::cout << "user_2: " << *val2 << std::endl;
        } else {
            std::cout << "❌ FAILURE: Data was lost!" << std::endl;
        }
    }
}

int main() {
    try {
        run_persistence_test();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}