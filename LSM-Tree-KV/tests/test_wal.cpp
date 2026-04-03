#include "engine/storage_engine.h"
#include "tests.h"
#include <iostream>
#include <cassert>
#include <filesystem>

void kv_tests::run_test_wal() {
    std::cout << "--- [Test 1] Crash and Recover (WAL) ---" << std::endl;
    std::string path = "./TestStorage/test_crash";
    cleanup_test_dir(path);

    {
        kv_engine::StorageEngine engine(path);
        engine.Put("key_1", "value_1");
        std::cout << "Data written to WAL. Crashing engine..." << std::endl;
    } // Scope ends, engine "crashes"

    kv_engine::StorageEngine recovery_engine(path);
    auto val = recovery_engine.Get("key_1");
    if (val && *val == "value_1") {
        std::cout << "✅ SUCCESS: WAL replayed successfully." << std::endl;
    } else {
        std::cout << "❌ FAILURE: WAL recovery failed." << std::endl;
    }
}