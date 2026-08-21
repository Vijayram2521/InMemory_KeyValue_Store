#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <filesystem>

void kv_tests::run_test_wal() {
    std::cout << "--- [Test] Crash and Recover (WAL) ---" << std::endl;
    std::string path = "./TestStorage/test_crash";
    cleanup_test_dir(path);

    {
        kv_engine::StorageEngine engine(path);
        KV_CHECK(engine.Put("key_1", "value_1"), "Put succeeds before simulated crash");
    } // Scope ends, engine "crashes" (no explicit shutdown/flush)

    kv_engine::StorageEngine recovery_engine(path);
    auto val = recovery_engine.Get("key_1");
    KV_CHECK(val.has_value(), "WAL replay recovers key_1 after restart");
    if (val.has_value()) {
        KV_CHECK_EQ(std::string("value_1"), *val, "Recovered value matches what was written");
    }
}
