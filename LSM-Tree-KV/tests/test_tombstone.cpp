#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <filesystem>

void kv_tests::run_test_tombstone() {
    std::cout << "--- [Test] Resurrection Bug (Tombstone Handling) ---" << std::endl;
    std::string path = "./TestStorage/test_db";
    cleanup_test_dir(path);
    kv_engine::StorageEngine engine(path);

    // 1. Initial state, flushed to disk as an SSTable.
    engine.Put("user_1", "active");
    engine.ForceFlush();

    // 2. Delete it, then write a different key and flush again so the
    //    tombstone for user_1 also lands on disk in a newer generation.
    engine.Delete("user_1");
    engine.Put("user_2", "active");
    engine.ForceFlush();

    KV_CHECK_FALSE(engine.Get("user_1").has_value(),
                   "Deleted key stays deleted after crossing an SSTable generation boundary");
    auto v2 = engine.Get("user_2");
    KV_CHECK(v2.has_value() && *v2 == "active", "Unrelated key written alongside the delete is unaffected");
}
