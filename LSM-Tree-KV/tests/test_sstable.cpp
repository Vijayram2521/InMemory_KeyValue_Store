#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <filesystem>

void kv_tests::run_test_sstable() {
    std::cout << "--- [Test] Persist to Disk (SSTable) ---" << std::endl;
    std::string test_dir = "./TestStorage/test_disk";
    cleanup_test_dir(test_dir);

    {
        kv_engine::StorageEngine engine(test_dir);
        // We put 8 keys. Since THRESHOLD = 5, the first 5 will flush to 000000.sst
        // and the remaining 3 will stay in the new active WAL.
        engine.Put("key_1", "Value 1");
        engine.Put("key_2", "Value 2");
        engine.Put("key_3", "Value 3");
        engine.Put("key_4", "Value 4");
        engine.Put("key_5", "Value 5"); // FLUSH TRIGGERED HERE
        engine.Put("key_6", "Value 6");
        engine.Put("key_7", "Value 7");
        engine.Put("key_8", "Value 8");
    } // engine closes; keys 1-5 only survive if the flush actually landed on disk

    kv_engine::StorageEngine engine(test_dir);

    auto v1 = engine.Get("key_1"); // expected to come from the flushed SSTable
    KV_CHECK(v1.has_value() && *v1 == "Value 1", "key_1 retrievable from flushed SSTable after restart");

    auto v8 = engine.Get("key_8"); // expected to come from WAL replay
    KV_CHECK(v8.has_value() && *v8 == "Value 8", "key_8 retrievable from WAL replay after restart");

    auto missing = engine.Get("key_does_not_exist");
    KV_CHECK_FALSE(missing.has_value(), "Get on a key never written returns nullopt");
}
