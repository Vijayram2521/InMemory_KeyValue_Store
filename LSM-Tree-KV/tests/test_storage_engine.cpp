#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <filesystem>

void kv_tests::run_test_storage_engine_edge_cases() {
    std::cout << "--- [Test] StorageEngine Edge Cases ---" << std::endl;
    std::string path = "./TestStorage/test_edge_cases";
    cleanup_test_dir(path);

    kv_engine::StorageEngine engine(path);

    KV_CHECK_FALSE(engine.Get("missing").has_value(), "Get on a never-written key returns nullopt");

    engine.Put("k", "v1");
    engine.Put("k", "v2");
    auto v = engine.Get("k");
    KV_CHECK(v.has_value() && *v == "v2", "Put on an existing key overwrites it with the latest value");

    engine.Delete("k");
    KV_CHECK_FALSE(engine.Get("k").has_value(), "Get right after Delete returns nullopt");

    engine.Put("k", "v3");
    auto v2 = engine.Get("k");
    KV_CHECK(v2.has_value() && *v2 == "v3", "Put after Delete revives the key with the new value");

    KV_CHECK(engine.Delete("never_existed"), "Delete on a key that was never written still succeeds");
    KV_CHECK_FALSE(engine.Get("never_existed").has_value(), "Get on a key deleted-without-ever-existing returns nullopt");
}

void kv_tests::run_test_storage_engine_generations() {
    std::cout << "--- [Test] StorageEngine Cross-Generation Search Order ---" << std::endl;
    std::string path = "./TestStorage/test_generations";
    cleanup_test_dir(path);

    {
        kv_engine::StorageEngine engine(path);
        engine.Put("dup_key", "old_value");
        engine.ForceFlush(); // generation 0 -> disk

        engine.Put("dup_key", "new_value");
        engine.ForceFlush(); // generation 1 -> disk, should shadow generation 0 for the same key
    }

    kv_engine::StorageEngine engine(path);
    auto val = engine.Get("dup_key");
    KV_CHECK(val.has_value(), "Key present in two SSTable generations is found after restart");
    if (val.has_value()) {
        KV_CHECK_EQ(std::string("new_value"), *val,
                    "Newest SSTable generation wins over an older one for the same key");
    }
}
