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

// Restart-correctness scenarios specific to the global key->location index
// (see include/engine/index_checkpoint.h and StorageEngine's constructor):
// the checkpoint is allowed to be stale relative to the live engine, and
// restart must still produce correct Get() results regardless of what
// changed since the last checkpoint was written.
void kv_tests::run_test_global_index_restart() {
    std::cout << "--- [Test] Global Key Index: Restart Correctness ---" << std::endl;

    // 1. A key written to a NEW generation after the last checkpoint is
    // still found correctly after restart (exercises fold-in-newer-
    // generations): the checkpoint only knows about "k1"; "k2" is flushed
    // to its own later generation afterward, with a fresh WAL segment
    // starting at that flush -- so by the time the engine "crashes" (goes
    // out of scope with no further writes), the WAL segment that gets
    // replayed at restart is empty and contributes nothing to finding
    // "k2"; only the checkpoint+fold-in path (or the pre-existing
    // per-generation scan, as a safety net) can produce the right answer.
    {
        std::string path = "./TestStorage/test_global_index_restart_newer_gen";
        cleanup_test_dir(path);
        {
            kv_engine::StorageEngine engine(path);
            engine.Put("k1", "v1");
            engine.ForceFlush(); // generation 0
            KV_CHECK(engine.CheckpointIndexOnce(), "checkpoint succeeds after generation 0");
            engine.Put("k2", "v2");
            engine.ForceFlush(); // generation 1 -- created after the checkpoint
        }
        kv_engine::StorageEngine reopened(path);
        auto v1 = reopened.Get("k1");
        KV_CHECK(v1.has_value() && *v1 == "v1", "checkpoint-covered key still reads correctly after restart");
        auto v2 = reopened.Get("k2");
        KV_CHECK(v2.has_value() && *v2 == "v2",
                 "key flushed to a generation created AFTER the checkpoint is still found after restart");
    }

    // 2. A key deleted after the last checkpoint, with no further writes,
    // still correctly reads as absent after restart -- end-to-end
    // regression coverage for the "checkpoint says alive, but it died
    // without a new generation to reveal that" staleness case.
    {
        std::string path = "./TestStorage/test_global_index_restart_deleted";
        cleanup_test_dir(path);
        {
            kv_engine::StorageEngine engine(path);
            engine.Put("d", "v");
            engine.ForceFlush(); // generation 0
            KV_CHECK(engine.CheckpointIndexOnce(), "checkpoint succeeds with 'd' present");
            engine.Delete("d"); // marks generation 0's record dead; no new generation created
        }
        kv_engine::StorageEngine reopened(path);
        KV_CHECK_FALSE(reopened.Get("d").has_value(),
                        "key deleted after the last checkpoint (no further flush) is still correctly "
                        "absent after restart");
    }

    // 3. A key whose only copy is still in the memtable at "crash" time
    // (never flushed) is recovered correctly via ordinary WAL replay --
    // same apply_put path a live Put uses, so the global index behaves
    // identically whether the Put happened live or via replay.
    {
        std::string path = "./TestStorage/test_global_index_restart_memtable_only";
        cleanup_test_dir(path);
        {
            kv_engine::StorageEngine engine(path);
            engine.Put("base", "b");
            engine.ForceFlush(); // generation 0
            KV_CHECK(engine.CheckpointIndexOnce(), "checkpoint succeeds after generation 0");
            engine.Put("unflushed", "u"); // stays in the memtable, never flushed
        }
        kv_engine::StorageEngine reopened(path);
        auto v = reopened.Get("unflushed");
        KV_CHECK(v.has_value() && *v == "u",
                 "a key still in the memtable at crash time is recovered correctly via WAL replay");
    }
}
