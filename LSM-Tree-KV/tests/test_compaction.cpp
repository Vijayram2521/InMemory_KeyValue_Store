#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace {
    size_t count_sst_files(const std::string& dir) {
        size_t count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            if (entry.path().extension() == ".sst") count++;
        }
        return count;
    }

    // Lexicographically-largest .sst filename in dir -- valid as "the
    // newest generation's path" only when called before any compaction has
    // run (regular flushes assign strictly-increasing, zero-padded
    // sequence numbers, so lexicographic order still matches recency then).
    std::string newest_sst_path_before_compaction(const std::string& dir) {
        std::string newest;
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            if (entry.path().extension() == ".sst") {
                std::string p = entry.path().string();
                if (p > newest) newest = p;
            }
        }
        return newest;
    }
}

void kv_tests::run_test_compaction() {
    std::cout << "--- [Test] Compaction ---" << std::endl;

    // 1. Below-threshold no-op: fewer than 3 flushed generations.
    {
        std::string path = "./TestStorage/test_compaction_below_threshold";
        cleanup_test_dir(path);
        kv_engine::StorageEngine engine(path);
        engine.Put("a", "1");
        engine.ForceFlush();
        engine.Put("b", "2");
        engine.ForceFlush();

        KV_CHECK_FALSE(engine.CompactOnce(),
                        "CompactOnce with only 2 flushed generations does nothing");
        KV_CHECK_EQ(size_t(2), count_sst_files(path),
                    "File count unchanged when compaction has nothing eligible");
    }

    // 2. Basic overwrite consolidation, and newest generation untouched.
    {
        std::string path = "./TestStorage/test_compaction_overwrite";
        cleanup_test_dir(path);
        kv_engine::StorageEngine engine(path);
        engine.Put("k", "v1");
        engine.ForceFlush(); // generation 0
        engine.Put("k", "v2");
        engine.ForceFlush(); // generation 1
        engine.Put("sentinel", "x");
        engine.ForceFlush(); // generation 2 -- must never be touched

        std::string newest_before = newest_sst_path_before_compaction(path);

        KV_CHECK(engine.CompactOnce(), "CompactOnce merges the two oldest generations");
        KV_CHECK_EQ(size_t(2), count_sst_files(path),
                    "3 generations become 2 after merging the oldest pair");

        auto v = engine.Get("k");
        KV_CHECK(v.has_value() && *v == "v2", "Newest value for an overwritten key survives compaction");
        auto sentinel = engine.Get("sentinel");
        KV_CHECK(sentinel.has_value() && *sentinel == "x", "Untouched newest generation's key still reads correctly");
        KV_CHECK(std::filesystem::exists(newest_before),
                 "The newest generation's own file is never renamed/removed by compaction");
    }

    // 3. Deleted-key reclamation: the critical correctness case -- the
    // tombstone must be fully dropped, not carried forward, and Get must
    // still correctly report the key as deleted afterward. "d" is the only
    // key in the oldest pair, so the merge produces zero surviving records
    // -- exercising the "both generations fully cancel out" path too:
    // 3 generations become 1 (2 removed, 0 replacement written), not 2.
    {
        std::string path = "./TestStorage/test_compaction_delete";
        cleanup_test_dir(path);
        kv_engine::StorageEngine engine(path);
        engine.Put("d", "v");
        engine.ForceFlush(); // generation 0: PUT d
        engine.Delete("d");
        engine.ForceFlush(); // generation 1: tombstone for d
        engine.Put("sentinel", "x");
        engine.ForceFlush(); // generation 2 -- untouched

        KV_CHECK(engine.CompactOnce(), "CompactOnce merges the PUT+tombstone pair");
        KV_CHECK_EQ(size_t(1), count_sst_files(path),
                    "A PUT+tombstone pair that cancels out entirely vanishes with no replacement file "
                    "(3 generations -> 1, not 2)");

        KV_CHECK_FALSE(engine.Get("d").has_value(),
                        "Deleted key still correctly reports absent after its tombstone is compacted away");
        auto sentinel = engine.Get("sentinel");
        KV_CHECK(sentinel.has_value() && *sentinel == "x",
                 "Untouched newest generation's key still reads correctly");
    }

    // 3b. Deleted-key reclamation where the pair does NOT fully cancel out
    // -- a surviving PUT alongside the dropped tombstone, so the merge
    // output is non-empty and a real replacement file IS written.
    {
        std::string path = "./TestStorage/test_compaction_delete_partial";
        cleanup_test_dir(path);
        kv_engine::StorageEngine engine(path);
        engine.Put("d", "v");
        engine.Put("keep", "still_here");
        engine.ForceFlush(); // generation 0: PUT d, PUT keep
        engine.Delete("d");
        engine.ForceFlush(); // generation 1: tombstone for d
        engine.Put("sentinel", "x");
        engine.ForceFlush(); // generation 2 -- untouched

        KV_CHECK(engine.CompactOnce(), "CompactOnce merges a pair with one surviving key and one dropped tombstone");
        KV_CHECK_EQ(size_t(2), count_sst_files(path),
                    "3 generations become 2 when the merge still has a surviving key to write");
        KV_CHECK_FALSE(engine.Get("d").has_value(), "Deleted key stays deleted");
        auto keep = engine.Get("keep");
        KV_CHECK(keep.has_value() && *keep == "still_here",
                 "A key untouched by the delete survives the merge alongside the dropped tombstone");
    }

    // 4. Repeated compaction converges: keep merging until nothing's left,
    // every key still correct throughout.
    {
        std::string path = "./TestStorage/test_compaction_converge";
        cleanup_test_dir(path);
        kv_engine::StorageEngine engine(path);
        for (int i = 0; i < 5; ++i) {
            engine.Put("k" + std::to_string(i), "v" + std::to_string(i));
            engine.ForceFlush();
        }
        KV_CHECK_EQ(size_t(5), count_sst_files(path), "5 puts with ForceFlush each produce 5 generations");

        int passes = 0;
        while (engine.CompactOnce()) {
            ++passes;
            if (passes > 10) break; // safety net against an infinite loop bug
        }
        KV_CHECK_EQ(size_t(2), count_sst_files(path),
                    "Repeated compaction converges to [fully-merged-oldest, single newest]");
        for (int i = 0; i < 5; ++i) {
            auto v = engine.Get("k" + std::to_string(i));
            KV_CHECK(v.has_value() && *v == ("v" + std::to_string(i)),
                     "Every key still reads correctly after full convergence: k" + std::to_string(i));
        }
    }

    // 5. Crash-recovery-style check: destroy and reopen the engine after
    // compaction, then do one more flush and confirm nothing collided.
    {
        std::string path = "./TestStorage/test_compaction_recovery";
        cleanup_test_dir(path);
        {
            kv_engine::StorageEngine engine(path);
            for (int i = 0; i < 4; ++i) {
                engine.Put("k" + std::to_string(i), "v" + std::to_string(i));
                engine.ForceFlush();
            }
            while (engine.CompactOnce()) {}
        }

        kv_engine::StorageEngine reopened(path);
        bool all_present = true;
        for (int i = 0; i < 4; ++i) {
            auto v = reopened.Get("k" + std::to_string(i));
            if (!v.has_value() || *v != ("v" + std::to_string(i))) all_present = false;
        }
        KV_CHECK(all_present, "All keys survive a full engine close+reopen after compaction");

        size_t before = count_sst_files(path);
        reopened.Put("new_key", "new_val");
        reopened.ForceFlush();
        KV_CHECK_EQ(before + 1, count_sst_files(path),
                    "A flush after reopening adds exactly one new file (no sequence-number collision)");

        all_present = true;
        for (int i = 0; i < 4; ++i) {
            auto v = reopened.Get("k" + std::to_string(i));
            if (!v.has_value() || *v != ("v" + std::to_string(i))) all_present = false;
        }
        KV_CHECK(all_present, "Pre-existing keys still read correctly after the post-reopen flush "
                               "(would fail if the new generation silently overwrote a compacted file)");
    }

    // 6. Concurrent Get() during CompactOnce(): smoke test, not a precise
    // timing assertion -- just proving no crash/deadlock and no missed
    // reads while compaction runs.
    {
        std::string path = "./TestStorage/test_compaction_concurrent";
        cleanup_test_dir(path);
        kv_engine::StorageEngine engine(path);
        for (int i = 0; i < 6; ++i) {
            engine.Put("sentinel", "stable_value");
            engine.Put("filler" + std::to_string(i), std::string(50, 'x'));
            engine.ForceFlush();
        }

        std::atomic<bool> stop{false};
        std::atomic<size_t> reads{0};
        std::atomic<bool> saw_wrong_value{false};
        std::thread reader([&]() {
            while (!stop) {
                auto v = engine.Get("sentinel");
                if (!v.has_value() || *v != "stable_value") saw_wrong_value = true;
                reads++;
            }
        });

        int passes = 0;
        while (engine.CompactOnce()) {
            ++passes;
            if (passes > 20) break;
        }
        stop = true;
        reader.join();

        KV_CHECK_FALSE(saw_wrong_value.load(),
                        "Concurrent Get() never observes a missing/wrong value while compaction runs");
        KV_CHECK(reads.load() > 0, "Concurrent reader thread actually got to run alongside compaction");
    }
}
