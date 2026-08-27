#include "engine/storage_engine.h"
#include "tests.h"
#include "test_framework.h"
#include <atomic>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

// Concurrent Put/Delete/Get stress test for the per-shard-locked global key
// index (see storage_engine.cpp's struct-level lock-ordering comment). The
// existing suite already covers concurrent Get() during compaction
// (test_compaction.cpp); nothing until now exercised concurrent
// Put/Delete/Get against EACH OTHER directly, which is exactly what
// splitting the single engine-wide lock into memtable_lock/files_lock/
// shard_locks is meant to make safe under real parallelism.
//
// Correctness oracle: each writer thread owns a disjoint range of keys (no
// two threads ever write the same key), so there's no last-writer-wins
// ambiguity to resolve -- each thread can just track its own keys' final
// expected state locally, with no cross-thread synchronization needed for
// the tracking itself. Concurrent reader threads hammer Get() across the
// WHOLE keyspace (including other threads' in-flight keys) purely to stress
// the locking machinery -- their individual results aren't checked (a value
// mid-flight is expected to sometimes be old, sometimes new, sometimes
// absent-then-present), only that nothing crashes/hangs/corrupts state.
// After every thread joins, a single-threaded pass verifies every writer's
// keys ended up in exactly the state that writer's own log says they
// should.
void kv_tests::run_test_concurrency() {
    std::cout << "--- [Test] Concurrent Put/Delete/Get Stress ---" << std::endl;
    std::string path = "./TestStorage/test_concurrency";
    cleanup_test_dir(path);

    constexpr unsigned kWriterThreads = 6;
    constexpr unsigned kReaderThreads = 4;
    constexpr size_t kKeysPerWriter = 400;
    constexpr int kOpsPerKey = 12;

    // Each writer's local log of "what should this key be after all my
    // ops finish" -- built up as it goes, no locking needed since each
    // thread only ever touches its own map. Declared outside the engine's
    // scope below so it's still available for the post-restart check.
    std::vector<std::map<std::string, std::optional<std::string>>> expected(kWriterThreads);

    {
        // Scoped so `engine` is fully destructed (WAL file closed, any
        // final in-memory state settled) before `reopened` below opens
        // the same directory -- two StorageEngine instances open on one
        // data_dir at once is not a supported scenario (no inter-process/
        // inter-instance locking guards against it), and doing so here
        // produced exactly the kind of intermittent, hard-to-attribute
        // failures a real double-open would cause, before this scoping
        // fix made it clear that was a test bug, not an engine bug.
        kv_engine::StorageEngine engine(path, /*memtable_threshold=*/500);
        engine.StartBackgroundCompaction(std::chrono::seconds(1));

        std::atomic<bool> stop_readers{false};
        std::atomic<size_t> read_ops{0};

        std::vector<std::thread> readers;
        for (unsigned r = 0; r < kReaderThreads; ++r) {
            readers.emplace_back([&, r] {
                std::mt19937_64 rng(9000 + r);
                std::uniform_int_distribution<unsigned> writer_dist(0, kWriterThreads - 1);
                std::uniform_int_distribution<size_t> key_dist(0, kKeysPerWriter - 1);
                while (!stop_readers.load(std::memory_order_relaxed)) {
                    std::string key = "w" + std::to_string(writer_dist(rng)) + "_k" + std::to_string(key_dist(rng));
                    engine.Get(key); // result intentionally unchecked -- see file comment
                    read_ops.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        std::vector<std::thread> writers;
        for (unsigned w = 0; w < kWriterThreads; ++w) {
            writers.emplace_back([&, w] {
                std::mt19937_64 rng(1000 + w);
                std::uniform_int_distribution<size_t> key_dist(0, kKeysPerWriter - 1);
                std::uniform_real_distribution<double> unit(0.0, 1.0);
                for (int op = 0; op < static_cast<int>(kKeysPerWriter) * kOpsPerKey; ++op) {
                    std::string key = "w" + std::to_string(w) + "_k" + std::to_string(key_dist(rng));
                    if (unit(rng) < 0.25) {
                        engine.Delete(key);
                        expected[w][key] = std::nullopt;
                    } else {
                        std::string value = "val_" + std::to_string(w) + "_" + std::to_string(op);
                        engine.Put(key, value);
                        expected[w][key] = value;
                    }
                }
            });
        }

        for (auto& t : writers) t.join();
        stop_readers = true;
        for (auto& t : readers) t.join();

        KV_CHECK(read_ops.load() > 0, "reader threads actually got to run concurrently with the writers");

        bool all_correct = true;
        size_t checked = 0;
        for (unsigned w = 0; w < kWriterThreads; ++w) {
            for (const auto& [key, expected_value] : expected[w]) {
                auto actual = engine.Get(key);
                ++checked;
                if (expected_value.has_value()) {
                    if (!actual.has_value() || *actual != *expected_value) all_correct = false;
                } else {
                    if (actual.has_value()) all_correct = false;
                }
            }
        }
        KV_CHECK(all_correct, "every key's final value matches its owning thread's last write/delete, "
                              "after " + std::to_string(checked) + " keys checked across " +
                              std::to_string(kWriterThreads) + " concurrent writer threads");

        engine.StopBackgroundCompaction();
    } // engine destructs here

    // Restart and re-verify -- confirms the concurrently-built state (WAL,
    // flushed generations, del-bitmaps, global key index) is durable and
    // self-consistent, not just correct while still resident in memory.
    kv_engine::StorageEngine reopened(path, /*memtable_threshold=*/500);
    bool all_correct_after_restart = true;
    int mismatches_printed = 0;
    for (unsigned w = 0; w < kWriterThreads; ++w) {
        for (const auto& [key, expected_value] : expected[w]) {
            auto actual = reopened.Get(key);
            bool ok;
            if (expected_value.has_value()) {
                ok = actual.has_value() && *actual == *expected_value;
            } else {
                ok = !actual.has_value();
            }
            if (!ok) {
                all_correct_after_restart = false;
                if (mismatches_printed < 15) {
                    std::cout << "  [MISMATCH] key=" << key
                              << " expected=" << (expected_value.has_value() ? *expected_value : std::string("<absent>"))
                              << " actual=" << (actual.has_value() ? *actual : std::string("<absent>")) << std::endl;
                    ++mismatches_printed;
                }
            }
        }
    }
    KV_CHECK(all_correct_after_restart,
             "every key's final value still matches after a full engine restart");
}
