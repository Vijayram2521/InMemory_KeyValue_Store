#include "engine/index_checkpoint.h"
#include "tests.h"
#include "test_framework.h"
#include <filesystem>

void kv_tests::run_test_index_checkpoint() {
    std::cout << "--- [Test] Index Checkpoint (delcol -> key->location snapshot) ---" << std::endl;
    using kv_engine::IndexCheckpoint;
    using kv_engine::Location;
    using kv_engine::ShardedIndex;
    using kv_engine::kNumShards;

    std::string dir = "./TestStorage/test_index_checkpoint";
    cleanup_test_dir(dir);
    std::filesystem::create_directories(dir);

    KV_CHECK_FALSE(IndexCheckpoint::load_latest(dir).has_value(),
                   "load_latest on a directory with no CHECKPOINT pointer returns nullopt");

    const size_t other_shard = kNumShards > 1 ? kNumShards - 1 : 0;
    ShardedIndex shards;
    shards[0]["alpha"] = Location{1, 10};
    shards[0]["beta"] = Location{2, 20};
    shards[other_shard]["gamma"] = Location{3, 30};
    IndexCheckpoint::write(dir, /*covered_seq=*/42, shards);

    auto loaded = IndexCheckpoint::load_latest(dir);
    KV_CHECK(loaded.has_value(), "load_latest finds a checkpoint after write()");
    if (loaded) {
        KV_CHECK_EQ(uint64_t(42), loaded->covered_seq, "loaded covered_seq matches what was written");
        auto a = loaded->shards[0].find("alpha");
        KV_CHECK(a != loaded->shards[0].end() && a->second.file_seq == 1 && a->second.serial == 10,
                 "shard 0's 'alpha' entry round-trips exactly");
        auto b = loaded->shards[0].find("beta");
        KV_CHECK(b != loaded->shards[0].end() && b->second.file_seq == 2 && b->second.serial == 20,
                 "shard 0's 'beta' entry round-trips exactly");
        auto g = loaded->shards[other_shard].find("gamma");
        KV_CHECK(g != loaded->shards[other_shard].end() && g->second.file_seq == 3 && g->second.serial == 30,
                 "the other shard's 'gamma' entry round-trips in the correct shard, not shard 0");
        size_t total = 0;
        for (const auto& s : loaded->shards) total += s.size();
        KV_CHECK_EQ(size_t(3), total, "total entries across all shards matches what was written");
    }

    // A second, newer checkpoint should supersede the first: load_latest
    // returns only the new pass's data, and the old pass's shard files are
    // actually removed from disk (not left as orphans forever).
    std::string old_shard0_file = dir + "/checkpoint_0_42.idx";
    KV_CHECK(std::filesystem::exists(old_shard0_file), "sanity: first checkpoint's shard 0 file exists on disk");

    ShardedIndex shards2;
    shards2[0]["alpha"] = Location{99, 1}; // "alpha" moved to a new location
    IndexCheckpoint::write(dir, /*covered_seq=*/100, shards2);

    auto loaded2 = IndexCheckpoint::load_latest(dir);
    KV_CHECK(loaded2.has_value(), "load_latest finds the newer checkpoint");
    if (loaded2) {
        KV_CHECK_EQ(uint64_t(100), loaded2->covered_seq, "load_latest returns the NEWER pass's covered_seq");
        auto a = loaded2->shards[0].find("alpha");
        KV_CHECK(a != loaded2->shards[0].end() && a->second.file_seq == 99,
                 "load_latest returns the newer pass's data, not the old pass's");
        KV_CHECK(loaded2->shards[other_shard].find("gamma") == loaded2->shards[other_shard].end(),
                 "the newer pass genuinely replaces the old one -- 'gamma' isn't carried forward "
                 "since the second write() didn't include it");
    }
    KV_CHECK_FALSE(std::filesystem::exists(old_shard0_file),
                   "the superseded checkpoint's shard file is actually removed from disk, not orphaned");

    cleanup_test_dir(dir);
}
