#include "engine/manifest.h"
#include "tests.h"
#include "test_framework.h"
#include <cstdint>
#include <filesystem>

void kv_tests::run_test_manifest() {
    std::cout << "--- [Test] Manifest Load/Store ---" << std::endl;
    std::string dir = "./TestStorage/test_manifest";
    cleanup_test_dir(dir);
    std::filesystem::create_directories(dir);

    KV_CHECK_EQ(uint64_t(0), kv_engine::Manifest::get_last_seq(dir),
                "get_last_seq on a directory with no MANIFEST file returns 0");
    KV_CHECK(kv_engine::Manifest::load_history(dir).empty(),
             "load_history on a directory with no MANIFEST file returns empty");

    kv_engine::Manifest::add_entry(dir, "000000.sst", 0);
    kv_engine::Manifest::add_entry(dir, "000001.sst", 1);
    kv_engine::Manifest::add_entry(dir, "000002.sst", 2);

    auto history = kv_engine::Manifest::load_history(dir);
    KV_CHECK_EQ(size_t(3), history.size(), "History has one entry per add_entry call");
    if (history.size() == 3) {
        KV_CHECK_EQ(std::string("000000.sst"), history[0].filename, "History preserves insertion order (oldest first)");
        KV_CHECK_EQ(std::string("000002.sst"), history[2].filename, "History preserves insertion order (newest last)");
    }
    KV_CHECK_EQ(uint64_t(2), kv_engine::Manifest::get_last_seq(dir),
                "get_last_seq returns the sequence number of the most recently added entry");

    // rewrite() must fully REPLACE the file's contents, not union with what
    // add_entry already wrote.
    kv_engine::Manifest::rewrite(dir, {
        {"000005.sst", 5},
        {"000002.sst", 2},
    });
    auto rewritten = kv_engine::Manifest::load_history(dir);
    KV_CHECK_EQ(size_t(2), rewritten.size(), "rewrite() replaces prior entries rather than appending to them");
    if (rewritten.size() == 2) {
        KV_CHECK_EQ(std::string("000005.sst"), rewritten[0].filename,
                    "rewrite() preserves the given entry order (here: the merged file positioned first/oldest)");
        KV_CHECK_EQ(std::string("000002.sst"), rewritten[1].filename,
                    "rewrite()'s second entry survives in position");
    }

    // get_last_seq must be the MAX sequence across all entries, not just
    // the last line's -- this is exactly what compaction produces: a
    // freshly-numbered (high-sequence) merged file positioned FIRST in the
    // file because it's logically the oldest surviving generation. The old
    // "last line" logic would return 2 here, which is wrong -- 5 is the
    // true highest sequence ever used, and a caller trusting the wrong
    // answer could reuse sequence 5 for a new flush and silently overwrite
    // the compacted file.
    KV_CHECK_EQ(uint64_t(5), kv_engine::Manifest::get_last_seq(dir),
                "get_last_seq returns the true maximum sequence even when it isn't the last line");

    cleanup_test_dir(dir);
}
