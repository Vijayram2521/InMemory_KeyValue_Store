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

    cleanup_test_dir(dir);
}
