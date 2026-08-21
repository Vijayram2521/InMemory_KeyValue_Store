#include "engine/sstable.h"
#include "tests.h"
#include "test_framework.h"
#include <algorithm>
#include <filesystem>

void kv_tests::run_test_sstable_index() {
    std::cout << "--- [Test] SSTable Binary-Search Index ---" << std::endl;
    std::string dir = "./TestStorage/test_sstable_index";
    cleanup_test_dir(dir);
    std::filesystem::create_directories(dir);
    std::string path = dir + "/000000.sst";

    std::map<std::string, std::string> data = {
        {"apple", "red"}, {"banana", "yellow"}, {"cherry", "dark red"},
        {"date", "brown"}, {"fig", "purple"}
    };
    // "eggplant" and "grape" alphabetically interleave with the data keys
    // above, so this also exercises write_file's put/delete merge logic,
    // not just a tombstones-are-separate-and-untested case.
    std::set<std::string> tombstones = {"eggplant", "grape"};

    std::vector<kv_engine::IndexEntry> index;
    KV_CHECK(kv_engine::SSTable::write_file(path, data, tombstones, index), "write_file succeeds");
    KV_CHECK_EQ(size_t(7), index.size(), "index has one entry per key (data + tombstones)");

    bool sorted = std::is_sorted(index.begin(), index.end(),
        [](const kv_engine::IndexEntry& a, const kv_engine::IndexEntry& b) { return a.key < b.key; });
    KV_CHECK(sorted, "index returned by write_file is sorted by key (required for binary search)");

    auto r1 = kv_engine::SSTable::search_with_index(path, index, "banana");
    KV_CHECK(r1.found && !r1.is_tombstone && r1.value == "yellow",
             "search_with_index finds a PUT key with the correct value");

    auto r2 = kv_engine::SSTable::search_with_index(path, index, "eggplant");
    KV_CHECK(r2.found && r2.is_tombstone,
             "search_with_index reports a tombstoned key as found+is_tombstone, not a miss");

    auto r3 = kv_engine::SSTable::search_with_index(path, index, "zucchini");
    KV_CHECK_FALSE(r3.found, "search_with_index reports a never-written key as not found");

    // load_index() is an independent read path (parses the footer and index
    // block fresh from disk) that must agree with the index write_file
    // handed back in memory -- this is what StorageEngine relies on to
    // rebuild the cache for SSTables recovered from the Manifest at startup.
    auto loaded = kv_engine::SSTable::load_index(path);
    KV_CHECK_EQ(index.size(), loaded.size(), "load_index recovers the same entry count from disk");
    bool same = index.size() == loaded.size() && std::equal(index.begin(), index.end(), loaded.begin(),
        [](const kv_engine::IndexEntry& a, const kv_engine::IndexEntry& b) {
            return a.key == b.key && a.offset == b.offset;
        });
    KV_CHECK(same, "load_index's on-disk round trip matches the in-memory index exactly");

    auto r4 = kv_engine::SSTable::search_with_index(path, loaded, "cherry");
    KV_CHECK(r4.found && !r4.is_tombstone && r4.value == "dark red",
             "search_with_index works against a freshly loaded (not just freshly written) index");

    cleanup_test_dir(dir);
}
