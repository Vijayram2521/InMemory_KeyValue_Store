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

    std::vector<kv_engine::IndexEntry> index;
    KV_CHECK(kv_engine::SSTable::write_file(path, data, index), "write_file succeeds");
    KV_CHECK_EQ(size_t(5), index.size(), "index has exactly one entry per key");

    bool sorted = std::is_sorted(index.begin(), index.end(),
        [](const kv_engine::IndexEntry& a, const kv_engine::IndexEntry& b) { return a.key < b.key; });
    KV_CHECK(sorted, "index returned by write_file is sorted by key (required for binary search)");

    // Serials are assigned by position in that same sorted order, 0..N-1 --
    // this is what lets a del-bitmap address a record by a plain array
    // index rather than needing any other key.
    bool serials_match_position = true;
    for (size_t i = 0; i < index.size(); ++i) {
        if (index[i].serial != i) serials_match_position = false;
    }
    KV_CHECK(serials_match_position, "write_file assigns serial numbers 0..N-1 by ascending-key position");

    auto r1 = kv_engine::SSTable::search_with_index(path, index, "banana");
    KV_CHECK(r1.found && r1.value == "yellow", "search_with_index finds a key with the correct value");
    KV_CHECK_EQ(uint64_t(1), r1.serial, "search_with_index returns the matched record's serial number");

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
            return a.key == b.key && a.offset == b.offset && a.serial == b.serial;
        });
    KV_CHECK(same, "load_index's on-disk round trip matches the in-memory index exactly, serials included");

    auto r4 = kv_engine::SSTable::search_with_index(path, loaded, "cherry");
    KV_CHECK(r4.found && r4.value == "dark red",
             "search_with_index works against a freshly loaded (not just freshly written) index");

    cleanup_test_dir(dir);
}

void kv_tests::run_test_del_bitmap() {
    std::cout << "--- [Test] Del-Bitmap (delCol) Helpers ---" << std::endl;
    using kv_engine::SSTable;

    auto bits = SSTable::make_del_bitmap(10);
    KV_CHECK_EQ(size_t(2), bits.size(), "make_del_bitmap(10) allocates ceil(10/8) = 2 bytes");
    for (uint64_t s = 0; s < 10; ++s) {
        KV_CHECK_FALSE(SSTable::is_dead(bits, s), "A freshly created bitmap has every bit live: serial " + std::to_string(s));
    }

    SSTable::mark_dead_in_memory(bits, 3);
    SSTable::mark_dead_in_memory(bits, 9);
    KV_CHECK(SSTable::is_dead(bits, 3), "mark_dead_in_memory sets the targeted bit");
    KV_CHECK(SSTable::is_dead(bits, 9), "mark_dead_in_memory sets a bit in the last byte correctly");
    KV_CHECK_FALSE(SSTable::is_dead(bits, 4), "mark_dead_in_memory leaves neighboring bits untouched");
    KV_CHECK_FALSE(SSTable::is_dead(bits, 0), "mark_dead_in_memory leaves unrelated bits untouched");

    // Out-of-range serials are treated as live rather than guessed at --
    // this matters for a freshly flushed file whose bitmap is sized to
    // memtable_threshold, which can exceed the actual record count.
    KV_CHECK_FALSE(SSTable::is_dead(bits, 1000), "is_dead treats an out-of-range serial as live, not dead");

    std::string dir = "./TestStorage/test_del_bitmap";
    cleanup_test_dir(dir);
    std::filesystem::create_directories(dir);
    std::string del_path = dir + "/000000.del";

    KV_CHECK(SSTable::save_del_bitmap(del_path, bits), "save_del_bitmap writes successfully");
    auto loaded = SSTable::load_del_bitmap(del_path);
    KV_CHECK(loaded == bits, "load_del_bitmap round-trips the exact bytes written by save_del_bitmap");

    // flip_dead_on_disk is a single-byte read-modify-write, independent of
    // save_del_bitmap/load_del_bitmap -- exercise it directly against the
    // file on disk, then confirm load_del_bitmap sees the change.
    KV_CHECK(SSTable::flip_dead_on_disk(del_path, 5), "flip_dead_on_disk succeeds on an existing file");
    auto reloaded = SSTable::load_del_bitmap(del_path);
    KV_CHECK(SSTable::is_dead(reloaded, 5), "flip_dead_on_disk's bit is visible after reloading from disk");
    KV_CHECK(SSTable::is_dead(reloaded, 3), "flip_dead_on_disk preserves a bit set earlier via save_del_bitmap");

    // Extending a too-short bitmap: flip a serial beyond the file's current
    // length and confirm it pads with zero bytes rather than failing.
    KV_CHECK(SSTable::flip_dead_on_disk(del_path, 40), "flip_dead_on_disk extends a too-short file with zero padding");
    auto extended = SSTable::load_del_bitmap(del_path);
    KV_CHECK(SSTable::is_dead(extended, 40), "The bit beyond the original file length is set after extension");
    KV_CHECK_FALSE(SSTable::is_dead(extended, 32), "Padding bytes introduced by extension default to live (zero)");

    KV_CHECK_EQ(std::string(dir + "/000000.del"), SSTable::del_path_for(dir + "/000000.sst"),
                "del_path_for swaps the .sst extension for .del");

    cleanup_test_dir(dir);
}
