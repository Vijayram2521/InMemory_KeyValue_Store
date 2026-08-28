#include "engine/sstable.h"
#include "tests.h"
#include "test_framework.h"
#include <algorithm>
#include <filesystem>
#include <random>

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
    KV_CHECK(kv_engine::SSTable::write_file(path, data, index, /*compress=*/false), "write_file succeeds");
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

void kv_tests::run_test_value_compression() {
    std::cout << "--- [Test] LZ4 Value Compression ---" << std::endl;
    using kv_engine::SSTable;
    using kv_engine::IndexEntry;

    std::string dir = "./TestStorage/test_value_compression";
    cleanup_test_dir(dir);
    std::filesystem::create_directories(dir);

    // A highly-compressible value (a long run of one repeated character)
    // should produce a smaller file with compression on than off.
    std::string compressible_value(2000, 'x');
    std::map<std::string, std::string> data = {{"k1", compressible_value}};

    std::string path_uncompressed = dir + "/uncompressed.sst";
    std::string path_compressed = dir + "/compressed.sst";
    std::vector<IndexEntry> idx_uncompressed, idx_compressed;
    KV_CHECK(SSTable::write_file(path_uncompressed, data, idx_uncompressed, /*compress=*/false),
             "write_file (compress=false) succeeds on a highly-compressible value");
    KV_CHECK(SSTable::write_file(path_compressed, data, idx_compressed, /*compress=*/true),
             "write_file (compress=true) succeeds on the same value");

    std::error_code ec;
    uint64_t size_uncompressed = std::filesystem::file_size(path_uncompressed, ec);
    uint64_t size_compressed = std::filesystem::file_size(path_compressed, ec);
    KV_CHECK(size_compressed < size_uncompressed,
             "a highly-compressible value produces a smaller file with compression on (" +
             std::to_string(size_compressed) + " < " + std::to_string(size_uncompressed) + " bytes)");

    auto r1 = SSTable::search_with_index(path_compressed, idx_compressed, "k1");
    KV_CHECK(r1.found && r1.value == compressible_value,
             "a compressed value round-trips to exactly the original via search_with_index");

    // A poorly-compressible (effectively random) value must still round-trip
    // correctly, falling back to raw storage rather than expanding on disk.
    std::mt19937 rng(42);
    std::string random_value(1000, '\0');
    for (auto& c : random_value) c = static_cast<char>(rng() % 256);
    std::map<std::string, std::string> random_data = {{"k2", random_value}};
    std::string path_random = dir + "/random.sst";
    std::vector<IndexEntry> idx_random;
    KV_CHECK(SSTable::write_file(path_random, random_data, idx_random, /*compress=*/true),
             "write_file (compress=true) succeeds on a poorly-compressible value");
    auto r2 = SSTable::search_with_index(path_random, idx_random, "k2");
    KV_CHECK(r2.found && r2.value == random_value,
             "a poorly-compressible value still round-trips exactly (falls back to raw storage)");
    uint64_t size_random = std::filesystem::file_size(path_random, ec);
    KV_CHECK(size_random < random_value.size() + 200,
             "a poorly-compressible value's file isn't inflated by a failed compression attempt");

    // merge_files must carry records through a compaction pass correctly
    // regardless of whether they were originally written compressed --
    // RecordCursor decompresses on read, then this decides fresh whether
    // to (re-)compress the output copy per its own `compress` argument.
    std::string other_value(500, 'y');
    std::map<std::string, std::string> data_a = {{"a1", compressible_value}};
    std::map<std::string, std::string> data_b = {{"b1", other_value}};
    std::string path_a = dir + "/merge_a.sst";
    std::string path_b = dir + "/merge_b.sst";
    std::vector<IndexEntry> idx_a, idx_b;
    KV_CHECK(SSTable::write_file(path_a, data_a, idx_a, /*compress=*/true), "merge input A writes with compression");
    KV_CHECK(SSTable::write_file(path_b, data_b, idx_b, /*compress=*/true), "merge input B writes with compression");
    auto del_a = SSTable::make_del_bitmap(idx_a.size());
    auto del_b = SSTable::make_del_bitmap(idx_b.size());

    std::string merged_path = dir + "/merged.sst";
    std::vector<IndexEntry> merged_index;
    std::vector<uint8_t> merged_del;
    KV_CHECK(SSTable::merge_files(path_a, del_a, path_b, del_b, merged_path, merged_index, merged_del,
                                   /*compress=*/true),
             "merge_files succeeds with compression enabled on the output");
    auto rm1 = SSTable::search_with_index(merged_path, merged_index, "a1");
    KV_CHECK(rm1.found && rm1.value == compressible_value,
             "a record originally written compressed round-trips correctly through a compaction pass");
    auto rm2 = SSTable::search_with_index(merged_path, merged_index, "b1");
    KV_CHECK(rm2.found && rm2.value == other_value,
             "a second merged record also round-trips correctly");

    cleanup_test_dir(dir);
}
