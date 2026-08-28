#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <optional>
#include <set>
#include <vector>

namespace kv_engine {
    struct SearchResult {
        bool found = false;
        std::string value;
        uint64_t serial = 0;
    };

    // One entry in an SSTable's index block: a key, the byte offset (from
    // the start of the file) where that key's full record begins in the
    // data block, and that record's serial number (its 0-based position in
    // the file's own sorted sequence, assigned at flush/merge time -- used
    // to look up the record's bit in this file's del-bitmap). The index is
    // built in the same fully-sorted-by-key order as the data block, so it
    // can be binary searched directly.
    struct IndexEntry {
        std::string key;
        uint64_t offset = 0;
        uint64_t serial = 0;
    };

    // One decoded data-block record -- used internally by merge_files.
    // Every record still on disk represents a live PUT at write time --
    // deletes no longer produce their own on-disk record (see del-bitmap
    // below), so there is no tombstone record type to decode here anymore.
    struct Record {
        std::string key;
        std::string value;
        uint64_t serial = 0;
    };

    // RAII wrapper around a whole SSTable file mapped once via mmap() and
    // kept resident for the file's lifetime (populated alongside
    // index_cache/bloom_cache), so a Get() that reaches this file reads
    // straight out of mapped memory instead of paying a fresh
    // open()/seekg()/read() syscall sequence on every single lookup. The OS
    // still pages the underlying file in/out of physical RAM transparently
    // -- this doesn't bypass the page cache, it just removes the repeated
    // per-lookup syscall overhead on top of it. Experimental, opt-in (see
    // StorageEngine's use_mmap_reads) so it can be A/B compared directly
    // against the existing ifstream path rather than replacing it outright.
    class MappedFile {
    public:
        MappedFile() = default;
        explicit MappedFile(const std::string& filename);
        ~MappedFile();
        MappedFile(MappedFile&& other) noexcept;
        MappedFile& operator=(MappedFile&& other) noexcept;
        MappedFile(const MappedFile&) = delete;
        MappedFile& operator=(const MappedFile&) = delete;

        bool valid() const { return data_ != nullptr; }
        const char* data() const { return static_cast<const char*>(data_); }
        size_t size() const { return size_; }

    private:
        void* data_ = nullptr;
        size_t size_ = 0;
        void reset();
    };

    // On-disk layout written by write_file:
    //   [data block]  one live-PUT record per key, in ascending-key order,
    //                 each tagged with its serial number (0-based position
    //                 in this sequence). Deletes no longer produce their
    //                 own on-disk record -- see the del-bitmap below. Each
    //                 record's value is optionally LZ4-compressed
    //                 per-record (never block-level, so a lookup never
    //                 needs to decompress more than the one record it
    //                 asked for) -- see the `compress` parameter below.
    //                 The record is self-describing (carries its own
    //                 compressed/not flag), so files written with
    //                 compression on and off can coexist and are always
    //                 read correctly regardless of the reading engine
    //                 instance's own setting.
    //   [index block] one (keyLen, key, offset, serial) quad per data
    //                 record, in the same ascending-key order.
    //   [footer]      fixed-size trailer: index block's offset, how many
    //                 entries it holds, and a magic number so a lookup can
    //                 fail fast on a file that isn't this format.
    //
    // Every SSTable has a companion del-bitmap file (see the del-bitmap
    // functions below): a flat bit array, one bit per serial number, set
    // when that record has been superseded or deleted. Unlike the SSTable
    // itself, the del-bitmap is mutable -- individual bits are flipped in
    // place as keys are overwritten or deleted elsewhere, which is what
    // lets compaction reclaim space without needing to merge specifically
    // adjacent generations (see StorageEngine's eager dead-marking).
    class SSTable {
    public:
        // Writes `data`'s entries, in key order, into a single sorted
        // SSTable file, assigning each a serial number by position (0-based),
        // followed by its index and footer. On success, `out_index` receives
        // the index built while writing -- callers that just created the
        // file (StorageEngine::flush) can cache it directly instead of
        // re-reading the file to rebuild it. `compress`: attempt LZ4 on
        // each value, per-record; a value that doesn't actually shrink is
        // stored raw instead (never expands a record on disk).
        static bool write_file(const std::string& filename,
                                const std::map<std::string, std::string>& data,
                                std::vector<IndexEntry>& out_index,
                                bool compress);

        // Reads just the footer and index block of an existing SSTable file
        // (not the data block) and returns its index. Used to rebuild the
        // in-memory index cache for SSTables this process didn't just write
        // itself, e.g. ones recovered from the Manifest at startup.
        // Returns an empty vector if the file is missing, empty, or not in
        // this format (footer magic mismatch).
        static std::vector<IndexEntry> load_index(const std::string& filename);

        // Merges two SSTables (older_path strictly precedes newer_path in
        // generation order) into a single sorted output file at
        // output_path, streaming one record at a time from each input
        // rather than loading either file fully into memory -- peak memory
        // is O(1) per input file, not O(file size), which matters on
        // memory-constrained deployments where large accumulator files
        // previously caused OOM kills. Consults each input's del-bitmap
        // (older_del/newer_del, one bit per that file's own serial numbers)
        // and skips any record already marked dead -- everything else is
        // known-live by the eager dead-marking invariant (nothing behind it
        // still needs it), so it's copied forward with a freshly assigned
        // sequential serial number. On success, out_index receives the
        // index built while writing (same convention as write_file) and
        // out_del receives a fresh all-live del-bitmap sized to the actual
        // survivor count. `compress`: same meaning as write_file's --
        // applies to every record this merge writes, regardless of whether
        // the source records were themselves compressed (each is
        // decompressed by the read side transparently, then this decides
        // fresh whether to compress the output copy).
        static bool merge_files(const std::string& older_path, const std::vector<uint8_t>& older_del,
                                 const std::string& newer_path, const std::vector<uint8_t>& newer_del,
                                 const std::string& output_path,
                                 std::vector<IndexEntry>& out_index, std::vector<uint8_t>& out_del,
                                 bool compress);

        // Binary searches the (already sorted) `index` for `key`. On a
        // match, seeks straight to its byte offset and reads that one
        // record -- no scanning. If `key` isn't in `index`, it isn't in the
        // file; there is no scan fallback. Returning `found` says nothing
        // about whether the record is still live -- callers must separately
        // check the file's del-bitmap at `result.serial`.
        static SearchResult search_with_index(const std::string& filename,
                                               const std::vector<IndexEntry>& index,
                                               const std::string& key);

        // Same lookup, same no-scan-fallback guarantee, but reads directly
        // from an already-mapped file instead of opening a fresh ifstream.
        // Bounds-checks every read against mapped.size() before touching
        // it, same defensive posture as search_with_index -- a corrupt
        // offset fails the lookup rather than reading out of bounds.
        static SearchResult search_with_index_mmap(const MappedFile& mapped,
                                                     const std::vector<IndexEntry>& index,
                                                     const std::string& key);

        // --- Del-bitmap helpers (one bit per serial number in an SSTable) ---

        // A fresh, all-zero (all-live) bitmap sized to hold `num_bits` bits.
        static std::vector<uint8_t> make_del_bitmap(size_t num_bits);

        // Loads a del-bitmap from disk. Returns an empty vector if missing
        // or unreadable -- callers should treat an empty vector as "nothing
        // is marked dead" only when they also know no bits were ever validly
        // addressable (e.g. a brand new file); StorageEngine always creates
        // a del-bitmap alongside every SSTable, so a missing one here is a
        // genuine (defensive-only) error case, not expected in normal use.
        static std::vector<uint8_t> load_del_bitmap(const std::string& path);

        // Writes the full bitmap to disk, overwriting any existing file.
        // Used once, at creation time (a fresh flush or compaction output);
        // subsequent updates go through flip_dead_on_disk instead, which
        // touches only the single affected byte.
        static bool save_del_bitmap(const std::string& path, const std::vector<uint8_t>& bits);

        static bool is_dead(const std::vector<uint8_t>& bits, uint64_t serial);
        static void mark_dead_in_memory(std::vector<uint8_t>& bits, uint64_t serial);

        // Flips the bit for `serial` directly on disk with a single-byte
        // read-modify-write (no full-file rewrite). Callers are expected to
        // also call mark_dead_in_memory on their own cached copy of the
        // same bitmap so the in-memory and on-disk views stay consistent.
        static bool flip_dead_on_disk(const std::string& path, uint64_t serial);

        // Derives a SSTable's companion del-bitmap path (swaps the ".sst"
        // extension for ".del"). Both files always share the same base name.
        static std::string del_path_for(const std::string& sst_path);
    };
}
