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
        bool is_tombstone = false;
    };

    // One entry in an SSTable's index block: a key and the byte offset (from
    // the start of the file) where that key's full record begins in the
    // data block. The index is built in the same fully-sorted-by-key order
    // as the data block, so it can be binary searched directly.
    struct IndexEntry {
        std::string key;
        uint64_t offset = 0;
    };

    // One decoded data-block record -- used internally by merge_files.
    struct Record {
        std::string key;
        std::string value;
        bool is_tombstone = false;
    };

    // On-disk layout written by write_file:
    //   [data block]  PUT/DELETE records for every key, in a single merged
    //                 ascending-key sequence (not "all puts then all
    //                 deletes" -- that would break the sort order the index
    //                 and binary search below both depend on).
    //   [index block] one (keyLen, key, offset) triple per data record, in
    //                 the same ascending-key order.
    //   [footer]      fixed-size trailer: index block's offset, how many
    //                 entries it holds, and a magic number so a lookup can
    //                 fail fast on a file that isn't this format.
    class SSTable {
    public:
        // Writes `data` (puts) merged with `tombstones` (deletes) into a
        // single sorted SSTable file, followed by its index and footer.
        // On success, `out_index` receives the index built while writing --
        // callers that just created the file (StorageEngine::flush) can
        // cache it directly instead of re-reading the file to rebuild it.
        static bool write_file(const std::string& filename,
                                const std::map<std::string, std::string>& data,
                                const std::set<std::string>& tombstones,
                                std::vector<IndexEntry>& out_index);

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
        // previously caused OOM kills. On a key present in both inputs,
        // the newer_path version wins; a winning tombstone is dropped
        // entirely (never written to the output) rather than carried
        // forward -- only correct when nothing older than older_path
        // survives, which the caller (StorageEngine's compactor) must
        // guarantee. On success, out_index receives the index built while
        // writing, same convention as write_file.
        static bool merge_files(const std::string& older_path, const std::string& newer_path,
                                 const std::string& output_path, std::vector<IndexEntry>& out_index);

        // Binary searches the (already sorted) `index` for `key`. On a
        // match, seeks straight to its byte offset and reads that one
        // record -- no scanning. If `key` isn't in `index`, it isn't in the
        // file; there is no scan fallback.
        static SearchResult search_with_index(const std::string& filename,
                                               const std::vector<IndexEntry>& index,
                                               const std::string& key);
    };
}
