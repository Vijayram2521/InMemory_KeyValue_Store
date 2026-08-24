#include "engine/storage_engine.h"
#include <map>
#include <mutex>
#include <shared_mutex>
#include <iostream>
#include "engine/wal.h"
#include "engine/manifest.h"
#include <iomanip>
#include <filesystem>
#include <algorithm>
#include "engine/sstable.h"
#include "engine/bloom_filter.h"
#include <set>
#include <unordered_map>
#include <chrono>
#include <condition_variable>
#include <thread>

namespace kv_engine {

std::string format_seq(uint64_t seq) {
    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << seq;
    return oss.str();
}

// Inverse of format_seq: recovers the sequence number a live SSTable path
// was assigned, by parsing the zero-padded basename back to an integer.
// Needed because sstable_files only stores paths -- compaction's Manifest
// rewrite needs each survivor's sequence number to build a fresh
// ManifestEntry list, and nothing else keeps that number around once a
// generation is loaded.
uint64_t parse_seq_from_path(const std::string& path) {
    return std::stoull(std::filesystem::path(path).stem().string());
}

// Without this gate, "always merge the two oldest" makes the oldest
// surviving file an ever-growing accumulator that gets fully re-read and
// re-written on EVERY subsequent pass just to fold in one more
// freshly-flushed generation -- observed in practice under sustained write
// load to reach multiple GB and tens of GB of cumulative compaction I/O,
// far more than the logical dataset size. Once the older file already
// dwarfs the newer one, merging them again mostly just re-copies bytes
// that were already merged last time -- skip that pass rather than pay the
// cost again. This does mean compaction can permanently stop making
// progress on a given pair once the ratio is crossed -- a real limit of
// "always merge oldest two" as a strategy, not something a ratio alone
// fully solves; true size-tiered compaction would let smaller, newer
// generations merge among themselves first instead. Noted as a real
// follow-up, not implemented here -- this bounds the worst case instead.
constexpr double kMaxOlderToNewerSizeRatio = 4.0;

// Builds a Bloom filter sized for `index`'s key count and populated with
// every key in it. Used both for freshly-flushed generations (which already
// have their index in hand) and for generations recovered via the Manifest
// at startup (whose index was just loaded from disk) -- either way, the
// index already lists every key, so this never needs its own disk read.
BloomFilter build_bloom_filter(const std::vector<IndexEntry>& index) {
    BloomFilter bloom(index.size());
    for (const auto& entry : index) {
        bloom.add(entry.key);
    }
    return bloom;
}

struct StorageEngine::Impl {
    std::map<std::string, std::string> memtable;
    std::shared_mutex rw_lock;
    std::unique_ptr<WAL> wal;
    uint64_t current_seq;
    std::string data_dir;
    std::vector<std::string> sstable_files;
    std::set<std::string> tombstones;
    size_t THRESHOLD;
    // Every SSTable's index, keyed by its file path. Populated once here at
    // startup (for generations recovered via the Manifest) or immediately
    // on flush() (for generations this process writes itself) -- never
    // lazily inside Get(), because Get() only takes a shared_lock and
    // multiple readers mutating this map concurrently would race. flush()
    // always runs under Put()/Delete()'s unique_lock, so it's safe to
    // mutate here without extra locking.
    std::unordered_map<std::string, std::vector<IndexEntry>> index_cache;
    // Same lifecycle/locking rationale as index_cache above: populated once
    // at startup or flush(), read-only from Get(). One Bloom filter per
    // SSTable lets a lookup rule out "definitely not in this file" without
    // ever opening it -- see bloom_filter.h for how it's built.
    std::unordered_map<std::string, BloomFilter> bloom_cache;

    // Compaction: serializes compact_once() calls (background thread vs. a
    // manual/test call) so two passes never run concurrently -- separate
    // from rw_lock on purpose, since compact_once()'s slow phase (read,
    // merge, write) deliberately holds no lock at all.
    std::mutex compaction_serialize_mutex_;
    std::thread compaction_thread_;
    std::mutex compaction_cv_mutex_;
    std::condition_variable compaction_cv_;
    bool compaction_stop_ = false;

    Impl(const std::string& dir, size_t threshold) : data_dir(dir), THRESHOLD(threshold) {
        std::filesystem::create_directories(data_dir);

        auto history = Manifest::load_history(data_dir);
        for (const auto& entry : history) {
            std::string full_path = (std::filesystem::path(data_dir) / entry.filename).string();
            sstable_files.push_back(full_path);
            auto index = SSTable::load_index(full_path);
            bloom_cache.emplace(full_path, build_bloom_filter(index));
            index_cache[full_path] = std::move(index);
        }
        // Deliberately the MAX sequence across all entries, not the last
        // line's: once compaction can rewrite the Manifest with a
        // high-sequence entry positioned earlier in the file (it's
        // logically the oldest surviving generation, even though its
        // number is high), "last line" and "highest sequence ever used"
        // are no longer the same thing -- using the wrong one here would
        // let a future flush reuse an already-used sequence number and
        // silently overwrite a compacted file.
        current_seq = 0;
        for (const auto& entry : history) {
            current_seq = std::max(current_seq, entry.sequence + 1);
        }

        std::string wal_path = get_wal_path(current_seq);
        wal = std::make_unique<WAL>(wal_path);
        wal->recover(memtable, tombstones);
    }

    std::string get_wal_path(uint64_t seq) {
        return (std::filesystem::path(data_dir) / (format_seq(seq) + ".log")).string();
    }
    std::string get_sst_path(uint64_t seq) {
        return (std::filesystem::path(data_dir) / (format_seq(seq) + ".sst")).string();
    }

    // --- NEW DEDICATED FLUSH METHOD ---
    void flush() {
        // Check both: we might have an empty memtable but pending tombstones
        if (memtable.empty() && tombstones.empty()) return;

        std::string sst_path = get_sst_path(current_seq);
        std::cout << "--- Persisting Generation " << current_seq << " to Disk ---" << std::endl;

        // 1. Write the SSTable (data + index + footer) using both the map
        // and the tombstone set, capturing the index built while writing so
        // we can cache it directly without re-reading the file.
        std::vector<IndexEntry> index;
        if (SSTable::write_file(sst_path, memtable, tombstones, index)) {
            bloom_cache.emplace(sst_path, build_bloom_filter(index));
            index_cache[sst_path] = std::move(index);

            // 2. Register the new file in the Manifest
            // This persists the filename and the current "timestamp"
            Manifest::add_entry(data_dir, format_seq(current_seq) + ".sst", current_seq);

            // 3. Prepare for the next generation
            uint64_t next_seq = current_seq + 1;
            std::string next_wal_path = get_wal_path(next_seq);
            
            // Rotate WAL: Old logs are now redundant because data is in the .sst
            wal = std::make_unique<WAL>(next_wal_path);
            
            // 4. Reset in-memory state
            memtable.clear();
            tombstones.clear(); 
            current_seq = next_seq;

            // 5. Update the live file list for "First Hit" searching. A
            // freshly flushed generation is always the newest by
            // construction, so it belongs at the end -- no sort needed.
            // (Recency is tracked purely by vector position, not by
            // filename/sequence value, so that compaction's merged files
            // -- which get a fresh, unrelated sequence number but must
            // stay logically oldest -- can't ever get scrambled out of
            // place by re-sorting.)
            sstable_files.push_back(sst_path);

            std::cout << "--- Flush Complete. Generation is now: " << current_seq << " ---" << std::endl;
        }
    }

    // Merges the two globally-oldest surviving SSTable generations into
    // one, if at least 3 flushed generations exist (2 to merge, 1 newest
    // left untouched). Returns false if there's nothing eligible.
    //
    // The slow part -- reading both files, merging, writing the new one --
    // holds NO lock, since SSTables are immutable once written and reading
    // one is safe to interleave with anything. Only the brief metadata
    // swap at the end takes rw_lock's unique_lock, same as a normal flush.
    bool compact_once() {
        std::lock_guard<std::mutex> serialize(compaction_serialize_mutex_);

        std::string path_a, path_b;
        {
            std::shared_lock lock(rw_lock);
            if (sstable_files.size() < 3) return false;
            path_a = sstable_files[0]; // older
            path_b = sstable_files[1]; // newer
        }

        // Size-ratio gate -- see kMaxOlderToNewerSizeRatio's comment above.
        // Cheap (just two stat() calls), so fine to redo every pass even
        // though the answer won't change until something else merges.
        {
            std::error_code ec_a, ec_b;
            uint64_t size_a = std::filesystem::file_size(path_a, ec_a);
            uint64_t size_b = std::filesystem::file_size(path_b, ec_b);
            if (!ec_a && !ec_b && size_b > 0 &&
                static_cast<double>(size_a) > kMaxOlderToNewerSizeRatio * static_cast<double>(size_b)) {
                return false;
            }
        }

        // --- Slow phase: no lock held ---
        // Streams one record at a time from each input file rather than
        // loading either fully into memory (peak memory here is O(1) per
        // input file, not O(file size)) -- the original load-both-files-
        // fully approach genuinely OOM-killed memory-constrained deployments
        // (observed directly: 1.2GB-capped compute node containers killed
        // by the kernel cgroup OOM killer while holding two full files'
        // records plus a merged copy simultaneously).
        std::string tmp_path = (std::filesystem::path(data_dir) / "compact.tmp").string();
        std::vector<IndexEntry> merged_index;
        if (!SSTable::merge_files(path_a, path_b, tmp_path, merged_index)) {
            // Couldn't write the merged file -- abort without touching any
            // live state. Old generations are untouched; try again on the
            // next pass.
            return false;
        }
        bool wrote_file = !merged_index.empty();
        if (!wrote_file) {
            // Both generations fully cancelled out (every key ended up
            // deleted or shadowed) -- discard the empty temp file rather
            // than registering a pointless empty generation; the pair just
            // vanishes with no replacement.
            std::filesystem::remove(tmp_path);
        }

        // --- Swap phase: brief unique_lock for in-memory + Manifest only ---
        std::unique_lock lock(rw_lock);

        std::string final_path;
        if (wrote_file) {
            uint64_t merged_seq = current_seq++;
            final_path = get_sst_path(merged_seq);
            // Fresh, never-before-used sequence number -> this rename can
            // never collide with or overwrite any live file.
            std::filesystem::rename(tmp_path, final_path);
            bloom_cache.emplace(final_path, build_bloom_filter(merged_index));
            index_cache[final_path] = std::move(merged_index);
        }

        sstable_files.erase(sstable_files.begin(), sstable_files.begin() + 2);
        index_cache.erase(path_a);
        index_cache.erase(path_b);
        bloom_cache.erase(path_a);
        bloom_cache.erase(path_b);
        if (wrote_file) {
            // Still logically the oldest survivor -- position, not the
            // (fresh, numerically large) sequence number, is what matters.
            sstable_files.insert(sstable_files.begin(), final_path);
        }

        // Durable commit point: rewrite the whole Manifest to match the
        // just-updated sstable_files exactly, in order. Must happen AFTER
        // the rename above -- if it happened first and we crashed before
        // the rename, the Manifest would reference a file that doesn't
        // physically exist yet, losing both source generations' data.
        // Doing the rename first means a crash before this point just
        // leaves a harmless orphaned temp file.
        std::vector<ManifestEntry> entries;
        entries.reserve(sstable_files.size());
        for (const auto& p : sstable_files) {
            entries.push_back({std::filesystem::path(p).filename().string(), parse_seq_from_path(p)});
        }
        Manifest::rewrite(data_dir, entries);

        lock.unlock();

        // Physically delete the two old files now that the Manifest no
        // longer references them. Safe outside the lock and after
        // everything above -- nothing reads them anymore; a crash before
        // this point just leaves harmless orphaned files on disk, never
        // data loss.
        std::filesystem::remove(path_a);
        std::filesystem::remove(path_b);

        return true;
    }

    void compaction_loop(std::chrono::seconds poll_interval) {
        std::unique_lock<std::mutex> lock(compaction_cv_mutex_);
        while (!compaction_cv_.wait_for(lock, poll_interval, [this] { return compaction_stop_; })) {
            lock.unlock();
            while (compact_once()) {
                // Keep compacting everything currently eligible before
                // sleeping again, rather than lagging behind at one merge
                // per poll_interval.
            }
            lock.lock();
        }
    }

    void start_background_compaction(std::chrono::seconds poll_interval) {
        if (compaction_thread_.joinable()) return; // already running
        compaction_stop_ = false;
        compaction_thread_ = std::thread([this, poll_interval] { compaction_loop(poll_interval); });
    }

    void stop_background_compaction() {
        if (!compaction_thread_.joinable()) return;
        {
            std::lock_guard<std::mutex> lock(compaction_cv_mutex_);
            compaction_stop_ = true;
        }
        compaction_cv_.notify_all();
        compaction_thread_.join();
    }

    ~Impl() {
        stop_background_compaction();
    }
};

StorageEngine::StorageEngine(const std::string& data_dir, size_t memtable_threshold)
    : pImpl(std::make_unique<Impl>(data_dir, memtable_threshold)) {
    std::cout << "Engine initialized at: " << data_dir 
              << " | Active Sequence: " << pImpl->current_seq << std::endl;
}

StorageEngine::~StorageEngine() = default;

bool StorageEngine::Put(const std::string& key, const std::string& value) {
    std::unique_lock lock(pImpl->rw_lock);
    
    if (!pImpl->wal->append(LogOp::PUT,pImpl->current_seq, key, value)) {
        return false; 
    }
    
    pImpl->memtable[key] = value;
    pImpl->tombstones.erase(key); // A fresh PUT revives a previously deleted key

    // Trigger flush if threshold reached
    if (pImpl->memtable.size() >= pImpl->THRESHOLD) {
        pImpl->flush();
    }

    return true;
}

// --- NEW PUBLIC FORCE FLUSH ---
void StorageEngine::ForceFlush() {
    std::unique_lock lock(pImpl->rw_lock);
    pImpl->flush();
}

std::optional<std::string> StorageEngine::Get(const std::string& key) {
    std::shared_lock lock(pImpl->rw_lock);

    if (pImpl->tombstones.count(key)) {
        return std::nullopt; // It's dead, don't look further
    }

    auto it = pImpl->memtable.find(key);
    if (it != pImpl->memtable.end()) {
        return it->second;
    }

    // Search SSTables newest to oldest. The Bloom filter check first can
    // only ever save work (a definite "not here" skips this file without
    // opening it); if it says "maybe," fall through to the same binary
    // search over the cached index plus a single seek+read on a hit -- no
    // linear scan of the file either way.
    for (auto rit = pImpl->sstable_files.rbegin(); rit != pImpl->sstable_files.rend(); ++rit) {
        auto bloom_it = pImpl->bloom_cache.find(*rit);
        if (bloom_it != pImpl->bloom_cache.end() && !bloom_it->second.maybe_contains(key)) {
            continue; // definitely not in this file
        }
        auto cache_it = pImpl->index_cache.find(*rit);
        if (cache_it == pImpl->index_cache.end()) continue; // shouldn't happen; defensive
        auto result = SSTable::search_with_index(*rit, cache_it->second, key);
        if (result.found) {
            if (result.is_tombstone) return std::nullopt;
            return result.value;
        }
    }
    return std::nullopt;
}

bool StorageEngine::Delete(const std::string& key) {
    std::unique_lock lock(pImpl->rw_lock);

    // 1. Log the DELETE with the current sequence
    if (!pImpl->wal->append(LogOp::DELETE, pImpl->current_seq, key, "")) {
        return false;
    }

    // 2. Update RAM state
    pImpl->memtable.erase(key);
    pImpl->tombstones.insert(key);

    // 3. Optional: Trigger flush if tombstone count + memtable size is too high
    if ((pImpl->memtable.size() + pImpl->tombstones.size()) >= pImpl->THRESHOLD) {
        pImpl->flush();
    }

    return true;
}

bool StorageEngine::CompactOnce() {
    return pImpl->compact_once();
}

void StorageEngine::StartBackgroundCompaction(std::chrono::seconds poll_interval) {
    pImpl->start_background_compaction(poll_interval);
}

void StorageEngine::StopBackgroundCompaction() {
    pImpl->stop_background_compaction();
}

} // namespace kv_engine