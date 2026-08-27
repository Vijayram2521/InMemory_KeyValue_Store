#include "engine/storage_engine.h"
#include <map>
#include <mutex>
#include <shared_mutex>
#include <iostream>
#include "engine/wal.h"
#include "engine/manifest.h"
#include "engine/index_checkpoint.h"
#include <iomanip>
#include <filesystem>
#include <algorithm>
#include "engine/sstable.h"
#include <unordered_map>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <functional>

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

struct StorageEngine::Impl {
    std::map<std::string, std::string> memtable;
    std::shared_mutex rw_lock;
    std::unique_ptr<WAL> wal;
    uint64_t current_seq;
    std::string data_dir;
    std::vector<std::string> sstable_files;
    size_t THRESHOLD;
    // Every SSTable's index, keyed by its file path. Populated once here at
    // startup (for generations recovered via the Manifest) or immediately
    // on flush() (for generations this process writes itself) -- never
    // lazily inside Get(), because Get() only takes a shared_lock and
    // multiple readers mutating this map concurrently would race. flush()
    // always runs under Put()/Delete()'s unique_lock, so it's safe to
    // mutate here without extra locking.
    std::unordered_map<std::string, std::vector<IndexEntry>> index_cache;
    // Same lifecycle/locking rationale as index_cache above: one del-bitmap
    // per SSTable, mirroring the on-disk .del file, mutated in place (both
    // here and on disk) whenever a record in that file is superseded or
    // deleted -- see find_live_location/mark_dead below. This is what lets
    // "dead" be known eagerly at write time rather than discovered lazily
    // at merge time.
    std::unordered_map<std::string, std::vector<uint8_t>> delcol_cache;

    // Experimental (see StorageEngine's use_mmap_reads doc comment). Only
    // ever populated when USE_MMAP is true -- left empty and untouched
    // otherwise, so the default (unchanged) code path pays zero extra cost.
    const bool USE_MMAP;
    std::unordered_map<std::string, MappedFile> mmap_cache;

    // Compaction: serializes compact_once() calls (background thread vs. a
    // manual/test call) so two passes never run concurrently -- separate
    // from rw_lock on purpose, since compact_once()'s slow phase (read,
    // merge, write) deliberately holds no lock at all.
    std::mutex compaction_serialize_mutex_;
    std::thread compaction_thread_;
    std::mutex compaction_cv_mutex_;
    std::condition_variable compaction_cv_;
    bool compaction_stop_ = false;

    // Global key -> current on-disk location index, sharded into
    // kNumShards independent maps (see index_checkpoint.h) purely to keep
    // individual checkpoint files and rehash events smaller -- everything
    // here still runs under the single rw_lock above, same as every other
    // cache in this struct. Exists only ever accelerate find_live_location
    // and Get() (see below); the per-file index_cache/delcol_cache above
    // remain ground truth, so a missing or stale shard entry degrades to
    // "fall back to the pre-existing scan," never a wrong answer.
    ShardedIndex global_key_index;

    static size_t shard_for(const std::string& key) {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    // Checkpointing: periodically snapshots global_key_index to disk (see
    // IndexCheckpoint) so a future restart doesn't have to rebuild it from
    // every live SSTable's index from scratch. Separate thread/mutex/cv
    // from compaction's, since checkpointing's "run every fixed interval"
    // shape doesn't map onto compact_once()'s "run until nothing eligible"
    // inner loop.
    std::mutex checkpoint_serialize_mutex_;
    std::thread checkpoint_thread_;
    std::mutex checkpoint_cv_mutex_;
    std::condition_variable checkpoint_cv_;
    bool checkpoint_stop_ = false;

    Impl(const std::string& dir, size_t threshold, bool use_mmap)
        : data_dir(dir), THRESHOLD(threshold), USE_MMAP(use_mmap) {
        std::filesystem::create_directories(data_dir);

        auto history = Manifest::load_history(data_dir);
        for (const auto& entry : history) {
            std::string full_path = (std::filesystem::path(data_dir) / entry.filename).string();
            sstable_files.push_back(full_path);
            auto index = SSTable::load_index(full_path);
            if (USE_MMAP) mmap_cache.emplace(full_path, MappedFile(full_path));
            delcol_cache[full_path] = SSTable::load_del_bitmap(SSTable::del_path_for(full_path));
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

        // Global key index: load whatever checkpoint exists (possibly
        // none), then fold in every live generation with sequence >= the
        // checkpoint's covered_seq, oldest to newest, so a later
        // generation's copy of a key correctly overwrites an earlier one.
        // covered_seq defaults to 0 (fold in everything) when no
        // checkpoint is found -- same loop either way, just a different
        // starting point. No separate reconciliation pass is needed for
        // keys that died without a new generation revealing it --
        // find_live_location's self-heal-on-lookup rule covers that
        // lazily instead (see its comment below).
        uint64_t covered_seq = 0;
        if (auto loaded = IndexCheckpoint::load_latest(data_dir)) {
            global_key_index = std::move(loaded->shards);
            covered_seq = loaded->covered_seq;
        }
        // `history` is already oldest-to-newest (Manifest::load_history
        // preserves insertion order, and compact_once()'s rewrite always
        // keeps the merged/oldest-surviving entry positioned first), so
        // iterating it directly here means a later generation's entry for
        // a given key naturally overwrites an earlier one below.
        for (const auto& entry : history) {
            if (entry.sequence < covered_seq) continue;
            std::string full_path = (std::filesystem::path(data_dir) / entry.filename).string();
            auto idx_it = index_cache.find(full_path);
            if (idx_it == index_cache.end()) continue;
            auto del_it = delcol_cache.find(full_path);
            const auto& del_bits = (del_it != delcol_cache.end()) ? del_it->second : std::vector<uint8_t>{};
            uint32_t file_seq = static_cast<uint32_t>(entry.sequence);
            for (const auto& e : idx_it->second) {
                if (!SSTable::is_dead(del_bits, e.serial)) {
                    global_key_index[shard_for(e.key)][e.key] = Location{file_seq, static_cast<uint32_t>(e.serial)};
                }
            }
        }

        std::string wal_path = get_wal_path(current_seq);
        wal = std::make_unique<WAL>(wal_path);

        // Replay: redo each PUT/DELETE through the SAME apply_put/
        // apply_delete logic live Put()/Delete() use below, so recovery
        // correctly re-runs eager dead-marking (find the key's prior
        // on-disk location and flip its bit) rather than just repopulating
        // the memtable. A dead-bit flip that didn't make it to disk before
        // a crash is exactly what this replay exists to redo -- sstable
        // state is already loaded above, so find_live_location has
        // everything it needs.
        wal->read_all([this](LogOp op, uint64_t /*seq*/, const std::string& key, const std::string& value) {
            if (op == LogOp::PUT) apply_put(key, value);
            else if (op == LogOp::DELETE) apply_delete(key);
        });
    }

    std::string get_wal_path(uint64_t seq) {
        return (std::filesystem::path(data_dir) / (format_seq(seq) + ".log")).string();
    }
    std::string get_sst_path(uint64_t seq) {
        return (std::filesystem::path(data_dir) / (format_seq(seq) + ".sst")).string();
    }

    // Looks up `key` in one specific SSTable file (mmap path if enabled and
    // valid, ifstream path otherwise -- exactly Get()'s existing
    // per-file branching, factored out so both Get()'s global_key_index
    // fast path and its per-generation slow-path scan share one
    // implementation instead of duplicating the mmap/ifstream branch).
    // Read-only; safe under either a shared_lock or unique_lock.
    SearchResult search_one_file(const std::string& file, const std::string& key) const {
        auto cache_it = index_cache.find(file);
        if (cache_it == index_cache.end()) return {};
        if (USE_MMAP) {
            auto mmap_it = mmap_cache.find(file);
            if (mmap_it != mmap_cache.end() && mmap_it->second.valid()) {
                return SSTable::search_with_index_mmap(mmap_it->second, cache_it->second, key);
            }
        }
        return SSTable::search_with_index(file, cache_it->second, key);
    }

    // Locates `key`'s current on-disk position, if any. Fast path: an O(1)
    // lookup in global_key_index's shard for this key, verified against
    // that file's del-bitmap before being trusted (self-healing: a hit
    // that turns out to be dead means a stale checkpoint-loaded entry from
    // before a delete/overwrite that never got a new generation to reveal
    // it -- see the Design notes on checkpoint staleness -- erase it and
    // fall through). Slow path (genuinely new key, or the crash-recovery
    // gap just described): scan every live SSTable, newest to oldest,
    // exactly as before this index existed -- this is ground truth; the
    // fast path is only ever an accelerator over it, never authoritative
    // on its own. Callers must hold the unique_lock (mutates
    // global_key_index on the self-heal path).
    std::optional<Location> find_live_location(const std::string& key) {
        auto& shard = global_key_index[shard_for(key)];
        auto gi_it = shard.find(key);
        if (gi_it != shard.end()) {
            std::string file = get_sst_path(gi_it->second.file_seq);
            auto del_it = delcol_cache.find(file);
            bool dead = del_it != delcol_cache.end() && SSTable::is_dead(del_it->second, gi_it->second.serial);
            if (!dead) return gi_it->second;
            shard.erase(gi_it);
        }

        for (auto rit = sstable_files.rbegin(); rit != sstable_files.rend(); ++rit) {
            auto cache_it = index_cache.find(*rit);
            if (cache_it == index_cache.end()) continue;
            auto it = std::lower_bound(cache_it->second.begin(), cache_it->second.end(), key,
                [](const IndexEntry& e, const std::string& k) { return e.key < k; });
            if (it != cache_it->second.end() && it->key == key) {
                auto del_it = delcol_cache.find(*rit);
                bool dead = del_it != delcol_cache.end() && SSTable::is_dead(del_it->second, it->serial);
                if (!dead) return Location{static_cast<uint32_t>(parse_seq_from_path(*rit)),
                                            static_cast<uint32_t>(it->serial)};
            }
        }
        return std::nullopt;
    }

    // Marks `loc` dead: flips its del-bitmap bit (in-memory + on-disk) and
    // removes `key` from global_key_index, since the index only ever
    // tracks currently-live locations. Callers must hold the unique_lock
    // (all callers here are apply_put/apply_delete, which already require
    // it).
    void mark_dead(const std::string& key, const Location& loc) {
        std::string file = get_sst_path(loc.file_seq);
        auto del_it = delcol_cache.find(file);
        if (del_it != delcol_cache.end()) {
            SSTable::mark_dead_in_memory(del_it->second, loc.serial);
        }
        SSTable::flip_dead_on_disk(SSTable::del_path_for(file), loc.serial);
        global_key_index[shard_for(key)].erase(key);
    }

    // Core Put/Delete logic, shared between live calls and WAL replay
    // during recovery -- callers are responsible for the WAL append (live
    // calls do it before calling these; replay already has a durable WAL
    // entry it's redoing). Requires the unique_lock (held by Put/Delete,
    // or implicitly single-threaded during construction).
    void apply_put(const std::string& key, const std::string& value) {
        auto loc = find_live_location(key);
        if (loc) mark_dead(key, *loc);
        memtable[key] = value;
    }

    void apply_delete(const std::string& key) {
        memtable.erase(key);
        auto loc = find_live_location(key);
        if (loc) mark_dead(key, *loc);
    }

    void flush() {
        if (memtable.empty()) return;

        std::string sst_path = get_sst_path(current_seq);
        std::cout << "--- Persisting Generation " << current_seq << " to Disk ---" << std::endl;

        std::vector<IndexEntry> index;
        if (SSTable::write_file(sst_path, memtable, index)) {
            if (USE_MMAP) mmap_cache.emplace(sst_path, MappedFile(sst_path));

            uint32_t this_file_seq = static_cast<uint32_t>(current_seq);
            for (const auto& entry : index) {
                global_key_index[shard_for(entry.key)][entry.key] =
                    Location{this_file_seq, static_cast<uint32_t>(entry.serial)};
            }
            index_cache[sst_path] = std::move(index);

            // Del-bitmap sized to the flush threshold (the "flush key
            // limit"), not the actual record count -- most bits stay
            // unused if fewer than THRESHOLD keys were flushed (e.g. via
            // ForceFlush), which is harmless.
            auto del_bits = SSTable::make_del_bitmap(THRESHOLD);
            SSTable::save_del_bitmap(SSTable::del_path_for(sst_path), del_bits);
            delcol_cache[sst_path] = std::move(del_bits);

            Manifest::add_entry(data_dir, format_seq(current_seq) + ".sst", current_seq);

            uint64_t next_seq = current_seq + 1;
            std::string next_wal_path = get_wal_path(next_seq);
            wal = std::make_unique<WAL>(next_wal_path);

            memtable.clear();
            current_seq = next_seq;

            // A freshly flushed generation is always the newest by
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
        std::vector<uint8_t> del_a_snapshot, del_b_snapshot;
        {
            std::shared_lock lock(rw_lock);
            if (sstable_files.size() < 3) return false;
            path_a = sstable_files[0]; // older
            path_b = sstable_files[1]; // newer
            auto it_a = delcol_cache.find(path_a);
            auto it_b = delcol_cache.find(path_b);
            del_a_snapshot = (it_a != delcol_cache.end()) ? it_a->second : std::vector<uint8_t>{};
            del_b_snapshot = (it_b != delcol_cache.end()) ? it_b->second : std::vector<uint8_t>{};
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
        // input file, not O(file size)). Uses the del-bitmap SNAPSHOTS
        // taken above, not the live cache -- see the staleness check below
        // for why that matters.
        std::string tmp_path = (std::filesystem::path(data_dir) / "compact.tmp").string();
        std::vector<IndexEntry> merged_index;
        std::vector<uint8_t> merged_del;
        if (!SSTable::merge_files(path_a, del_a_snapshot, path_b, del_b_snapshot,
                                   tmp_path, merged_index, merged_del)) {
            // Couldn't write the merged file -- abort without touching any
            // live state. Old generations are untouched; try again on the
            // next pass.
            return false;
        }
        bool wrote_file = !merged_index.empty();

        // --- Swap phase: brief unique_lock for in-memory + Manifest only ---
        std::unique_lock lock(rw_lock);

        // A concurrent Put/Delete may have marked a record in path_a or
        // path_b dead WHILE the slow merge above was working from a
        // snapshot of their del-bitmaps taken before it started -- if so,
        // the merge may have copied forward a record that's actually dead
        // now, which would silently resurrect a stale value if committed.
        // Detect this by comparing the live del-bitmaps against the
        // snapshot; if either changed, discard this merge's result and
        // retry from scratch on the next pass rather than risk committing
        // a merge computed from stale liveness data.
        auto still_matches = [this](const std::string& path, const std::vector<uint8_t>& snapshot) {
            auto it = delcol_cache.find(path);
            const std::vector<uint8_t>& live = (it != delcol_cache.end()) ? it->second : std::vector<uint8_t>{};
            return live == snapshot;
        };
        if (!still_matches(path_a, del_a_snapshot) || !still_matches(path_b, del_b_snapshot)) {
            lock.unlock();
            if (wrote_file) std::filesystem::remove(tmp_path);
            return false;
        }

        if (!wrote_file) {
            // Both generations fully cancelled out (every key ended up
            // deleted or shadowed) -- discard the empty temp file rather
            // than registering a pointless empty generation; the pair just
            // vanishes with no replacement.
            std::filesystem::remove(tmp_path);
        }

        std::string final_path;
        if (wrote_file) {
            uint64_t merged_seq = current_seq++;
            final_path = get_sst_path(merged_seq);
            // Fresh, never-before-used sequence number -> this rename can
            // never collide with or overwrite any live file.
            std::filesystem::rename(tmp_path, final_path);
            if (USE_MMAP) mmap_cache.emplace(final_path, MappedFile(final_path));
            SSTable::save_del_bitmap(SSTable::del_path_for(final_path), merged_del);
            delcol_cache[final_path] = std::move(merged_del);

            // Every surviving record gets a FRESH serial in the merged
            // file, so (unlike Put/Delete) this can't rely on
            // global_key_index already being correct for these keys --
            // each one needs its entry rewritten to point at the new
            // file+serial directly. Entries for the merged-away files' now
            // fully-dead keys need no cleanup here: they were already
            // erased when they died, via mark_dead, potentially
            // generations before this compaction pass ever ran.
            uint32_t merged_file_seq = static_cast<uint32_t>(merged_seq);
            for (const auto& entry : merged_index) {
                global_key_index[shard_for(entry.key)][entry.key] =
                    Location{merged_file_seq, static_cast<uint32_t>(entry.serial)};
            }
            index_cache[final_path] = std::move(merged_index);
        }

        sstable_files.erase(sstable_files.begin(), sstable_files.begin() + 2);
        index_cache.erase(path_a);
        index_cache.erase(path_b);
        delcol_cache.erase(path_a);
        delcol_cache.erase(path_b);
        mmap_cache.erase(path_a);
        mmap_cache.erase(path_b);
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
        std::filesystem::remove(SSTable::del_path_for(path_a));
        std::filesystem::remove(SSTable::del_path_for(path_b));

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

    // Snapshots global_key_index to disk. Copies it under a brief
    // shared_lock (consistent with how compact_once() snapshots
    // del-bitmaps before its own slow phase), then does the actual disk
    // write outside any lock, so a large index doesn't hold up concurrent
    // Put/Delete/Get for the duration of the write -- only for the copy.
    bool checkpoint_index_once() {
        std::lock_guard<std::mutex> serialize(checkpoint_serialize_mutex_);

        ShardedIndex snapshot;
        uint64_t seq_marker;
        {
            std::shared_lock lock(rw_lock);
            snapshot = global_key_index;
            seq_marker = current_seq;
        }
        IndexCheckpoint::write(data_dir, seq_marker, snapshot);
        return true;
    }

    void checkpoint_loop(std::chrono::seconds interval) {
        std::unique_lock<std::mutex> lock(checkpoint_cv_mutex_);
        while (!checkpoint_cv_.wait_for(lock, interval, [this] { return checkpoint_stop_; })) {
            lock.unlock();
            checkpoint_index_once();
            lock.lock();
        }
    }

    void start_index_checkpointing(std::chrono::seconds interval) {
        if (checkpoint_thread_.joinable()) return; // already running
        checkpoint_stop_ = false;
        checkpoint_thread_ = std::thread([this, interval] { checkpoint_loop(interval); });
    }

    void stop_index_checkpointing() {
        if (!checkpoint_thread_.joinable()) return;
        {
            std::lock_guard<std::mutex> lock(checkpoint_cv_mutex_);
            checkpoint_stop_ = true;
        }
        checkpoint_cv_.notify_all();
        checkpoint_thread_.join();
    }

    ~Impl() {
        stop_background_compaction();
        stop_index_checkpointing();
    }
};

StorageEngine::StorageEngine(const std::string& data_dir, size_t memtable_threshold, bool use_mmap_reads)
    : pImpl(std::make_unique<Impl>(data_dir, memtable_threshold, use_mmap_reads)) {
    std::cout << "Engine initialized at: " << data_dir
              << " | Active Sequence: " << pImpl->current_seq << std::endl;
}

StorageEngine::~StorageEngine() = default;

bool StorageEngine::Put(const std::string& key, const std::string& value) {
    std::unique_lock lock(pImpl->rw_lock);

    if (!pImpl->wal->append(LogOp::PUT, pImpl->current_seq, key, value)) {
        return false;
    }

    pImpl->apply_put(key, value);

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

    auto it = pImpl->memtable.find(key);
    if (it != pImpl->memtable.end()) {
        return it->second;
    }

    // Fast path: global_key_index gives an O(1) candidate location instead
    // of scanning every live generation newest-to-oldest. A dead candidate
    // falls through to the slow path below rather than being trusted --
    // Get() only holds a shared_lock, so unlike find_live_location it can't
    // self-heal a stale entry itself (that happens lazily, the next time a
    // Put/Delete touches this key, under the unique_lock).
    auto& shard = pImpl->global_key_index[Impl::shard_for(key)];
    auto gi_it = shard.find(key);
    if (gi_it != shard.end()) {
        std::string file = pImpl->get_sst_path(gi_it->second.file_seq);
        auto del_it = pImpl->delcol_cache.find(file);
        bool dead = del_it != pImpl->delcol_cache.end() &&
                    SSTable::is_dead(del_it->second, gi_it->second.serial);
        if (!dead) {
            auto result = pImpl->search_one_file(file, key);
            if (result.found) return result.value;
        }
    }

    // Slow path: scan every live generation newest-to-oldest, exactly as
    // before this optimization existed -- reached for a genuine miss, or
    // the narrow crash-recovery gap where global_key_index hasn't caught
    // up to a newer generation yet. On an index hit, check the file's
    // del-bitmap before trusting the value -- a set bit means this record
    // has since been superseded or deleted, so keep searching older
    // generations exactly as a tombstone used to make this loop do.
    for (auto rit = pImpl->sstable_files.rbegin(); rit != pImpl->sstable_files.rend(); ++rit) {
        auto result = pImpl->search_one_file(*rit, key);
        if (result.found) {
            auto del_it = pImpl->delcol_cache.find(*rit);
            bool dead = del_it != pImpl->delcol_cache.end() && SSTable::is_dead(del_it->second, result.serial);
            if (dead) continue; // superseded/deleted -- try the next (older) generation
            return result.value;
        }
    }
    return std::nullopt;
}

bool StorageEngine::Delete(const std::string& key) {
    std::unique_lock lock(pImpl->rw_lock);

    if (!pImpl->wal->append(LogOp::DELETE, pImpl->current_seq, key, "")) {
        return false;
    }

    pImpl->apply_delete(key);

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

bool StorageEngine::CheckpointIndexOnce() {
    return pImpl->checkpoint_index_once();
}

void StorageEngine::StartIndexCheckpointing(std::chrono::seconds interval) {
    pImpl->start_index_checkpointing(interval);
}

void StorageEngine::StopIndexCheckpointing() {
    pImpl->stop_index_checkpointing();
}

} // namespace kv_engine
