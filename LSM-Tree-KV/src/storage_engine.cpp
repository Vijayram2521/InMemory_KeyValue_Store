#include "engine/storage_engine.h"
#include <map>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <array>
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

// The memtable is sharded independently of the global key index (which
// already shards by hash(key) % kNumShards) -- different structure,
// different access pattern, different right-sized shard count. The index
// covers the full on-disk keyspace (millions of keys); the memtable is
// bounded by memtable_threshold (tens of thousands to a few hundred
// thousand entries at any moment), so far fewer shards are needed before
// returns diminish. A single named constant, same one-line-change
// precedent as kNumShards, in case a different count matters later.
// Deliberately a DIFFERENT name from shard_for/kNumShards (index) so the
// two sharding schemes can never be silently conflated in code.
constexpr size_t kNumMemtableShards = 8;

struct StorageEngine::Impl {
    // --- Lock domains ---
    // Governing rules, load-bearing for deadlock freedom:
    //   1. `files_lock` and any `shard_locks[i]` (index shards) are NEVER
    //      held simultaneously by the same thread, in either order --
    //      every function below acquires one, fully releases it, then (if
    //      needed) acquires the other, rather than nesting them.
    //   2. A `memtable_shard_locks[i]`, when held by Put/Delete, is always
    //      the OUTERMOST lock relative to files_lock/index shard_locks
    //      (apply_put/apply_delete -- invoked while still holding it --
    //      reach files_lock/index shard_locks internally via
    //      find_live_location/mark_dead). Nothing ever acquires
    //      files_lock/index shard_locks first and then tries to acquire a
    //      memtable shard lock while still holding them.
    //   3. Put/Delete NEVER hold their own memtable shard lock while
    //      calling flush() -- they fully release it first, check the
    //      total size across shards (one shard's lock at a time, shared,
    //      never two held simultaneously -- summing while holding a
    //      different shard exclusively would be a lock-ordering deadlock
    //      risk against another thread doing the same in the opposite
    //      shard order), and only then, holding nothing, call flush().
    //      flush() itself acquires every memtable shard lock together, in
    //      the fixed order 0..N-1 -- the only place that ever happens, so
    //      no other code path's ordering can conflict with it.
    //   4. `wal_lock` (guards only the `wal` pointer itself, not WAL's
    //      internals, which already have their own mutex for the actual
    //      write) is always acquired AFTER any memtable shard lock a
    //      thread holds, never before -- Put/Delete take it after their
    //      own shard's mutation; flush() takes it last, after every
    //      memtable shard lock, when rotating to a new WAL segment.
    //
    // flush() keeps holding every memtable shard lock for its entire body,
    // including the disk write, rather than releasing them partway through
    // -- see flush()'s own comment for why that's not just an optimization
    // left on the table.
    std::array<std::shared_mutex, kNumMemtableShards> memtable_shard_locks;
    std::array<std::unordered_map<std::string, std::string>, kNumMemtableShards> memtable_shards;
    std::shared_mutex wal_lock;
    std::unique_ptr<WAL> wal;
    // Serializes flush() calls so concurrent Puts that all independently
    // observed "over threshold" don't all try to flush at once -- only the
    // first actually proceeds; the rest, once they get this, find every
    // shard already empty and return immediately. Mirrors
    // compaction_serialize_mutex_'s role for compact_once().
    std::mutex flush_serialize_mutex_;

    static size_t memtable_shard_for(const std::string& key) {
        return std::hash<std::string>{}(key) % kNumMemtableShards;
    }

    // Sequence numbers are claimed via fetch_add wherever a flush or
    // compaction pass needs one -- atomicity alone guarantees no two
    // passes ever claim the same number, with no need to coordinate with
    // any memtable shard lock or files_lock just to keep a counter
    // consistent.
    std::atomic<uint64_t> current_seq{0};

    std::string data_dir;
    size_t THRESHOLD;

    // files_lock guards every SSTable-level cache: index_cache,
    // delcol_cache (bitmap CONTENTS included -- flipping a bit is a
    // read-modify-write on a shared byte, which needs exclusive access
    // just like inserting into the map does), mmap_cache, and
    // sstable_files.
    std::shared_mutex files_lock;
    std::vector<std::string> sstable_files;
    std::unordered_map<std::string, std::vector<IndexEntry>> index_cache;
    std::unordered_map<std::string, std::vector<uint8_t>> delcol_cache;
    const bool USE_MMAP;
    std::unordered_map<std::string, MappedFile> mmap_cache;
    // Opt-in, write-side only -- see StorageEngine's use_value_compression
    // doc comment. Read paths never consult this; every record
    // self-describes whether it was compressed.
    const bool USE_COMPRESSION;

    // Compaction: serializes compact_once() calls (background thread vs. a
    // manual/test call) so two passes never run concurrently -- separate
    // from files_lock on purpose, since compact_once()'s slow phase (read,
    // merge, write) deliberately holds no lock at all.
    std::mutex compaction_serialize_mutex_;
    std::thread compaction_thread_;
    std::mutex compaction_cv_mutex_;
    std::condition_variable compaction_cv_;
    bool compaction_stop_ = false;

    // Global key -> current on-disk location index, sharded into
    // kNumShards independent maps (see index_checkpoint.h), each with its
    // own lock -- this is what turns different-shard Puts/Deletes/Gets
    // into genuinely independent operations instead of all serializing on
    // one structure. Exists only to accelerate find_live_location and
    // Get() (see below); the per-file index_cache/delcol_cache above
    // remain ground truth, so a missing or stale shard entry degrades to
    // "fall back to the pre-existing scan," never a wrong answer.
    ShardedIndex global_key_index;
    std::array<std::shared_mutex, kNumShards> shard_locks;

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

    // Constructor runs single-threaded (nothing else can reach this object
    // yet, since StorageEngine's constructor hasn't returned), so it reads
    // and mutates every field below directly with no locking at all --
    // correct in the same way none of this needed rw_lock before either.
    Impl(const std::string& dir, size_t threshold, bool use_mmap, bool use_compression)
        : data_dir(dir), THRESHOLD(threshold), USE_MMAP(use_mmap), USE_COMPRESSION(use_compression) {
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
        uint64_t max_seq = 0;
        for (const auto& entry : history) {
            max_seq = std::max(max_seq, entry.sequence + 1);
        }
        current_seq = max_seq;

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

        // Find the current WAL segment by scanning for .log files
        // directly, rather than deriving its path from current_seq.
        // current_seq is a single shared counter that BOTH flush() and
        // compact_once() claim unique sequence numbers from (purely to
        // keep SSTable naming globally unique) -- but only flush() ever
        // creates a new WAL file, always at (that flush's own claimed
        // sequence + 1). If compact_once() claims a later number AFTER
        // the last flush before shutdown (very possible with background
        // compaction running independently of the write path), current_seq
        // ends up higher than the sequence the actual active WAL file was
        // named with -- computing the path from current_seq would then
        // open a nonexistent file, silently replaying nothing and losing
        // every write since that last flush. The highest-numbered .log
        // file actually present on disk is unambiguously the correct one
        // regardless of what compaction did to the shared counter, since
        // nothing else ever creates .log files.
        uint64_t latest_wal_seq = 0;
        bool found_wal = false;
        for (const auto& dirent : std::filesystem::directory_iterator(data_dir)) {
            if (dirent.path().extension() == ".log") {
                uint64_t seq = std::stoull(dirent.path().stem().string());
                if (!found_wal || seq > latest_wal_seq) {
                    latest_wal_seq = seq;
                    found_wal = true;
                }
            }
        }
        std::string wal_path = found_wal ? get_wal_path(latest_wal_seq) : get_wal_path(current_seq.load());
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
    // valid, ifstream path otherwise). Read-only. Callers must hold
    // files_lock (shared is sufficient).
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
    // that file's del-bitmap before being trusted. Slow path (genuinely
    // new key, or the narrow crash-recovery gap described below): scan
    // every live SSTable, newest to oldest, exactly as before this index
    // existed -- this is ground truth; the fast path is only ever an
    // accelerator over it, never authoritative on its own.
    //
    // Locking: shard lock and files_lock are acquired and fully released
    // one at a time, never nested (see the struct-level comment on lock
    // ordering) -- the Location value is copied out of the shard map
    // before files_lock is even touched.
    std::optional<Location> find_live_location(const std::string& key) {
        size_t shard = shard_for(key);

        std::optional<Location> maybe_loc;
        {
            std::shared_lock slock(shard_locks[shard]);
            auto gi_it = global_key_index[shard].find(key);
            if (gi_it != global_key_index[shard].end()) maybe_loc = gi_it->second;
        }

        if (maybe_loc) {
            std::string file = get_sst_path(maybe_loc->file_seq);
            bool trustworthy_and_live;
            {
                std::shared_lock flock(files_lock);
                auto del_it = delcol_cache.find(file);
                // If the file isn't in files_lock's domain at all, it was
                // most likely just compacted away by a pass whose
                // separate shard-index update (see compact_once()) hasn't
                // run yet -- global_key_index[shard] is temporarily
                // pointing at a generation files_lock no longer knows
                // about. Treating that as "not dead, therefore live" (the
                // original bug here) would let mark_dead below flip a bit
                // in a file that's gone instead of the record's actual
                // current copy, leaving that real copy permanently
                // unmarked -- exactly the gap that let a later compaction
                // pass "resurrect" a stale pointer over a legitimately
                // newer one. Only a file files_lock still recognizes, with
                // its bit unset, counts as trustworthy; anything else
                // (explicitly dead, or unknown) falls through to the
                // slow-path scan below, which is always internally
                // consistent since it holds files_lock for its whole
                // duration.
                trustworthy_and_live = del_it != delcol_cache.end() &&
                                        !SSTable::is_dead(del_it->second, maybe_loc->serial);
            }
            if (trustworthy_and_live) return *maybe_loc;

            // Stale entry (explicitly dead, or pointing at a file
            // files_lock no longer recognizes) -- self-heal by erasing
            // it, but re-verify it's still exactly the same entry first
            // (files_lock was not held together with the shard lock
            // above, so another thread -- e.g. a flush() for this very
            // key -- could have already replaced it with a fresh, correct
            // entry in the gap; never blindly erase without checking).
            std::unique_lock slock(shard_locks[shard]);
            auto gi_it = global_key_index[shard].find(key);
            if (gi_it != global_key_index[shard].end() &&
                gi_it->second.file_seq == maybe_loc->file_seq && gi_it->second.serial == maybe_loc->serial) {
                global_key_index[shard].erase(gi_it);
            }
        }

        std::shared_lock flock(files_lock);
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

    // Marks `loc` dead: flips its del-bitmap bit (in-memory + on-disk),
    // then removes `key` from global_key_index, since the index only ever
    // tracks currently-live locations. files_lock and the shard lock are
    // acquired and released sequentially, never nested.
    void mark_dead(const std::string& key, const Location& loc) {
        std::string file = get_sst_path(loc.file_seq);
        {
            std::unique_lock flock(files_lock);
            auto del_it = delcol_cache.find(file);
            if (del_it != delcol_cache.end()) {
                SSTable::mark_dead_in_memory(del_it->second, loc.serial);
            }
            SSTable::flip_dead_on_disk(SSTable::del_path_for(file), loc.serial);
        }
        {
            std::unique_lock slock(shard_locks[shard_for(key)]);
            global_key_index[shard_for(key)].erase(key);
        }
    }

    // Core Put/Delete logic, shared between live calls and WAL replay
    // during recovery -- callers are responsible for the WAL append (live
    // calls do it before calling these; replay already has a durable WAL
    // entry it's redoing). Requires the target key's memtable shard lock
    // held exclusively (held by Put/Delete for their own shard, or
    // implicitly single-threaded during construction) -- indexes directly
    // into memtable_shards[memtable_shard_for(key)] without locking here
    // itself, matching that assumption.
    void apply_put(const std::string& key, const std::string& value) {
        auto loc = find_live_location(key);
        if (loc) mark_dead(key, *loc);
        memtable_shards[memtable_shard_for(key)][key] = value;
    }

    void apply_delete(const std::string& key) {
        memtable_shards[memtable_shard_for(key)].erase(key);
        auto loc = find_live_location(key);
        if (loc) mark_dead(key, *loc);
    }

    // Fully self-locking (no single caller could reasonably pre-acquire
    // every memtable shard lock the way one memtable_lock used to suffice)
    // -- deliberately keeps holding all of them for its ENTIRE body,
    // including the SSTable disk write, rather than releasing partway
    // through the way compact_once() releases its lock during its slow
    // merge phase. That's not an oversight: compact_once() merges
    // already-durable, already-visible files, so releasing its lock
    // exposes nothing new to a concurrent reader. flush() is draining the
    // ONLY copy of not-yet-durable data out of memory -- clearing a shard
    // before the write completes would create a window where a
    // just-flushed key is invisible to Get() (gone from the memtable, not
    // yet indexed on disk). The standard fix for that (keeping a
    // read-only "immutable memtable" snapshot visible during the write)
    // reopens a worse hole: a concurrent Delete() for a key in that
    // snapshot can't remove it from a structure being concurrently read
    // by the write itself, so the delete would go nowhere and the flush
    // would durably persist the stale value -- solving that needs a
    // tombstone-shaped mechanism again, undoing exactly what the
    // del-bitmap redesign removed. Sharding the memtable doesn't change
    // that reasoning, so this stays fully synchronous, at the cost of
    // Put/Delete still blocking on any flush they trigger -- same as
    // before, just no longer blocking Gets/Puts/Deletes for OTHER keys
    // that don't need the same memtable/index-shard/file locks flush()
    // briefly takes.
    void flush() {
        std::lock_guard<std::mutex> serialize(flush_serialize_mutex_);

        // Acquire every memtable shard lock together, in this fixed order
        // -- the only place that ever happens, so no other code path's
        // ordering (which only ever touches one shard at a time) can
        // conflict with it. Held via RAII for the whole function.
        std::vector<std::unique_lock<std::shared_mutex>> held;
        held.reserve(kNumMemtableShards);
        for (size_t i = 0; i < kNumMemtableShards; ++i) {
            held.emplace_back(memtable_shard_locks[i]);
        }

        size_t total = 0;
        for (const auto& shard : memtable_shards) total += shard.size();
        if (total == 0) return; // another flush already handled it (see flush_serialize_mutex_)

        // Combine all shards into one sorted map -- the only place a sort
        // is actually needed (write_file requires it), so the shards
        // themselves stay unordered_map for cheaper everyday Put/Delete.
        std::map<std::string, std::string> combined;
        for (const auto& shard : memtable_shards) {
            combined.insert(shard.begin(), shard.end());
        }

        uint64_t seq = current_seq.fetch_add(1);
        std::string sst_path = get_sst_path(seq);
        std::cout << "--- Persisting Generation " << seq << " to Disk ---" << std::endl;

        std::vector<IndexEntry> index;
        if (SSTable::write_file(sst_path, combined, index, USE_COMPRESSION)) {
            // Del-bitmap sized to the flush threshold (the "flush key
            // limit"), not the actual record count -- most bits stay
            // unused if fewer than THRESHOLD keys were flushed (e.g. via
            // ForceFlush), which is harmless.
            auto del_bits = SSTable::make_del_bitmap(THRESHOLD);
            SSTable::save_del_bitmap(SSTable::del_path_for(sst_path), del_bits);

            // Register the new file in files_lock's domain FIRST, before
            // global_key_index can point at it below, and Manifest::add_entry
            // (an append) INSIDE files_lock, not after releasing it -- both
            // load-bearing fixes for real bugs the concurrency stress test
            // found during the per-shard-locking phase (see that phase's
            // notes in this plan's history for the full explanation);
            // unchanged here, just reached via a different outer lock.
            {
                std::unique_lock flock(files_lock);
                if (USE_MMAP) mmap_cache.emplace(sst_path, MappedFile(sst_path));
                index_cache[sst_path] = index; // copy -- the shard loop below still needs `index`
                delcol_cache[sst_path] = std::move(del_bits);
                // A freshly flushed generation is always the newest by
                // construction, so it belongs at the end -- no sort
                // needed. (Recency is tracked purely by vector position,
                // not by filename/sequence value, so that compaction's
                // merged files -- which get a fresh, unrelated sequence
                // number but must stay logically oldest -- can't ever get
                // scrambled out of place by re-sorting.)
                sstable_files.push_back(sst_path);
                Manifest::add_entry(data_dir, format_seq(seq) + ".sst", seq);
            }

            uint32_t this_file_seq = static_cast<uint32_t>(seq);
            for (const auto& entry : index) {
                std::unique_lock slock(shard_locks[shard_for(entry.key)]);
                global_key_index[shard_for(entry.key)][entry.key] =
                    Location{this_file_seq, static_cast<uint32_t>(entry.serial)};
            }

            for (auto& shard : memtable_shards) shard.clear();

            {
                std::unique_lock wlock(wal_lock);
                wal = std::make_unique<WAL>(get_wal_path(seq + 1));
            }

            std::cout << "--- Flush Complete. Generation is now: " << current_seq.load() << " ---" << std::endl;
        }
        // `held` releases every memtable shard lock here, RAII.
    }

    // Merges the two globally-oldest surviving SSTable generations into
    // one, if at least 3 flushed generations exist (2 to merge, 1 newest
    // left untouched). Returns false if there's nothing eligible.
    //
    // The slow part -- reading both files, merging, writing the new one --
    // holds NO lock, since SSTables are immutable once written and reading
    // one is safe to interleave with anything. Only the brief metadata
    // swap at the end takes files_lock exclusively, plus per-key shard
    // locks (sequentially, never nested with files_lock -- see the
    // struct-level lock-ordering comment). Never touches any memtable
    // shard lock at all -- compaction doesn't read or write the memtable.
    bool compact_once() {
        std::lock_guard<std::mutex> serialize(compaction_serialize_mutex_);

        std::string path_a, path_b;
        std::vector<uint8_t> del_a_snapshot, del_b_snapshot;
        {
            std::shared_lock flock(files_lock);
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
        std::string tmp_path = (std::filesystem::path(data_dir) / "compact.tmp").string();
        std::vector<IndexEntry> merged_index;
        std::vector<uint8_t> merged_del;
        if (!SSTable::merge_files(path_a, del_a_snapshot, path_b, del_b_snapshot,
                                   tmp_path, merged_index, merged_del, USE_COMPRESSION)) {
            // Couldn't write the merged file -- abort without touching any
            // live state. Old generations are untouched; try again on the
            // next pass.
            return false;
        }
        bool wrote_file = !merged_index.empty();

        // --- Swap phase: files_lock only; shard updates happen after it's released ---
        std::string final_path;
        uint32_t merged_file_seq = 0;
        bool committed_new_file = false;
        {
            std::unique_lock flock(files_lock);

            // A concurrent Put/Delete may have marked a record in path_a
            // or path_b dead WHILE the slow merge above was working from a
            // snapshot of their del-bitmaps taken before it started -- if
            // so, the merge may have copied forward a record that's
            // actually dead now, which would silently resurrect a stale
            // value if committed. Detect this by comparing the live
            // del-bitmaps against the snapshot; if either changed, discard
            // this merge's result and retry from scratch on the next pass
            // rather than risk committing a merge computed from stale
            // liveness data.
            auto still_matches = [this](const std::string& path, const std::vector<uint8_t>& snapshot) {
                auto it = delcol_cache.find(path);
                const std::vector<uint8_t>& live = (it != delcol_cache.end()) ? it->second : std::vector<uint8_t>{};
                return live == snapshot;
            };
            if (!still_matches(path_a, del_a_snapshot) || !still_matches(path_b, del_b_snapshot)) {
                flock.unlock();
                if (wrote_file) {
                    std::error_code ec;
                    std::filesystem::remove(tmp_path, ec);
                }
                return false;
            }

            if (!wrote_file) {
                // Both generations fully cancelled out (every key ended up
                // deleted or shadowed) -- discard the empty temp file
                // rather than registering a pointless empty generation;
                // the pair just vanishes with no replacement.
                std::error_code ec;
                std::filesystem::remove(tmp_path, ec);
            } else {
                uint64_t merged_seq = current_seq.fetch_add(1);
                merged_file_seq = static_cast<uint32_t>(merged_seq);
                final_path = get_sst_path(merged_seq);
                // Fresh, never-before-used sequence number -> this rename
                // can never collide with or overwrite any live file.
                std::filesystem::rename(tmp_path, final_path);
                if (USE_MMAP) mmap_cache.emplace(final_path, MappedFile(final_path));
                SSTable::save_del_bitmap(SSTable::del_path_for(final_path), merged_del);
                delcol_cache[final_path] = std::move(merged_del);
                // Copy (not move) merged_index here -- the shard-update
                // loop below still needs it, and that loop must run
                // outside this files_lock hold (never nesting files_lock
                // with shard_locks).
                index_cache[final_path] = merged_index;
                committed_new_file = true;
            }

            sstable_files.erase(sstable_files.begin(), sstable_files.begin() + 2);
            index_cache.erase(path_a);
            index_cache.erase(path_b);
            delcol_cache.erase(path_a);
            delcol_cache.erase(path_b);
            mmap_cache.erase(path_a);
            mmap_cache.erase(path_b);
            if (committed_new_file) {
                // Still logically the oldest survivor -- position, not the
                // (fresh, numerically large) sequence number, is what
                // matters.
                sstable_files.insert(sstable_files.begin(), final_path);
            }

            // Durable commit point: rewrite the whole Manifest to match
            // the just-updated sstable_files exactly, in order. Must
            // happen AFTER the rename above -- if it happened first and we
            // crashed before the rename, the Manifest would reference a
            // file that doesn't physically exist yet, losing both source
            // generations' data. Doing the rename first means a crash
            // before this point just leaves a harmless orphaned temp file.
            std::vector<ManifestEntry> entries;
            entries.reserve(sstable_files.size());
            for (const auto& p : sstable_files) {
                entries.push_back({std::filesystem::path(p).filename().string(), parse_seq_from_path(p)});
            }
            Manifest::rewrite(data_dir, entries);
        }

        // Every surviving record gets a FRESH serial in the merged file,
        // so (unlike Put/Delete) this can't rely on global_key_index
        // already being correct for these keys -- each one needs its
        // entry rewritten to point at the new file+serial directly.
        // Entries for the merged-away files' now fully-dead keys need no
        // cleanup here: they were already erased when they died, via
        // mark_dead, potentially generations before this compaction pass
        // ever ran.
        if (committed_new_file) {
            for (const auto& entry : merged_index) {
                std::unique_lock slock(shard_locks[shard_for(entry.key)]);
                global_key_index[shard_for(entry.key)][entry.key] =
                    Location{merged_file_seq, static_cast<uint32_t>(entry.serial)};
            }
        }

        // Physically delete the two old files now that the Manifest no
        // longer references them. Safe outside any lock and after
        // everything above -- nothing reads them anymore in the sense
        // that matters (no live SSTable-level cache still names them) --
        // but a concurrent Get() already past that check may still have
        // one open (e.g. mid-read via ifstream) when this runs, which
        // some platforms (Windows in particular, unlike POSIX) refuse to
        // delete out from under an open handle. Use the non-throwing
        // overload and treat failure as a harmless orphan to clean up
        // later, exactly like this codebase already tolerates elsewhere
        // (e.g. leftover WAL segments after a flush) -- a failed delete
        // here is not a correctness problem, since the Manifest already
        // durably stopped referencing these files.
        std::error_code ec;
        std::filesystem::remove(path_a, ec);
        std::filesystem::remove(path_b, ec);
        std::filesystem::remove(SSTable::del_path_for(path_a), ec);
        std::filesystem::remove(SSTable::del_path_for(path_b), ec);

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

    // Snapshots global_key_index to disk, one shard at a time (each
    // shard's lock held only briefly, for its own copy) rather than one
    // lock covering the whole structure at once -- slightly weaker
    // snapshot consistency (the shards are no longer copied atomically as
    // one point-in-time view), acceptable since the checkpoint is already
    // explicitly designed to tolerate staleness. The actual disk write
    // then happens outside any lock.
    bool checkpoint_index_once() {
        std::lock_guard<std::mutex> serialize(checkpoint_serialize_mutex_);

        ShardedIndex snapshot;
        for (size_t i = 0; i < kNumShards; ++i) {
            std::shared_lock slock(shard_locks[i]);
            snapshot[i] = global_key_index[i];
        }
        uint64_t seq_marker = current_seq.load();
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

StorageEngine::StorageEngine(const std::string& data_dir, size_t memtable_threshold, bool use_mmap_reads,
                              bool use_value_compression)
    : pImpl(std::make_unique<Impl>(data_dir, memtable_threshold, use_mmap_reads, use_value_compression)) {
    std::cout << "Engine initialized at: " << data_dir
              << " | Active Sequence: " << pImpl->current_seq.load() << std::endl;
}

StorageEngine::~StorageEngine() = default;

bool StorageEngine::Put(const std::string& key, const std::string& value) {
    size_t shard = Impl::memtable_shard_for(key);
    {
        // This shard's lock is released before the threshold check below
        // (never held while calling flush(), which needs to acquire every
        // memtable shard lock including this one -- see the struct-level
        // lock-ordering comment for why that's a hard requirement, not
        // just tidiness).
        std::unique_lock slock(pImpl->memtable_shard_locks[shard]);
        bool appended;
        {
            std::shared_lock wlock(pImpl->wal_lock);
            appended = pImpl->wal->append(LogOp::PUT, pImpl->current_seq.load(), key, value);
        }
        if (!appended) return false;
        pImpl->apply_put(key, value);
    }

    // Total size across all shards, one shard's lock at a time, shared,
    // never two held simultaneously -- avoids a lock-ordering deadlock
    // against another thread summing in the opposite shard order.
    size_t total = 0;
    for (size_t i = 0; i < kNumMemtableShards; ++i) {
        std::shared_lock l(pImpl->memtable_shard_locks[i]);
        total += pImpl->memtable_shards[i].size();
    }
    if (total >= pImpl->THRESHOLD) {
        pImpl->flush();
    }

    return true;
}

void StorageEngine::ForceFlush() {
    pImpl->flush(); // fully self-locking
}

std::optional<std::string> StorageEngine::Get(const std::string& key) {
    {
        size_t mshard = Impl::memtable_shard_for(key);
        std::shared_lock lock(pImpl->memtable_shard_locks[mshard]);
        auto it = pImpl->memtable_shards[mshard].find(key);
        if (it != pImpl->memtable_shards[mshard].end()) {
            return it->second;
        }
    }

    // Fast path: global_key_index gives an O(1) candidate location instead
    // of scanning every live generation newest-to-oldest. A dead candidate
    // falls through to the slow path below rather than being trusted --
    // Get() can't self-heal a stale entry itself (that happens lazily, the
    // next time find_live_location touches this key). The shard lock and
    // files_lock are acquired and released sequentially here too, never
    // nested, matching the rest of this file.
    size_t shard = Impl::shard_for(key);
    std::optional<Location> maybe_loc;
    {
        std::shared_lock slock(pImpl->shard_locks[shard]);
        auto gi_it = pImpl->global_key_index[shard].find(key);
        if (gi_it != pImpl->global_key_index[shard].end()) maybe_loc = gi_it->second;
    }
    if (maybe_loc) {
        std::string file = pImpl->get_sst_path(maybe_loc->file_seq);
        std::shared_lock flock(pImpl->files_lock);
        auto del_it = pImpl->delcol_cache.find(file);
        bool dead = del_it != pImpl->delcol_cache.end() &&
                    SSTable::is_dead(del_it->second, maybe_loc->serial);
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
    std::shared_lock flock(pImpl->files_lock);
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
    // Delete never triggers a flush (unchanged from before this phase):
    // under eager dead-marking, a delete only shrinks or leaves unchanged
    // whichever memtable shard it touches, never grows one, so it was
    // never the thing that could newly cross THRESHOLD.
    size_t shard = Impl::memtable_shard_for(key);
    std::unique_lock slock(pImpl->memtable_shard_locks[shard]);
    bool appended;
    {
        std::shared_lock wlock(pImpl->wal_lock);
        appended = pImpl->wal->append(LogOp::DELETE, pImpl->current_seq.load(), key, "");
    }
    if (!appended) return false;

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
