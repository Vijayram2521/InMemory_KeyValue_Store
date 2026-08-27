#pragma once
#include <array>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <optional>

namespace kv_engine {

// One entry in StorageEngine's global key -> SSTable-location index: which
// generation a key currently lives in (by sequence number, not path -- see
// StorageEngine::Impl::global_key_index for why) and its serial number
// within that file's own del-bitmap.
struct Location {
    uint32_t file_seq = 0;
    uint32_t serial = 0;
};

// The global key index is sharded into this many independent maps, chosen
// by hash(key) % kNumShards, rather than one single map -- smaller
// individual checkpoint files and smaller individual rehash events as each
// shard grows on its own, and it sets up (without yet implementing)
// per-shard locking as a later, separate step if the engine ever moves
// beyond one lock covering the whole StorageEngine. A single named
// constant so the shard count is a one-line change if it ever needs to
// differ.
constexpr size_t kNumShards = 32;

using ShardedIndex = std::array<std::unordered_map<std::string, Location>, kNumShards>;

// Result of IndexCheckpoint::load_latest: each shard's entries as of the
// same checkpoint pass, plus the sequence number they're valid "as of" --
// every SSTable with a lower sequence number is fully reflected in `shards`
// already. The caller (StorageEngine's constructor) is responsible for
// folding in anything with sequence >= covered_seq itself; this class only
// knows about the checkpoint file format, not SSTables.
struct LoadedCheckpoint {
    uint64_t covered_seq = 0;
    ShardedIndex shards;
};

// Periodic durable snapshot of StorageEngine's global_key_index, so restart
// doesn't have to rebuild it from every live SSTable's index from scratch.
// Deliberately allowed to be stale -- see StorageEngine's restart logic for
// how a stale checkpoint gets reconciled (fold in newer generations, plus
// lazy self-healing against del-bitmaps for entries that died without a new
// generation being created).
//
// On-disk shape: one small pointer file (CHECKPOINT, mirroring how
// MANIFEST already anchors SSTable state) naming the sequence number shared
// by the latest checkpoint pass, plus one data file per shard per pass
// (checkpoint_<shard>_<seq>.idx) holding that shard's flat
// [keyLen:u32][key][file_seq:u32][serial:u32] records.
class IndexCheckpoint {
public:
    // Writes all 32 shards to new checkpoint files (temp-file-then-rename
    // each, same crash-safety pattern Manifest::rewrite and
    // compact_once()'s swap already use), then atomically updates the
    // CHECKPOINT pointer to name this pass's sequence number, then removes
    // whichever pass the pointer previously named. The pointer is only
    // updated after every shard file has fully landed, and the old pass's
    // files are only removed after the pointer has fully landed -- a crash
    // at any point in this sequence leaves either the old pass or the new
    // one fully valid and findable via load_latest, never a pointer naming
    // a partially-written pass.
    static void write(const std::string& dir, uint64_t covered_seq, const ShardedIndex& shards);

    // Reads the CHECKPOINT pointer, then every shard file it names.
    // Returns std::nullopt only if no pointer file exists at all -- a
    // missing or unreadable individual shard file just leaves that one
    // shard's map empty in the result (degrading to "rebuild this shard
    // from live generations," same fallback reasoning as no checkpoint at
    // all, scoped to just that shard).
    static std::optional<LoadedCheckpoint> load_latest(const std::string& dir);
};

}
