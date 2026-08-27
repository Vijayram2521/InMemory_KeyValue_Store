#include "engine/index_checkpoint.h"
#include <filesystem>
#include <fstream>

namespace kv_engine {

namespace {
    std::string pointer_path(const std::string& dir) {
        return (std::filesystem::path(dir) / "CHECKPOINT").string();
    }
    // Checkpoint filenames don't need to sort lexicographically the way
    // .sst/.log/.del files do -- the current pass is always found via the
    // CHECKPOINT pointer, never by listing the directory -- so plain
    // (non-zero-padded) numbers keep this module independent of
    // storage_engine.cpp's format_seq.
    std::string shard_path(const std::string& dir, size_t shard, uint64_t seq) {
        return (std::filesystem::path(dir) /
                ("checkpoint_" + std::to_string(shard) + "_" + std::to_string(seq) + ".idx")).string();
    }

    void write_shard(const std::string& final_path, const std::unordered_map<std::string, Location>& shard) {
        std::string tmp_path = final_path + ".tmp";
        {
            std::ofstream ofs(tmp_path, std::ios::binary | std::ios::trunc);
            if (!ofs.is_open()) return;
            for (const auto& [key, loc] : shard) {
                uint32_t kLen = static_cast<uint32_t>(key.size());
                ofs.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
                ofs.write(key.data(), kLen);
                ofs.write(reinterpret_cast<const char*>(&loc.file_seq), sizeof(loc.file_seq));
                ofs.write(reinterpret_cast<const char*>(&loc.serial), sizeof(loc.serial));
            }
        }
        std::error_code ec;
        std::filesystem::rename(tmp_path, final_path, ec);
        if (ec) std::filesystem::remove(tmp_path);
    }

    void load_shard(const std::string& path, std::unordered_map<std::string, Location>& out) {
        std::ifstream ifs(path, std::ios::binary);
        if (!ifs.is_open()) return; // missing/unreadable shard -- caller degrades to empty for it
        while (ifs.peek() != EOF) {
            uint32_t kLen = 0;
            if (!ifs.read(reinterpret_cast<char*>(&kLen), sizeof(kLen))) break;
            std::string key(kLen, '\0');
            if (kLen > 0 && !ifs.read(&key[0], kLen)) break;
            Location loc;
            if (!ifs.read(reinterpret_cast<char*>(&loc.file_seq), sizeof(loc.file_seq))) break;
            if (!ifs.read(reinterpret_cast<char*>(&loc.serial), sizeof(loc.serial))) break;
            out[std::move(key)] = loc;
        }
    }
}

void IndexCheckpoint::write(const std::string& dir, uint64_t covered_seq, const ShardedIndex& shards) {
    // Read the OLD pointer (if any) before overwriting it, purely so that
    // pass's files can be cleaned up afterward -- not needed for
    // correctness of the new pass itself.
    uint64_t old_seq = 0;
    bool had_old = false;
    {
        std::ifstream ptr_ifs(pointer_path(dir));
        if (ptr_ifs.is_open() && (ptr_ifs >> old_seq)) had_old = true;
    }

    for (size_t shard = 0; shard < kNumShards; ++shard) {
        write_shard(shard_path(dir, shard, covered_seq), shards[shard]);
    }

    std::string ptr_tmp = pointer_path(dir) + ".tmp";
    {
        std::ofstream ofs(ptr_tmp, std::ios::trunc);
        // A crash or failure here leaves this pass's shard files orphaned
        // (the pointer never names them) -- harmless, matching how this
        // codebase already treats e.g. leftover WAL segments after a flush.
        if (!ofs.is_open()) return;
        ofs << covered_seq;
    }
    std::error_code ec;
    std::filesystem::rename(ptr_tmp, pointer_path(dir), ec);
    if (ec) return;

    // Only now that the pointer durably names the NEW pass is it safe to
    // delete the old one's files -- a crash between the rename above and
    // this loop just leaves harmless orphaned files on disk, never a
    // pointer naming something missing.
    if (had_old && old_seq != covered_seq) {
        for (size_t shard = 0; shard < kNumShards; ++shard) {
            std::filesystem::remove(shard_path(dir, shard, old_seq), ec);
        }
    }
}

std::optional<LoadedCheckpoint> IndexCheckpoint::load_latest(const std::string& dir) {
    std::ifstream ptr_ifs(pointer_path(dir));
    uint64_t seq = 0;
    if (!ptr_ifs.is_open() || !(ptr_ifs >> seq)) return std::nullopt;

    LoadedCheckpoint result;
    result.covered_seq = seq;
    for (size_t shard = 0; shard < kNumShards; ++shard) {
        load_shard(shard_path(dir, shard, seq), result.shards[shard]);
    }
    return result;
}

}
