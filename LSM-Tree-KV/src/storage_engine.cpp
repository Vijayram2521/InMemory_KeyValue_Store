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
#include <set>

namespace kv_engine {

std::string format_seq(uint64_t seq) {
    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << seq;
    return oss.str();
}

struct StorageEngine::Impl {
    std::map<std::string, std::string> memtable;
    std::shared_mutex rw_lock; 
    std::unique_ptr<WAL> wal;
    uint64_t current_seq;
    std::string data_dir;
    std::vector<std::string> sstable_files;
    std::set<std::string> tombstones;
    const size_t THRESHOLD = 5;

    Impl(const std::string& dir) : data_dir(dir) {
        std::filesystem::create_directories(data_dir);

        auto history = Manifest::load_history(data_dir);
        for (const auto& entry : history) {
            std::string full_path = (std::filesystem::path(data_dir) / entry.filename).string();
            sstable_files.push_back(full_path);
        }
        current_seq = history.empty() ? 0 : history.back().sequence + 1;

        std::string wal_path = get_wal_path(current_seq);
        wal = std::make_unique<WAL>(wal_path);
        wal->recover(memtable, tombstones);

        // std::sort(sstable_files.begin(), sstable_files.end());
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

        // 1. Write the SSTable using both the map and the tombstone set
        if (SSTable::write_file(sst_path, memtable, tombstones)) {
            
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

            // 5. Update the live file list for "First Hit" searching
            sstable_files.push_back(sst_path);
            // Keep them sorted so rbegin() is always the newest
            std::sort(sstable_files.begin(), sstable_files.end());

            std::cout << "--- Flush Complete. Generation is now: " << current_seq << " ---" << std::endl;
        }
    }

};

StorageEngine::StorageEngine(const std::string& data_dir) 
    : pImpl(std::make_unique<Impl>(data_dir)) {
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

    // Search SSTables newest to oldest
    for (auto rit = pImpl->sstable_files.rbegin(); rit != pImpl->sstable_files.rend(); ++rit) {
        // We will need to update SSTable::search_file to return a 'SearchResult' 
        // that distinguishes between "Not Found" and "Found a Tombstone"
        auto result = SSTable::search_file(*rit, key); 
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

} // namespace kv_engine