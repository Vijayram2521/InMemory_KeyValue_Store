#include "../include/engine/storage_engine.h"
#include <map>
#include <mutex>
#include <shared_mutex>
#include <iostream>
#include "../include/engine/wal.h"
#include "../include/engine/manifest.h"
#include <iomanip>
#include <filesystem>
#include <algorithm>
#include "../include/engine/sstable.h"

namespace kv_engine {

std::string format_seq(uint64_t seq) {
    std::ostringstream oss;
    oss << std::setw(6) << std::setfill('0') << seq;
    return oss.str();
}

// Internal structure to hide implementation details from the header
struct StorageEngine::Impl {
    std::map<std::string, std::string> memtable;
    std::shared_mutex rw_lock; // Allows multiple readers, one writer
    std::unique_ptr<WAL> wal;
    uint64_t current_seq;
    std::string data_dir;
    std::vector<std::string> sstable_files;
    const size_t THRESHOLD = 5;

    Impl(const std::string& dir) : data_dir(dir) {
        // 1. Ensure directory exists
        std::filesystem::create_directories(data_dir);

        // 2. Get the last known sequence from Manifest
        current_seq = Manifest::get_last_seq(data_dir);

        // 3. Initialize WAL with the current sequence
        std::string wal_path = get_wal_path(current_seq);
        wal = std::make_unique<WAL>(wal_path);

        // 4. Recover data into MemTable
        wal->recover(memtable);

        // 5. Scan directory for any existing .sst files
        for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
            if (entry.path().extension() == ".sst") {
                sstable_files.push_back(entry.path().string());
            }
        }
        // Sort SSTables so we search newest first later
        std::sort(sstable_files.rbegin(), sstable_files.rend());
    }

    std::string get_wal_path(uint64_t seq) {
        return (std::filesystem::path(data_dir) / (format_seq(seq) + ".log")).string();
    }
    std::string get_sst_path(uint64_t seq) {
        return (std::filesystem::path(data_dir) / (format_seq(seq) + ".sst")).string();
    }
};

StorageEngine::StorageEngine(const std::string& data_dir) 
    : pImpl(std::make_unique<Impl>(data_dir)) {
    std::cout << "Engine initialized at: " << data_dir 
              << " | Active Sequence: " << pImpl->current_seq << std::endl;
}

StorageEngine::~StorageEngine() = default;

bool StorageEngine::Put(const std::string& key, const std::string& value) {
    std::unique_lock lock(pImpl->rw_lock); // Exclusive lock for writing
    if (!pImpl->wal->append(LogOp::PUT, key, value)) {
        return false; // If we can't log it, we don't write it to memory!
    }
    pImpl->memtable[key] = value;

    if (pImpl->memtable.size() >= pImpl->THRESHOLD) {
        std::cout << "--- Threshold reached. Flushing to SSTable #" << pImpl->current_seq << " ---" << std::endl;

        // A. Write current MemTable to SSTable
        std::string sst_path = pImpl->get_sst_path(pImpl->current_seq);
        if (SSTable::write_file(sst_path, pImpl->memtable)) {
            pImpl->sstable_files.push_back(sst_path);
            
            // B. Increment Sequence and Rotate WAL
            uint64_t next_seq = pImpl->current_seq + 1;
            std::string next_wal_path = pImpl->get_wal_path(next_seq);
            
            // Close old WAL and open new one
            pImpl->wal = std::make_unique<WAL>(next_wal_path);
            
            // C. Update Manifest so we know where to start next time
            Manifest::update_seq(pImpl->data_dir, next_seq);
            
            // D. Clear MemTable and update sequence
            pImpl->memtable.clear();
            pImpl->current_seq = next_seq;
            
            std::cout << "--- Flush Complete. New Active WAL: " << next_seq << " ---" << std::endl;
        }
    }

    return true;
}

std::optional<std::string> StorageEngine::Get(const std::string& key) {
    std::shared_lock lock(pImpl->rw_lock);

    // 1. Search MemTable (Most recent data)
    auto it = pImpl->memtable.find(key);
    if (it != pImpl->memtable.end()) {
        return it->second;
    }

    // 2. Search SSTables (from newest to oldest)
    // We iterate backwards through sstable_files
    for (auto rit = pImpl->sstable_files.rbegin(); rit != pImpl->sstable_files.rend(); ++rit) {
        auto val = SSTable::search_file(*rit, key);
        if (val) return val;
    }

    return std::nullopt;
}

bool StorageEngine::Delete(const std::string& key) {
    std::unique_lock lock(pImpl->rw_lock);
    return pImpl->memtable.erase(key) > 0;
}

} 