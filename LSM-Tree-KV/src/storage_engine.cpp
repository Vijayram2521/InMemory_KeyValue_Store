#include "../include/engine/storage_engine.h"
#include <map>
#include <mutex>
#include <shared_mutex>
#include <iostream>
#include "../include/engine/wal.h"

namespace kv_engine {

// Internal structure to hide implementation details from the header
struct StorageEngine::Impl {
    std::map<std::string, std::string> data_map;
    std::shared_mutex rw_lock; // Allows multiple readers, one writer
    WAL wal;

    Impl(const std::string& path) : wal(path) {}
};

StorageEngine::StorageEngine(const std::string& wal_path) : pImpl(std::make_unique<Impl>(wal_path)) {
    // On startup, recover the MemTable state from the WAL
    pImpl->wal.recover(pImpl->data_map);
}

StorageEngine::~StorageEngine() = default;

bool StorageEngine::Put(const std::string& key, const std::string& value) {
    std::unique_lock lock(pImpl->rw_lock); // Exclusive lock for writing
    if (!pImpl->wal.append(LogOp::PUT, key, value)) {
        return false; // If we can't log it, we don't write it to memory!
    }
    pImpl->data_map[key] = value;
    return true;
}

std::optional<std::string> StorageEngine::Get(const std::string& key) {
    std::shared_lock lock(pImpl->rw_lock); // Shared lock for reading
    auto it = pImpl->data_map.find(key);
    if (it != pImpl->data_map.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool StorageEngine::Delete(const std::string& key) {
    std::unique_lock lock(pImpl->rw_lock);
    return pImpl->data_map.erase(key) > 0;
}

} 