#include "../include/engine/storage_engine.h"
#include <map>
#include <mutex>
#include <shared_mutex>
#include <iostream>

namespace kv_engine {

// Internal structure to hide implementation details from the header
struct StorageEngine::Impl {
    std::map<std::string, std::string> data_map;
    std::shared_mutex rw_lock; // Allows multiple readers, one writer
};

StorageEngine::StorageEngine() : pImpl(std::make_unique<Impl>()) {
    std::cout << "Storage Engine Initialized (In-Memory Mode)" << std::endl;
}

StorageEngine::~StorageEngine() = default;

bool StorageEngine::Put(const std::string& key, const std::string& value) {
    std::unique_lock lock(pImpl->rw_lock); // Exclusive lock for writing
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