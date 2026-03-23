#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <string>
#include <optional>
#include <vector>
#include <memory>

namespace kv_engine {
    class StorageEngine {
        public : 
            StorageEngine() ;
            ~StorageEngine() ;

            // Prevent copying to avoid multiple engines fighting over the same files
            StorageEngine(const StorageEngine&) = delete;
            StorageEngine& operator=(const StorageEngine&) = delete;

            /**
             * Inserts or updates a key-value pair.
             * @return true if the write was persisted to the WAL and MemTable.
             */
            bool Put(const std::string& key, const std::string& value);

            /**
             * Retrieves a value associated with a key.
             * @return std::nullopt if key is not found, otherwise the string value.
             */
            std::optional<std::string> Get(const std::string& key);

            /**
             * Removes a key from the store. 
             * Note: In LSM-trees, this is usually a "Tombstone" write.
             */
            bool Delete(const std::string& key);
        
        private:
            // Mem Table management and WAL handling
            struct Impl;
            std::unique_ptr<Impl> pImpl;
    };
}

#endif // STORAGE_ENGINE_H