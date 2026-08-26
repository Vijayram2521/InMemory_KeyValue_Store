#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <chrono>
#include <string>
#include <optional>
#include <vector>
#include <memory>

namespace kv_engine {
    class StorageEngine {
        public : 
            /**
             * @param memtable_threshold Number of entries the in-memory table
             * accumulates before it is flushed to a new SSTable. Defaults to
             * the historical value used throughout the test suite; callers
             * that need higher throughput (e.g. benchmarks) can raise it so
             * they don't end up with one SSTable file per few writes.
             * @param use_mmap_reads Experimental, opt-in: when true, every
             * SSTable is mmap()'d once (at flush/load/compaction time,
             * alongside the index and Bloom filter) and Get() reads directly
             * from mapped memory instead of a fresh open()/seekg()/read()
             * per lookup. Default false leaves the original ifstream-based
             * read path unchanged, so this is purely opt-in for A/B testing.
             */
            StorageEngine(const std::string& data_dir = "./data", size_t memtable_threshold = 5,
                          bool use_mmap_reads = false);
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

            /**
             * Forces a flush of the MemTable to disk.
             */
            void ForceFlush();

            /**
             * Runs one compaction pass synchronously: merges the two oldest
             * flushed SSTable generations into one, dropping obsolete
             * overwritten values and tombstones that provably shadow
             * nothing older. Never touches the newest generation. Requires
             * at least 3 flushed generations to do anything.
             * @return true if a merge happened, false if nothing was
             * eligible. Thread-safe; safe to call concurrently with
             * Put/Get/Delete and while background compaction is running
             * (compaction passes never overlap each other).
             */
            bool CompactOnce();

            /**
             * Starts a background thread that repeatedly runs CompactOnce()
             * (looping until nothing is left to merge, then sleeping for
             * poll_interval) until StopBackgroundCompaction() is called or
             * the engine is destroyed. The slow part of a compaction pass
             * (reading, merging, writing) holds no lock, so this never
             * meaningfully blocks concurrent Get/Put/Delete. No-op if
             * already running.
             */
            void StartBackgroundCompaction(std::chrono::seconds poll_interval = std::chrono::seconds(5));

            /**
             * Stops the background compaction thread if running. No-op if
             * not running. Also called from the destructor.
             */
            void StopBackgroundCompaction();
        private:
            // Mem Table management and WAL handling
            struct Impl;
            std::unique_ptr<Impl> pImpl;
    };
}

#endif // STORAGE_ENGINE_H