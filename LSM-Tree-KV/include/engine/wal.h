#ifndef WAL_H
#define WAL_H

#include <string>
#include <fstream>
#include <functional>
#include <mutex>

enum class LogOp : char {
    PUT = 1,
    DELETE = 2
};

class WAL {
public:
    explicit WAL(const std::string& filepath);
    ~WAL();

    // Appends a write operation to the log file
    bool append(LogOp op, uint64_t sequence,const std::string& key, const std::string& value = "");

    // Flushes buffers to physical disk
    void flush();

    // Reads every record in this log file, in order, handing each decoded
    // (op, sequence, key, value) to `visit`. Unlike the old recover(), this
    // doesn't interpret what a record MEANS (memtable insert vs. tombstone
    // tracking) -- it just decodes bytes. StorageEngine::Impl replays each
    // record through the same apply_put/apply_delete logic live Put/Delete
    // use, since recovery now needs to redo eager dead-marking (find the
    // key's prior on-disk location and flip its bit), which requires
    // consulting index/del-bitmap state WAL itself has no knowledge of.
    void read_all(const std::function<void(LogOp op, uint64_t sequence,
                                            const std::string& key, const std::string& value)>& visit);

private:
    std::ofstream log_file;
    std::string path;
    std::mutex write_mutex; // Ensure thread-safe file access
};

#endif