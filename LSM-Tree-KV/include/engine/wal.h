#ifndef WAL_H
#define WAL_H

#include <string>
#include <fstream>
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
    bool append(LogOp op, const std::string& key, const std::string& value = "");

    // Flushes buffers to physical disk
    void flush();

private:
    std::ofstream log_file;
    std::string path;
    std::mutex write_mutex; // Ensure thread-safe file access
};

#endif