#include "../include/engine/wal.h"
#include <iostream>
#include <map>

WAL::WAL(const std::string& filepath) : path(filepath) {
    // Open in append and binary mode
    log_file.open(path, std::ios::app | std::ios::binary);
    if (!log_file.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + path);
    }
}

WAL::~WAL() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

bool WAL::append(LogOp op, const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(write_mutex);

    char type = static_cast<char>(op);
    uint32_t kLen = static_cast<uint32_t>(key.size());
    uint32_t vLen = static_cast<uint32_t>(value.size());

    try {
        log_file.write(&type, sizeof(type));
        log_file.write(reinterpret_cast<const char*>(&kLen), sizeof(kLen));
        log_file.write(key.data(), kLen);
        
        if (op == LogOp::PUT) {
            log_file.write(reinterpret_cast<const char*>(&vLen), sizeof(vLen));
            log_file.write(value.data(), vLen);
        }

        log_file.flush(); // Ensure it hits the OS buffer immediately
        return true;
    } catch (...) {
        return false;
    }
}

void WAL::recover(std::map<std::string, std::string>& memtable) {
    std::ifstream reader(path, std::ios::binary | std::ios::in);
    if (!reader.is_open()) return; // No log file yet, that's fine

    while (reader.peek() != EOF) {
        char type_raw;
        uint32_t kLen, vLen;

        // 1. Read Type
        reader.read(&type_raw, sizeof(type_raw));
        LogOp op = static_cast<LogOp>(type_raw);

        // 2. Read Key
        reader.read(reinterpret_cast<char*>(&kLen), sizeof(kLen));
        std::string key(kLen, ' ');
        reader.read(&key[0], kLen);

        if (op == LogOp::PUT) {
            // 3. Read Value
            reader.read(reinterpret_cast<char*>(&vLen), sizeof(vLen));
            std::string value(vLen, ' ');
            reader.read(&value[0], vLen);
            
            memtable[key] = value; // Restore to memory
        } else if (op == LogOp::DELETE) {
            memtable.erase(key); // Replay the deletion
        }
    }
}