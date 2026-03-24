#include "engine/wal.h"
#include <iostream>

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